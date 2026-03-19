"""
Клиент для API OWS v3 goszakup.gov.kz (GraphQL + REST).
Поддерживает пагинацию, rate limiting, retry с backoff.
"""
import asyncio
import logging
from typing import Any

import httpx

from src.config import api_config
from src.ingestion.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class GoszakupAPIError(Exception):
    """Ошибка API goszakup.gov.kz."""
    pass


class GoszakupClient:
    """GraphQL-клиент для OWS v3."""

    GRAPHQL_URL = f"{api_config.base_url}/v3/graphql"

    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {api_config.token}",
            "Content-Type": "application/json",
        }
        self.rate_limiter = RateLimiter(api_config.requests_per_second)
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers=self.headers,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _execute_graphql(self, query: str, variables: dict | None = None) -> dict:
        """Выполняет GraphQL-запрос с retry и rate limiting."""
        client = await self._get_client()
        payload = {"query": query}
        if variables:
            payload["variables"] = variables  # type: ignore

        for attempt in range(api_config.max_retries + 1):
            await self.rate_limiter.acquire()
            try:
                resp = await client.post(self.GRAPHQL_URL, json=payload)

                if resp.status_code == 429:
                    wait = api_config.retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limited (429). Retry in {wait}s...")
                    await asyncio.sleep(wait)
                    continue

                if resp.status_code == 401:
                    raise GoszakupAPIError("Unauthorized: проверьте API-токен")

                resp.raise_for_status()
                data = resp.json()

                if "errors" in data:
                    raise GoszakupAPIError(f"GraphQL errors: {data['errors']}")

                return data

            except httpx.TimeoutException:
                if attempt < api_config.max_retries:
                    wait = api_config.retry_delay * (2 ** attempt)
                    logger.warning(f"Timeout. Retry {attempt+1}/{api_config.max_retries} in {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    raise GoszakupAPIError("API timeout after all retries")

            except httpx.HTTPStatusError as e:
                if attempt < api_config.max_retries and e.response.status_code >= 500:
                    wait = api_config.retry_delay * (2 ** attempt)
                    logger.warning(f"Server error {e.response.status_code}. Retry in {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    raise GoszakupAPIError(f"HTTP {e.response.status_code}: {e.response.text[:200]}")

        raise GoszakupAPIError("Max retries exceeded")

    async def fetch_all(
        self,
        query_name: str,
        query: str,
        limit: int = 200,
        after: int | None = None,
        max_records: int | None = None,
    ) -> list[dict]:
        """
        Выгружает все записи с автоматической пагинацией.

        Args:
            query_name: имя корневого поля в GraphQL (TrdBuy, Lots, Contract, ...)
            query: GraphQL-запрос (должен принимать $limit и $after)
            limit: записей на страницу (max 200 для GraphQL)
            after: начальный курсор (ID последней загруженной записи)
            max_records: макс. количество записей (None = все)

        Returns:
            Список всех записей.
        """
        all_records = []
        cursor = after
        page = 0

        while True:
            variables = {"limit": limit}
            if cursor is not None:
                variables["after"] = cursor

            data = await self._execute_graphql(query, variables)

            records = data.get("data", {}).get(query_name, [])
            extensions = data.get("extensions", {}).get("pageInfo", {})

            if not records:
                break

            all_records.extend(records)
            page += 1

            total = extensions.get("totalCount", "?")
            has_next = extensions.get("hasNextPage", False)
            cursor = extensions.get("lastId")

            logger.info(
                f"[{query_name}] Page {page}: +{len(records)} records "
                f"(total fetched: {len(all_records)}/{total})"
            )

            if not has_next or cursor is None:
                break

            if max_records and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                break

        logger.info(f"[{query_name}] Completed: {len(all_records)} records total")
        return all_records

    async def fetch_announcements(self, org_bin: str | None = None, after: int | None = None, max_records: int | None = None) -> list[dict]:
        """Загружает объявления о закупках (фильтр по orgBin)."""
        filter_clause = ""
        if org_bin:
            filter_clause = f', filter: {{orgBin: "{org_bin}"}}'

        query = """
        query($limit: Int, $after: Int) {
            TrdBuy(limit: $limit, after: $after""" + filter_clause + """) {
                id numberAnno nameRu nameKz
                totalSum publishDate startDate endDate
                orgBin orgNameRu customerBin customerNameRu
                refTradeMethodsId refBuyStatusId refSubjectTypeId
                countLots finYear kato
                isConstructionWork isLightIndustry
                lastUpdateDate indexDate systemId
            }
        }
        """
        return await self.fetch_all("TrdBuy", query, after=after, max_records=max_records)

    async def fetch_lots(self, customer_bin: str | None = None, after: int | None = None, max_records: int | None = None) -> list[dict]:
        """Загружает лоты."""
        filter_clause = ""
        if customer_bin:
            filter_clause = f', filter: {{customerBin: "{customer_bin}"}}'

        query = """
        query($limit: Int, $after: Int) {
            Lots(limit: $limit, after: $after""" + filter_clause + """) {
                id lotNumber nameRu nameKz
                descriptionRu amount count
                customerBin customerNameRu trdBuyId trdBuyNumberAnno
                refTradeMethodsId refLotStatusId
                enstruList plnPointKatoList
                isConstructionWork isLightIndustry
                lastUpdateDate indexDate systemId
                Plans { refEnstruCode refUnitsCode nameRu PlansKato { refKatoCode } }
            }
        }
        """
        return await self.fetch_all("Lots", query, after=after, max_records=max_records)

    async def fetch_contracts(self, customer_bin: str | None = None, supplier_bin: str | None = None, after: int | None = None, max_records: int | None = None) -> list[dict]:
        """Загружает договоры с предметами (ContractUnits)."""
        filter_clause = ""
        if customer_bin:
            filter_clause = f', filter: {{customerBin: "{customer_bin}"}}'
        elif supplier_bin:
            filter_clause = f', filter: {{supplierBiin: "{supplier_bin}"}}'

        query = """
        query($limit: Int, $after: Int) {
            Contract(limit: $limit, after: $after""" + filter_clause + """) {
                id contractNumber contractNumberSys
                contractSum signDate ecEndDate crdate finYear
                customerBin supplierId supplierBiin
                trdBuyId trdBuyNumberAnno trdBuyNameRu
                refContractStatusId refContractTypeId
                refContractAgrFormId refSubjectTypeId
                faktTradeMethodsId faktSum
                deleted lastUpdateDate indexDate systemId
                ContractUnits {
                    id lotId plnPointId itemPrice quantity totalSum
                    contractSum factSum
                    deleted trdBuyId systemId
                    Plans {
                        refEnstruCode refUnitsCode descRu nameRu
                        PlansKato { refKatoCode }
                    }
                }
            }
        }
        """
        return await self.fetch_all("Contract", query, after=after, max_records=max_records)

    async def fetch_plans(self, subject_bin: str | None = None, after: int | None = None, max_records: int | None = None) -> list[dict]:
        """Загружает годовые планы."""
        filter_clause = ""
        if subject_bin:
            filter_clause = f', filter: {{subjectBiin: "{subject_bin}"}}'

        query = """
        query($limit: Int, $after: Int) {
            Plans(limit: $limit, after: $after""" + filter_clause + """) {
                id rootrecordId subjectBiin subjectNameRu
                nameRu nameKz count price amount
                plnPointYear refTradeMethodsId refSubjectTypeId
                refEnstruCode refUnitsCode refMonthsId refPlnPointStatusId
                refFinsourceId isDeleted dateCreate timestamp indexDate systemId
                PlansKato { refKatoCode }
            }
        }
        """
        return await self.fetch_all("Plans", query, after=after, max_records=max_records)

    async def fetch_subjects(self, bin: str | None = None, after: int | None = None, max_records: int | None = None) -> list[dict]:
        """Загружает участников (организации)."""
        filter_clause = ""
        if bin:
            filter_clause = f', filter: {{bin: "{bin}"}}'

        query = """
        query($limit: Int, $after: Int) {
            Subjects(limit: $limit, after: $after""" + filter_clause + """) {
                pid bin iin nameRu nameKz fullNameRu
                customer supplier organizer
                katoList indexDate systemId
            }
        }
        """
        return await self.fetch_all("Subjects", query, after=after, max_records=max_records)

    async def fetch_applications(self, buy_id: int | None = None, after: int | None = None, max_records: int | None = None) -> list[dict]:
        """Загружает заявки поставщиков."""
        filter_clause = ""
        if buy_id:
            filter_clause = f', filter: {{buyId: {buy_id}}}'

        query = """
        query($limit: Int, $after: Int) {
            TrdApp(limit: $limit, after: $after""" + filter_clause + """) {
                id buyId supplierId supplierBinIin
                dateApply crFio indexDate systemId
                AppLots {
                    lotId price
                }
            }
        }
        """
        return await self.fetch_all("TrdApp", query, after=after, max_records=max_records)

    async def _fetch_rest_paginated(self, endpoint: str, max_pages: int = 500) -> list[dict]:
        """Загружает справочник через REST с пагинацией."""
        client = await self._get_client()
        all_items = []
        url = f"{api_config.base_url}{endpoint}"
        prev_url = None
        page = 0

        while url and page < max_pages:
            if url == prev_url:
                logger.warning(f"[REST {endpoint}] Pagination loop detected, stopping")
                break
            prev_url = url

            await self.rate_limiter.acquire()
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            if not items:
                break
            all_items.extend(items)
            page += 1

            next_page = data.get("next_page", "")
            if next_page:
                if next_page.startswith("/"):
                    url = f"{api_config.base_url}{next_page}"
                else:
                    url = next_page
            else:
                url = None

            logger.info(f"[REST {endpoint}] +{len(items)} (total: {len(all_items)})")

        return all_items

    async def fetch_ref_trade_methods(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_trade_methods")

    async def fetch_ref_buy_status(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_buy_status")

    async def fetch_ref_lots_status(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_lots_status")

    async def fetch_ref_contract_status(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_contract_status")

    async def fetch_ref_pln_point_status(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_pln_point_status")

    async def fetch_ref_units(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_units")

    async def fetch_ref_kato(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_kato")

    async def fetch_ref_finsource(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_finsource")

    async def fetch_ref_subject_type(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_subject_type")

    async def fetch_ref_contract_type(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_contract_type")

    async def fetch_ref_currency(self) -> list[dict]:
        return await self._fetch_rest_paginated("/v3/refs/ref_currency")


def run_sync(coro):
    """Запускает async-функцию синхронно."""
    return asyncio.run(coro)


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)

    async def test():
        client = GoszakupClient()
        try:
            records = await client.fetch_announcements(max_records=5)
            print(f"\n=== Announcements ({len(records)}) ===")
            for r in records:
                print(f"  [{r['id']}] {r['nameRu'][:80]} | {r['totalSum']} тг")

            methods = await client.fetch_ref_trade_methods()
            print(f"\n=== Trade Methods ({len(methods)}) ===")
            for m in methods[:5]:
                print(f"  [{m.get('id')}] {m.get('name_ru')}")

            statuses = await client.fetch_ref_buy_status()
            print(f"\n=== Buy Statuses ({len(statuses)}) ===")
            for s in statuses[:5]:
                print(f"  [{s.get('id')}] {s.get('name_ru')}")
        finally:
            await client.close()

    asyncio.run(test())
