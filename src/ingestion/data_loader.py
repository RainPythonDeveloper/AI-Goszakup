"""
Bulk-загрузка основных сущностей из GraphQL API в PostgreSQL.
Загружает: subjects, announcements, lots, contracts, plans, applications по BIN.
"""
import asyncio
import json
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from src.config import db_config, TARGET_BINS
from src.ingestion.api_client import GoszakupClient

logger = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )

def _parse_date(val) -> datetime | None:
    if not val:
        return None
    if isinstance(val, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(val[:19], fmt)
            except ValueError:
                continue
    return None


def _resolve_status_id(conn, code, entity_type: str) -> int | None:
    """Получает ID статуса из ref_statuses по коду и типу."""
    if code is None:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM ref_statuses WHERE code = %s AND entity_type = %s",
            (str(code), entity_type),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _resolve_method_id(conn, code) -> int | None:
    """Получает ID метода из ref_methods по коду."""
    if code is None:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM ref_methods WHERE code = %s", (str(code),))
        row = cur.fetchone()
        return row[0] if row else None


def _build_unit_lookup(conn) -> dict[str, int]:
    """Строит кеш {code: id} из ref_units (621 запись)."""
    with conn.cursor() as cur:
        cur.execute("SELECT code, id FROM ref_units")
        return {row[0]: row[1] for row in cur.fetchall()}


def _save_raw(conn, endpoint: str, source_id, raw_json: dict):
    """Сохраняет сырые данные для аудита."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw_data (source_endpoint, source_id, raw_json)
               VALUES (%s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (endpoint, source_id, json.dumps(raw_json, ensure_ascii=False)),
        )


def _update_load_metadata(conn, entity_type: str, status: str, total: int = 0,
                          last_id: int | None = None, error: str | None = None):
    """Обновляет метаданные загрузки."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM load_metadata WHERE entity_type = %s", (entity_type,)
        )
        row = cur.fetchone()
        now = datetime.now()
        if row:
            cur.execute(
                """UPDATE load_metadata
                   SET status = %s, total_loaded = %s, last_source_id = %s,
                       completed_at = %s, error_message = %s
                   WHERE entity_type = %s""",
                (status, total, last_id, now if status in ("completed", "failed") else None, error, entity_type),
            )
        else:
            cur.execute(
                """INSERT INTO load_metadata (entity_type, status, total_loaded, last_source_id, started_at)
                   VALUES (%s, %s, %s, %s, %s)""",
                (entity_type, status, total, last_id, now),
            )
    conn.commit()

def load_subjects(conn, records: list[dict]) -> int:
    """Загружает участников (subjects) в БД."""
    rows = []
    for r in records:
        rows.append((
            r.get("pid"),
            r.get("bin", ""),
            r.get("iin"),
            r.get("nameRu"),
            r.get("nameKz"),
            bool(r.get("customer")),
            bool(r.get("organizer")),
            bool(r.get("supplier")),
            (r.get("katoList") or [""])[0] if isinstance(r.get("katoList"), list) else r.get("katoList"),
        ))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO subjects (source_id, bin, iin, name_ru, name_kz,
                                     is_customer, is_organizer, is_supplier, kato_code)
               VALUES %s
               ON CONFLICT (source_id) DO UPDATE SET
                   name_ru = EXCLUDED.name_ru,
                   name_kz = EXCLUDED.name_kz,
                   is_customer = EXCLUDED.is_customer,
                   is_organizer = EXCLUDED.is_organizer,
                   is_supplier = EXCLUDED.is_supplier,
                   kato_code = EXCLUDED.kato_code,
                   updated_at = NOW()""",
            rows,
        )
    conn.commit()
    return len(rows)


def load_announcements(conn, records: list[dict]) -> int:
    """Загружает объявления (TrdBuy) в БД."""
    rows = []
    for r in records:
        method_id = _resolve_method_id(conn, r.get("refTradeMethodsId"))
        status_id = _resolve_status_id(conn, r.get("refBuyStatusId"), "announcement")

        rows.append((
            r.get("id"),
            r.get("numberAnno"),
            r.get("nameRu"),
            r.get("nameKz"),
            r.get("totalSum"),
            method_id,
            status_id,
            r.get("orgBin"),
            r.get("customerBin"),
            _parse_date(r.get("startDate")),
            _parse_date(r.get("endDate")),
            _parse_date(r.get("publishDate")),
            bool(r.get("isConstructionWork")),
            r.get("countLots"),
            r.get("systemId"),
        ))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO announcements (source_id, number_anno, name_ru, name_kz, total_sum,
                                          method_id, status_id, organizer_bin, customer_bin,
                                          start_date, end_date, publish_date,
                                          is_construction, lot_count, source_system)
               VALUES %s
               ON CONFLICT (source_id) DO UPDATE SET
                   name_ru = EXCLUDED.name_ru,
                   name_kz = EXCLUDED.name_kz,
                   total_sum = EXCLUDED.total_sum,
                   method_id = EXCLUDED.method_id,
                   status_id = EXCLUDED.status_id,
                   updated_at = NOW()""",
            rows,
        )
    conn.commit()
    return len(rows)


def load_lots(conn, records: list[dict], unit_lookup: dict[str, int] | None = None) -> int:
    """Загружает лоты в БД."""
    if unit_lookup is None:
        unit_lookup = _build_unit_lookup(conn)
    rows = []
    for r in records:
        status_id = _resolve_status_id(conn, r.get("refLotStatusId"), "lot")

        ann_id = None
        trd_buy_id = r.get("trdBuyId")
        if trd_buy_id:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM announcements WHERE source_id = %s", (trd_buy_id,))
                row = cur.fetchone()
                if row:
                    ann_id = row[0]

        enstru = None
        plans = r.get("Plans")
        if isinstance(plans, list):
            for plan in plans:
                code = plan.get("refEnstruCode") if isinstance(plan, dict) else None
                if code and str(code).strip() and str(code).strip() != "0":
                    enstru = str(code).strip()
                    break

        if not enstru:
            enstru_list = r.get("enstruList")
            if isinstance(enstru_list, list) and enstru_list:
                val = str(enstru_list[0])
                if val != "0":
                    enstru = val
            elif isinstance(enstru_list, str) and enstru_list and enstru_list != "0":
                enstru = enstru_list

        kato = None
        kato_list = r.get("plnPointKatoList")
        if isinstance(kato_list, list) and kato_list:
            kato = str(kato_list[0])
        elif isinstance(kato_list, str) and kato_list:
            kato = kato_list

        if not kato:
            plans = r.get("Plans")
            if isinstance(plans, list):
                for plan in plans:
                    plans_kato = plan.get("PlansKato")
                    if isinstance(plans_kato, list) and plans_kato:
                        code = plans_kato[0].get("refKatoCode")
                        if code and str(code).strip() and str(code).strip() != "0":
                            kato = str(code).strip()
                            break

        unit_id = None
        plans = r.get("Plans")
        if isinstance(plans, list):
            for plan in plans:
                uc = plan.get("refUnitsCode") if isinstance(plan, dict) else None
                if uc and str(uc).strip() and str(uc).strip() != "0":
                    unit_id = unit_lookup.get(str(uc).strip())
                    break

        amount = r.get("amount")
        count = r.get("count")
        price_per_unit = None
        if amount and count and float(count) > 0:
            price_per_unit = float(amount) / float(count)

        rows.append((
            r.get("id"),
            ann_id,
            r.get("lotNumber"),
            r.get("nameRu"),
            r.get("nameKz"),
            amount,
            count,
            price_per_unit,
            enstru,
            r.get("customerBin"),
            kato,
            status_id,
            unit_id,
        ))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO lots (source_id, announcement_id, lot_number, name_ru, name_kz,
                                 amount, count, price_per_unit, enstru_code,
                                 customer_bin, delivery_kato, status_id, unit_id)
               VALUES %s
               ON CONFLICT (source_id) DO UPDATE SET
                   name_ru = EXCLUDED.name_ru,
                   name_kz = EXCLUDED.name_kz,
                   amount = EXCLUDED.amount,
                   count = EXCLUDED.count,
                   price_per_unit = EXCLUDED.price_per_unit,
                   enstru_code = EXCLUDED.enstru_code,
                   delivery_kato = EXCLUDED.delivery_kato,
                   status_id = EXCLUDED.status_id,
                   unit_id = EXCLUDED.unit_id,
                   updated_at = NOW()""",
            rows,
        )
    conn.commit()
    return len(rows)


def load_contracts(conn, records: list[dict], unit_lookup: dict[str, int] | None = None) -> int:
    """Загружает договоры и предметы договора (contract_subjects)."""
    if unit_lookup is None:
        unit_lookup = _build_unit_lookup(conn)
    contract_count = 0
    subject_count = 0

    for r in records:
        status_id = _resolve_status_id(conn, r.get("refContractStatusId"), "contract")

        ann_id = None
        trd_buy_id = r.get("trdBuyId")
        if trd_buy_id:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM announcements WHERE source_id = %s", (trd_buy_id,))
                row = cur.fetchone()
                if row:
                    ann_id = row[0]

        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO contracts (source_id, contract_number, announcement_id,
                                          supplier_bin, customer_bin, contract_sum,
                                          sign_date, ec_end_date, status_id,
                                          contract_type, source_system)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (source_id) DO UPDATE SET
                       contract_sum = EXCLUDED.contract_sum,
                       status_id = EXCLUDED.status_id,
                       updated_at = NOW()
                   RETURNING id""",
                (
                    r.get("id"),
                    r.get("contractNumber") or r.get("contractNumberSys"),
                    ann_id,
                    r.get("supplierBiin"),
                    r.get("customerBin"),
                    r.get("contractSum") or r.get("faktSum"),
                    _parse_date(r.get("signDate")),
                    _parse_date(r.get("ecEndDate")),
                    status_id,
                    str(r.get("refContractTypeId", "")),
                    r.get("systemId"),
                ),
            )
            result = cur.fetchone()
            contract_id = result[0] if result else None
            contract_count += 1

        units = r.get("ContractUnits") or []
        for unit in units:
            plan = unit.get("Plans") or {}
            enstru = plan.get("refEnstruCode") if plan else None
            desc = plan.get("descRu") or plan.get("nameRu") if plan else None

            kato = None
            plans_kato = plan.get("PlansKato") or [] if plan else []
            if isinstance(plans_kato, list) and plans_kato:
                kato = plans_kato[0].get("refKatoCode") if isinstance(plans_kato[0], dict) else str(plans_kato[0])

            unit_code = plan.get("refUnitsCode") if plan else None
            unit_id = unit_lookup.get(str(unit_code).strip()) if unit_code else None

            quantity = unit.get("quantity")
            item_price = unit.get("itemPrice")
            total_sum = unit.get("totalSum")

            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO contract_subjects
                           (source_id, contract_id, enstru_code, name_ru,
                            quantity, price_per_unit, total_price, delivery_kato, unit_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (source_id) DO UPDATE SET
                           price_per_unit = EXCLUDED.price_per_unit,
                           total_price = EXCLUDED.total_price,
                           unit_id = EXCLUDED.unit_id""",
                    (
                        unit.get("id"),
                        contract_id,
                        enstru,
                        desc,
                        quantity,
                        item_price,
                        total_sum,
                        kato,
                        unit_id,
                    ),
                )
            subject_count += 1

    conn.commit()
    logger.info(f"  Contracts: {contract_count}, ContractSubjects: {subject_count}")
    return contract_count


def load_plans(conn, records: list[dict]) -> int:
    """Загружает годовые планы."""
    rows = []
    for r in records:
        method_id = _resolve_method_id(conn, r.get("refTradeMethodsId"))

        kato = None
        kato_list = r.get("PlansKato") or []
        if isinstance(kato_list, list) and kato_list:
            kato = kato_list[0].get("refKatoCode") if isinstance(kato_list[0], dict) else str(kato_list[0])

        rows.append((
            r.get("id"),
            r.get("subjectBiin"),
            r.get("refEnstruCode"),
            r.get("amount"),
            r.get("count"),
            method_id,
            r.get("plnPointYear"),
            r.get("refMonthsId"),
            r.get("refFinsourceId"),
            kato,
            str(r.get("refPlnPointStatusId", "")),
        ))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO plans (source_id, bin, enstru_code, plan_sum, plan_count,
                                  method_id, year, month, source_funding,
                                  delivery_kato, status)
               VALUES %s
               ON CONFLICT (source_id) DO UPDATE SET
                   plan_sum = EXCLUDED.plan_sum,
                   plan_count = EXCLUDED.plan_count,
                   status = EXCLUDED.status,
                   updated_at = NOW()""",
            rows,
        )
    conn.commit()
    return len(rows)


def load_applications(conn, records: list[dict]) -> int:
    """Загружает заявки поставщиков."""
    count = 0
    for r in records:
        ann_id = None
        buy_id = r.get("buyId")
        if buy_id:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM announcements WHERE source_id = %s", (buy_id,))
                row = cur.fetchone()
                if row:
                    ann_id = row[0]

        app_lots = r.get("AppLots") or []
        if not app_lots:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO applications (source_id, announcement_id, supplier_bin,
                                                 submission_date)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (source_id) DO NOTHING""",
                    (r.get("id"), ann_id, r.get("supplierBinIin"), _parse_date(r.get("dateApply"))),
                )
            count += 1
        else:
            for lot_info in app_lots:
                source_id = int(f"{r.get('id', 0)}{lot_info.get('lotId', 0)}")
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO applications (source_id, announcement_id, supplier_bin,
                                                     price_offer, submission_date)
                           VALUES (%s, %s, %s, %s, %s)
                           ON CONFLICT (source_id) DO NOTHING""",
                        (source_id, ann_id, r.get("supplierBinIin"),
                         lot_info.get("price"), _parse_date(r.get("dateApply"))),
                    )
                count += 1

    conn.commit()
    return count

async def load_all_data(bins: list[str] | None = None, max_per_entity: int | None = None):
    """
    Загружает все данные по списку БИНов.

    Args:
        bins: список БИНов (по умолчанию TARGET_BINS из config)
        max_per_entity: макс. записей на сущность (None = все, для тестов ставьте 100)
    """
    if bins is None:
        bins = TARGET_BINS

    client = GoszakupClient()
    conn = get_conn()

    try:
        logger.info("=" * 60)
        logger.info("LOADING SUBJECTS")
        logger.info("=" * 60)
        _update_load_metadata(conn, "subjects", "running")
        total_subjects = 0
        for bin_code in bins:
            logger.info(f"  Fetching subjects for BIN={bin_code}...")
            records = await client.fetch_subjects(bin=bin_code, max_records=max_per_entity)
            if records:
                cnt = load_subjects(conn, records)
                total_subjects += cnt
                logger.info(f"  BIN={bin_code}: {cnt} subjects loaded")
            else:
                logger.info(f"  BIN={bin_code}: no subjects found")
        _update_load_metadata(conn, "subjects", "completed", total_subjects)

        logger.info("=" * 60)
        logger.info("LOADING ANNOUNCEMENTS")
        logger.info("=" * 60)
        _update_load_metadata(conn, "announcements", "running")
        total_ann = 0
        for bin_code in bins:
            logger.info(f"  Fetching announcements for orgBin={bin_code}...")
            records = await client.fetch_announcements(org_bin=bin_code, max_records=max_per_entity)
            if records:
                cnt = load_announcements(conn, records)
                total_ann += cnt
                logger.info(f"  BIN={bin_code}: {cnt} announcements loaded")
        _update_load_metadata(conn, "announcements", "completed", total_ann)

        logger.info("=" * 60)
        logger.info("LOADING LOTS")
        logger.info("=" * 60)
        _update_load_metadata(conn, "lots", "running")
        total_lots = 0
        for bin_code in bins:
            logger.info(f"  Fetching lots for customerBin={bin_code}...")
            records = await client.fetch_lots(customer_bin=bin_code, max_records=max_per_entity)
            if records:
                cnt = load_lots(conn, records)
                total_lots += cnt
                logger.info(f"  BIN={bin_code}: {cnt} lots loaded")
        _update_load_metadata(conn, "lots", "completed", total_lots)

        logger.info("=" * 60)
        logger.info("LOADING CONTRACTS")
        logger.info("=" * 60)
        _update_load_metadata(conn, "contracts", "running")
        total_contracts = 0
        for bin_code in bins:
            logger.info(f"  Fetching contracts for customerBin={bin_code}...")
            records = await client.fetch_contracts(customer_bin=bin_code, max_records=max_per_entity)
            if records:
                cnt = load_contracts(conn, records)
                total_contracts += cnt
                logger.info(f"  BIN={bin_code}: {cnt} contracts loaded")
        _update_load_metadata(conn, "contracts", "completed", total_contracts)

        logger.info("=" * 60)
        logger.info("LOADING PLANS")
        logger.info("=" * 60)
        _update_load_metadata(conn, "plans", "running")
        total_plans = 0
        for bin_code in bins:
            logger.info(f"  Fetching plans for subjectBiin={bin_code}...")
            records = await client.fetch_plans(subject_bin=bin_code, max_records=max_per_entity)
            if records:
                cnt = load_plans(conn, records)
                total_plans += cnt
                logger.info(f"  BIN={bin_code}: {cnt} plans loaded")
        _update_load_metadata(conn, "plans", "completed", total_plans)

        logger.info("=" * 60)
        logger.info("LOAD COMPLETE")
        logger.info(f"  Subjects:      {total_subjects}")
        logger.info(f"  Announcements: {total_ann}")
        logger.info(f"  Lots:          {total_lots}")
        logger.info(f"  Contracts:     {total_contracts}")
        logger.info(f"  Plans:         {total_plans}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise
    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(load_all_data())
