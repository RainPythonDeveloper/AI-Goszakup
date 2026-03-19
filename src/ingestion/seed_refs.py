"""
Загрузка справочных данных из REST API в PostgreSQL.
Справочники: методы закупок, статусы, КАТО, единицы измерения и т.д.
"""
import asyncio
import logging

import psycopg2
from psycopg2.extras import execute_values

from src.config import db_config
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


def upsert_ref_methods(conn, items: list[dict]):
    """ref_trade_methods → ref_methods."""
    rows = []
    for item in items:
        code = str(item.get("id", ""))
        name_ru = item.get("name_ru", "")
        name_kz = item.get("name_kz", "")
        if code:
            rows.append((code, name_ru, name_kz))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ref_methods (code, name_ru, name_kz)
            VALUES %s
            ON CONFLICT (code) DO UPDATE SET
                name_ru = EXCLUDED.name_ru,
                name_kz = EXCLUDED.name_kz
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def upsert_ref_statuses(conn, items: list[dict], entity_type: str):
    """ref_buy_status / ref_lots_status / ref_contract_status → ref_statuses."""
    rows = []
    for item in items:
        code = str(item.get("id", ""))
        name_ru = item.get("name_ru", "")
        name_kz = item.get("name_kz", "")
        if code:
            rows.append((code, name_ru, name_kz, entity_type))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ref_statuses (code, name_ru, name_kz, entity_type)
            VALUES %s
            ON CONFLICT (code, entity_type) DO UPDATE SET
                name_ru = EXCLUDED.name_ru,
                name_kz = EXCLUDED.name_kz
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def upsert_ref_units(conn, items: list[dict]):
    """ref_units → ref_units."""
    rows = []
    for item in items:
        code = str(item.get("id", item.get("code", "")))
        name_ru = item.get("name_ru", "")
        name_kz = item.get("name_kz", "")
        if code:
            rows.append((code, name_ru, name_kz))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ref_units (code, name_ru, name_kz)
            VALUES %s
            ON CONFLICT (code) DO UPDATE SET
                name_ru = EXCLUDED.name_ru,
                name_kz = EXCLUDED.name_kz
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def upsert_ref_kato(conn, items: list[dict]):
    """ref_kato → ref_kato (с автоматическим определением региона и уровня)."""
    rows = []
    for item in items:
        code = str(item.get("code", item.get("id", "")))
        name_ru = item.get("name_ru", "")
        name_kz = item.get("name_kz", "")

        if not code:
            continue

        code_stripped = code.rstrip("0")
        if len(code_stripped) <= 2:
            level = 1  # область
        elif len(code_stripped) <= 4:
            level = 2  # район
        else:
            level = 3  # город/село

        region = _kato_region(code[:2]) if len(code) >= 2 else None

        rows.append((code, name_ru, name_kz, region, level))

    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ref_kato (code, name_ru, name_kz, region, level)
            VALUES %s
            ON CONFLICT (code) DO UPDATE SET
                name_ru = EXCLUDED.name_ru,
                name_kz = EXCLUDED.name_kz,
                region = EXCLUDED.region,
                level = EXCLUDED.level
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def _kato_region(prefix: str) -> str:
    """Маппинг первых 2 цифр КАТО на область."""
    mapping = {
        "01": "Акмолинская область",
        "03": "Актюбинская область",
        "05": "Алматинская область",
        "06": "Область Жетісу",
        "07": "Атырауская область",
        "09": "Западно-Казахстанская область",
        "10": "Жамбылская область",
        "11": "Карагандинская область",
        "12": "Область Ұлытау",
        "13": "Костанайская область",
        "14": "Кызылординская область",
        "15": "Мангистауская область",
        "16": "Павлодарская область",
        "17": "Область Абай",
        "18": "Северо-Казахстанская область",
        "19": "Туркестанская область",
        "20": "Восточно-Казахстанская область",
        "31": "г. Астана",
        "33": "г. Алматы",
        "35": "г. Шымкент",
        "39": "Байконур",
        "55": "г. Алматы",
        "71": "г. Астана",
        "75": "г. Шымкент",
        "79": "г. Шымкент",
    }
    return mapping.get(prefix, f"Код {prefix}")


def upsert_ref_currencies(conn, items: list[dict]):
    """ref_currency → ref_currencies."""
    seen = {}
    for item in items:
        code = str(item.get("code", item.get("id", "")))
        name_ru = item.get("name_ru", "")
        if code and code not in seen:
            seen[code] = (code, name_ru)

    rows = list(seen.values())
    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ref_currencies (code, name_ru)
            VALUES %s
            ON CONFLICT (code) DO UPDATE SET
                name_ru = EXCLUDED.name_ru
            """,
            rows,
        )
    conn.commit()
    return len(rows)


async def seed_all_refs():
    """Загружает все справочники из API в БД."""
    client = GoszakupClient()
    conn = get_conn()

    try:
        # 1. Способы закупок
        logger.info("Loading ref_trade_methods...")
        methods = await client.fetch_ref_trade_methods()
        cnt = upsert_ref_methods(conn, methods)
        logger.info(f"  ref_methods: {cnt} records")

        # 2. Статусы объявлений
        logger.info("Loading ref_buy_status...")
        statuses = await client.fetch_ref_buy_status()
        cnt = upsert_ref_statuses(conn, statuses, "announcement")
        logger.info(f"  ref_statuses (announcement): {cnt} records")

        # 3. Статусы лотов
        logger.info("Loading ref_lots_status...")
        statuses = await client.fetch_ref_lots_status()
        cnt = upsert_ref_statuses(conn, statuses, "lot")
        logger.info(f"  ref_statuses (lot): {cnt} records")

        # 4. Статусы договоров
        logger.info("Loading ref_contract_status...")
        statuses = await client.fetch_ref_contract_status()
        cnt = upsert_ref_statuses(conn, statuses, "contract")
        logger.info(f"  ref_statuses (contract): {cnt} records")

        # 5. Статусы планов
        logger.info("Loading ref_pln_point_status...")
        statuses = await client.fetch_ref_pln_point_status()
        cnt = upsert_ref_statuses(conn, statuses, "plan")
        logger.info(f"  ref_statuses (plan): {cnt} records")

        # 6. Единицы измерения
        logger.info("Loading ref_units...")
        units = await client.fetch_ref_units()
        cnt = upsert_ref_units(conn, units)
        logger.info(f"  ref_units: {cnt} records")

        # 7. КАТО
        logger.info("Loading ref_kato...")
        kato = await client.fetch_ref_kato()
        cnt = upsert_ref_kato(conn, kato)
        logger.info(f"  ref_kato: {cnt} records")

        # 8. Источники финансирования → ref_statuses (entity_type='finsource')
        logger.info("Loading ref_finsource...")
        finsource = await client.fetch_ref_finsource()
        cnt = upsert_ref_statuses(conn, finsource, "finsource")
        logger.info(f"  ref_statuses (finsource): {cnt} records")

        # 9. Типы субъектов → ref_statuses (entity_type='subject_type')
        logger.info("Loading ref_subject_type...")
        stypes = await client.fetch_ref_subject_type()
        cnt = upsert_ref_statuses(conn, stypes, "subject_type")
        logger.info(f"  ref_statuses (subject_type): {cnt} records")

        # 10. Типы договоров → ref_statuses (entity_type='contract_type')
        logger.info("Loading ref_contract_type...")
        ctypes = await client.fetch_ref_contract_type()
        cnt = upsert_ref_statuses(conn, ctypes, "contract_type")
        logger.info(f"  ref_statuses (contract_type): {cnt} records")

        # 11. Валюты
        logger.info("Loading ref_currency...")
        currencies = await client.fetch_ref_currency()
        cnt = upsert_ref_currencies(conn, currencies)
        logger.info(f"  ref_currencies: {cnt} records")

        logger.info("All reference data loaded successfully!")

    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(seed_all_refs())
