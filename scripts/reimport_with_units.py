"""
Переимпорт лотов и договоров с извлечением unit_id из API.
Пересоздание MV с колонкой unit_id.

Запуск: python -m scripts.reimport_with_units
"""
import asyncio
import logging
import time
import psycopg2

from src.config import db_config, TARGET_BINS
from src.ingestion.api_client import GoszakupClient
from src.ingestion.data_loader import (
    load_lots, load_contracts, _build_unit_lookup,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 30
PAUSE_BETWEEN_BINS = 3


def get_conn():
    return psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )


def recreate_materialized_views(conn):
    """Пересоздаёт MV с новой колонкой unit_id."""
    logger.info("Пересоздание materialized views с unit_id...")

    with conn.cursor() as cur:
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_price_statistics CASCADE")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_regional_coefficients CASCADE")
        conn.commit()
        logger.info("  Старые MV удалены")

        cur.execute("""
            CREATE MATERIALIZED VIEW mv_price_statistics AS
            SELECT
                cs.enstru_code,
                cs.unit_id,
                cs.delivery_kato,
                rk.region AS delivery_region,
                date_trunc('quarter', c.sign_date)::DATE AS period,
                EXTRACT(YEAR FROM c.sign_date)::SMALLINT AS year,
                COUNT(*) AS sample_size,
                SUM(cs.price_per_unit * cs.quantity) / NULLIF(SUM(cs.quantity), 0) AS weighted_avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) AS median_price,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cs.price_per_unit) AS q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cs.price_per_unit) AS q3,
                MIN(cs.price_per_unit) AS min_price,
                MAX(cs.price_per_unit) AS max_price,
                STDDEV(cs.price_per_unit) AS std_dev,
                SUM(cs.quantity) AS total_quantity,
                SUM(cs.total_price) AS total_sum
            FROM contract_subjects cs
            JOIN contracts c ON cs.contract_id = c.id
            LEFT JOIN ref_kato rk ON cs.delivery_kato = rk.code
            WHERE cs.price_per_unit IS NOT NULL
              AND cs.price_per_unit > 0
              AND c.sign_date IS NOT NULL
            GROUP BY cs.enstru_code, cs.unit_id, cs.delivery_kato, rk.region,
                     date_trunc('quarter', c.sign_date), EXTRACT(YEAR FROM c.sign_date)
            WITH DATA
        """)
        conn.commit()
        logger.info("  mv_price_statistics создан")

        cur.execute("""
            CREATE MATERIALIZED VIEW mv_regional_coefficients AS
            SELECT
                cs.enstru_code,
                cs.unit_id,
                rk.region,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) AS regional_median,
                COUNT(*) AS sample_size
            FROM contract_subjects cs
            JOIN contracts c ON cs.contract_id = c.id
            LEFT JOIN ref_kato rk ON cs.delivery_kato = rk.code
            WHERE cs.price_per_unit IS NOT NULL
              AND cs.price_per_unit > 0
              AND rk.region IS NOT NULL
            GROUP BY cs.enstru_code, cs.unit_id, rk.region
            WITH DATA
        """)
        conn.commit()
        logger.info("  mv_regional_coefficients создан")


def refresh_all_mvs(conn):
    """Обновляет все materialized views."""
    logger.info("Обновление всех materialized views...")
    views = [
        'mv_price_statistics',
        'mv_regional_coefficients',
        'mv_volume_trends',
        'mv_supplier_stats',
        'mv_data_overview',
    ]
    with conn.cursor() as cur:
        for view in views:
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
                conn.commit()
                logger.info(f"  {view} обновлён")
            except Exception as e:
                conn.rollback()
                logger.warning(f"  {view} не удалось обновить: {e}")


def verify_unit_coverage(conn):
    """Проверяет покрытие unit_id после импорта."""
    logger.info("\n" + "=" * 60)
    logger.info("ВЕРИФИКАЦИЯ unit_id")
    logger.info("=" * 60)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(unit_id) as with_unit,
                ROUND(COUNT(unit_id) * 100.0 / NULLIF(COUNT(*), 0), 1) as pct
            FROM contract_subjects
        """)
        row = cur.fetchone()
        logger.info(f"  contract_subjects: {row[1]}/{row[0]} с unit_id ({row[2]}%)")

        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(unit_id) as with_unit,
                ROUND(COUNT(unit_id) * 100.0 / NULLIF(COUNT(*), 0), 1) as pct
            FROM lots
        """)
        row = cur.fetchone()
        logger.info(f"  lots: {row[1]}/{row[0]} с unit_id ({row[2]}%)")

        cur.execute("""
            SELECT ru.name_ru, COUNT(*) as cnt
            FROM contract_subjects cs
            LEFT JOIN ref_units ru ON cs.unit_id = ru.id
            WHERE cs.unit_id IS NOT NULL
            GROUP BY ru.name_ru
            ORDER BY cnt DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        if rows:
            logger.info("  Топ-10 единиц в contract_subjects:")
            for name, cnt in rows:
                logger.info(f"    {name}: {cnt}")

        cur.execute("""
            SELECT ru.name_ru as unit, COUNT(*) as cnt,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) as median
            FROM contract_subjects cs
            LEFT JOIN ref_units ru ON cs.unit_id = ru.id
            WHERE cs.enstru_code = '325011.500.000011'
            GROUP BY ru.name_ru
            ORDER BY cnt DESC
        """)
        rows = cur.fetchall()
        if rows:
            logger.info("  Проверка ENSTRU 325011.500.000011 (пример пользователя):")
            for unit, cnt, median in rows:
                logger.info(f"    {unit or 'NULL'}: {cnt} записей, медиана={median:,.2f}")


async def reimport_entity(client, conn, entity_name, fetch_func, load_func,
                          bins, unit_lookup, bin_param_name):
    """Переимпорт одной сущности для всех BIN с ретраями."""
    logger.info("=" * 60)
    logger.info(f"REIMPORTING {entity_name.upper()}")
    logger.info("=" * 60)

    total = 0
    failed_bins = []

    for i, bin_code in enumerate(bins, 1):
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"  [{i}/{len(bins)}] BIN={bin_code} (попытка {attempt})...")
                kwargs = {bin_param_name: bin_code}
                records = await fetch_func(**kwargs)
                if records:
                    cnt = load_func(conn, records, unit_lookup=unit_lookup)
                    total += cnt
                    logger.info(f"  BIN={bin_code}: {cnt} {entity_name} loaded")
                else:
                    logger.info(f"  BIN={bin_code}: нет записей")
                success = True
                break
            except Exception as e:
                logger.warning(f"  BIN={bin_code} попытка {attempt} ошибка: {e}")
                if attempt < MAX_RETRIES:
                    logger.info(f"  Повтор через {RETRY_DELAY}с...")
                    await asyncio.sleep(RETRY_DELAY)

        if not success:
            failed_bins.append(bin_code)
            logger.error(f"  BIN={bin_code}: ВСЕ {MAX_RETRIES} ПОПЫТКИ НЕУДАЧНЫ")

        if i < len(bins):
            await asyncio.sleep(PAUSE_BETWEEN_BINS)

    logger.info(f"  ИТОГО {entity_name}: {total} записей")
    if failed_bins:
        logger.error(f"  НЕУДАЧНЫЕ BIN: {failed_bins}")

    return total, failed_bins


async def main():
    start = time.time()
    client = GoszakupClient()
    conn = get_conn()

    try:
        unit_lookup = _build_unit_lookup(conn)
        logger.info(f"Unit lookup: {len(unit_lookup)} единиц")

        lots_total, lots_failed = await reimport_entity(
            client, conn, "lots",
            fetch_func=client.fetch_lots,
            load_func=load_lots,
            bins=TARGET_BINS,
            unit_lookup=unit_lookup,
            bin_param_name="customer_bin",
        )

        contracts_total, contracts_failed = await reimport_entity(
            client, conn, "contracts",
            fetch_func=client.fetch_contracts,
            load_func=load_contracts,
            bins=TARGET_BINS,
            unit_lookup=unit_lookup,
            bin_param_name="customer_bin",
        )

        recreate_materialized_views(conn)

        verify_unit_coverage(conn)

        elapsed = time.time() - start
        logger.info(f"\nВсего: lots={lots_total}, contracts={contracts_total}")
        logger.info(f"Время: {elapsed/60:.1f} мин")

        if lots_failed or contracts_failed:
            logger.error(f"НЕУДАЧНЫЕ: lots={lots_failed}, contracts={contracts_failed}")
        else:
            logger.info("Все 27 BIN успешно загружены!")

    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
