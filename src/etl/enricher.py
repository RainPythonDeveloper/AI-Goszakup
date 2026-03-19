"""
ETL: Обогащение данных (Data Enrichment).
- Связывание сущностей (announcements ↔ lots ↔ contracts)
- Обновление materialized views
- Вычисление вспомогательных метрик
"""
import logging

import psycopg2

from src.config import db_config

logger = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )


def link_lots_to_announcements(conn):
    """Связывает лоты с объявлениями через trdBuyId (source_id)."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE lots l
            SET announcement_id = a.id
            FROM announcements a
            WHERE l.announcement_id IS NULL
              AND a.source_id IS NOT NULL
              AND l.source_id IN (
                  SELECT source_id FROM lots WHERE announcement_id IS NULL
              )
        """)
        updated = cur.rowcount
    conn.commit()
    logger.info(f"Linked {updated} lots to announcements")
    return updated


def compute_contract_subjects_total(conn):
    """Пересчитывает total_price для contract_subjects где отсутствует."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE contract_subjects
            SET total_price = price_per_unit * quantity
            WHERE total_price IS NULL
              AND price_per_unit IS NOT NULL
              AND quantity IS NOT NULL
              AND price_per_unit > 0
              AND quantity > 0
        """)
        updated = cur.rowcount
    conn.commit()
    logger.info(f"Computed total_price for {updated} contract_subjects")
    return updated


def enrich_subjects_from_contracts(conn):
    """Добавляет поставщиков как subjects если их нет."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO subjects (bin, name_ru, is_supplier)
            SELECT DISTINCT c.supplier_bin, '(Поставщик из договора)', TRUE
            FROM contracts c
            WHERE c.supplier_bin IS NOT NULL
              AND c.supplier_bin != ''
              AND NOT EXISTS (
                  SELECT 1 FROM subjects s WHERE s.bin = c.supplier_bin
              )
            ON CONFLICT (bin) DO UPDATE SET
                is_supplier = TRUE
        """)
        new_suppliers = cur.rowcount
    conn.commit()
    logger.info(f"Added {new_suppliers} new suppliers from contracts")
    return new_suppliers


def refresh_materialized_views(conn):
    """Обновляет все materialized views."""
    views = [
        'mv_price_statistics',
        'mv_volume_trends',
        'mv_supplier_stats',
        'mv_regional_coefficients',
        'mv_data_overview',
    ]

    for view in views:
        try:
            with conn.cursor() as cur:
                cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
            conn.commit()
            logger.info(f"Refreshed {view}")
        except Exception as e:
            conn.rollback()
            logger.warning(f"Failed to refresh {view}: {e}")


def show_data_overview(conn):
    """Выводит обзор данных из mv_data_overview."""
    with conn.cursor() as cur:
        cur.execute("SELECT entity, cnt FROM mv_data_overview ORDER BY entity")
        rows = cur.fetchall()

    logger.info("=== Data Overview ===")
    for entity, cnt in rows:
        logger.info(f"  {entity}: {cnt:,}")


def run_enrichment():
    """Запускает полный цикл обогащения."""
    conn = get_conn()
    try:
        logger.info("=== ETL: Data Enrichment ===")
        compute_contract_subjects_total(conn)
        enrich_subjects_from_contracts(conn)
        refresh_materialized_views(conn)
        show_data_overview(conn)
        logger.info("=== Enrichment complete ===")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_enrichment()
