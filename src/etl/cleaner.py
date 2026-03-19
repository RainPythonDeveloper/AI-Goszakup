"""
ETL: Очистка данных (Data Cleaning).
- Нормализация текста (удаление лишних пробелов, спецсимволов)
- Вычисление price_per_unit для лотов
- Логирование проблем в data_quality_log
"""
import logging
import re

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


def _clean_text(text: str | None) -> str | None:
    """Очищает текст: убирает лишние пробелы, переносы строк, спецсимволы."""
    if not text:
        return None
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)
    return text if text else None


def clean_lots_names(conn):
    """Очищает название лотов → name_ru_clean."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, name_ru FROM lots WHERE name_ru IS NOT NULL AND name_ru_clean IS NULL")
        rows = cur.fetchall()

    updated = 0
    for lot_id, name_ru in rows:
        clean = _clean_text(name_ru)
        if clean:
            clean = re.sub(r'[«»"\'""„]', '', clean)
            clean = clean.strip()

        with conn.cursor() as cur:
            cur.execute("UPDATE lots SET name_ru_clean = %s WHERE id = %s", (clean, lot_id))
        updated += 1

    conn.commit()
    logger.info(f"Cleaned {updated} lot names")
    return updated


def compute_price_per_unit(conn):
    """Вычисляет price_per_unit для лотов где нет значения."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE lots
            SET price_per_unit = amount / NULLIF(count, 0)
            WHERE price_per_unit IS NULL
              AND amount IS NOT NULL
              AND count IS NOT NULL
              AND count > 0
        """)
        updated = cur.rowcount
    conn.commit()
    logger.info(f"Computed price_per_unit for {updated} lots")
    return updated


def log_quality_issue(conn, table: str, record_id: int, source_id: int | None,
                      issue: str, severity: str = 'warning'):
    """Записывает проблему качества данных."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO data_quality_log (table_name, record_id, source_id, issue, severity)
               VALUES (%s, %s, %s, %s, %s)""",
            (table, record_id, source_id, issue, severity),
        )


def validate_data_quality(conn):
    """Проверяет качество данных и логирует проблемы."""
    issues_found = 0

    with conn.cursor() as cur:
        # 1. Лоты без суммы
        cur.execute("SELECT id, source_id FROM lots WHERE amount IS NULL OR amount = 0")
        for row in cur.fetchall():
            log_quality_issue(conn, 'lots', row[0], row[1], 'Missing or zero amount', 'warning')
            issues_found += 1

        # 2. Лоты без ENSTRU
        cur.execute("SELECT id, source_id FROM lots WHERE enstru_code IS NULL")
        for row in cur.fetchall():
            log_quality_issue(conn, 'lots', row[0], row[1], 'Missing ENSTRU code', 'info')
            issues_found += 1

        # 3. Договоры без суммы
        cur.execute("SELECT id, source_id FROM contracts WHERE contract_sum IS NULL OR contract_sum = 0")
        for row in cur.fetchall():
            log_quality_issue(conn, 'contracts', row[0], row[1], 'Missing or zero contract_sum', 'warning')
            issues_found += 1

        # 4. Договоры без даты подписания
        cur.execute("SELECT id, source_id FROM contracts WHERE sign_date IS NULL")
        for row in cur.fetchall():
            log_quality_issue(conn, 'contracts', row[0], row[1], 'Missing sign_date', 'warning')
            issues_found += 1

        # 5. Contract_subjects с нулевой ценой
        cur.execute("""
            SELECT id, source_id FROM contract_subjects
            WHERE (price_per_unit IS NULL OR price_per_unit = 0) AND total_price > 0
        """)
        for row in cur.fetchall():
            log_quality_issue(conn, 'contract_subjects', row[0], row[1],
                              'Zero price_per_unit with non-zero total_price', 'warning')
            issues_found += 1

        # 6. Отрицательные цены
        cur.execute("SELECT id, source_id FROM contract_subjects WHERE price_per_unit < 0")
        for row in cur.fetchall():
            log_quality_issue(conn, 'contract_subjects', row[0], row[1],
                              'Negative price_per_unit', 'error')
            issues_found += 1

    conn.commit()
    logger.info(f"Data quality check: {issues_found} issues found")
    return issues_found


def run_cleaning():
    """Запускает полный цикл очистки."""
    conn = get_conn()
    try:
        logger.info("=== ETL: Data Cleaning ===")
        clean_lots_names(conn)
        compute_price_per_unit(conn)
        validate_data_quality(conn)
        logger.info("=== Cleaning complete ===")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_cleaning()
