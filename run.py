"""
AI Госзакупки РК — единая точка запуска.

Использование:
    python3 run.py              # Полный запуск: проверка БД → загрузка → ETL → сервер
    python3 run.py server       # Только сервер (данные уже загружены)
    python3 run.py load         # Только загрузка данных
    python3 run.py etl          # Только ETL (очистка + обновление views)
    python3 run.py status       # Показать статус данных
"""
import asyncio
import logging
import subprocess
import sys
import time

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run")

def check_postgres():
    """Проверяет что PostgreSQL работает."""
    from src.config import db_config
    try:
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            dbname=db_config.database,
            user=db_config.user,
            password=db_config.password,
        )
        conn.close()
        logger.info("PostgreSQL: OK")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL: НЕ ДОСТУПЕН ({e})")
        logger.info("Запустите: docker-compose up -d postgres")
        return False


def check_docker_postgres():
    """Проверяет и запускает PostgreSQL через Docker если нужно."""
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=goszakup_postgres", "--format", "{{.Status}}"],
        capture_output=True, text=True,
    )
    if "Up" in result.stdout:
        logger.info("Docker PostgreSQL: уже запущен")
        return True

    logger.info("Docker PostgreSQL: запускаю...")
    subprocess.run(["docker-compose", "up", "-d", "postgres"], cwd=".")
    for i in range(30):
        time.sleep(2)
        if check_postgres():
            return True
    logger.error("PostgreSQL не запустился за 60 секунд")
    return False


def get_data_counts() -> dict:
    """Возвращает количество записей в каждой таблице."""
    from src.config import db_config
    conn = psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )
    tables = [
        'subjects', 'announcements', 'lots', 'contracts',
        'contract_subjects', 'plans',
        'ref_methods', 'ref_statuses', 'ref_units', 'ref_kato',
    ]
    counts = {}
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0] # type: ignore
    conn.close()
    return counts


def show_status():
    """Показывает текущий статус данных."""
    if not check_postgres():
        return

    counts = get_data_counts()
    print("\n" + "=" * 50)
    print("  СТАТУС ДАННЫХ")
    print("=" * 50)

    print("\n  Справочники:")
    for t in ['ref_methods', 'ref_statuses', 'ref_units', 'ref_kato']:
        print(f"    {t}: {counts[t]:,}")

    print("\n  Основные данные:")
    for t in ['subjects', 'announcements', 'lots', 'contracts', 'contract_subjects', 'plans']:
        status = "OK" if counts[t] > 0 else "ПУСТО"
        print(f"    {t}: {counts[t]:,}  [{status}]")

    total = sum(counts[t] for t in ['announcements', 'lots', 'contracts', 'contract_subjects', 'plans'])
    print(f"\n  ИТОГО записей: {total:,}")

    refs_ok = all(counts[t] > 0 for t in ['ref_methods', 'ref_statuses', 'ref_units', 'ref_kato'])
    data_ok = all(counts[t] > 0 for t in ['announcements', 'lots', 'contracts'])

    if refs_ok and data_ok:
        print("\n  Система готова к работе!")
        print("  Запустите: python3 run.py server")
    elif refs_ok:
        print("\n  Справочники загружены. Нужно загрузить данные:")
        print("  Запустите: python3 run.py load")
    else:
        print("\n  Нужна первичная загрузка:")
        print("  Запустите: python3 run.py")

    print("=" * 50 + "\n")

async def load_refs():
    """Загружает справочники."""
    from src.ingestion.seed_refs import seed_all_refs
    counts = get_data_counts()
    if counts['ref_methods'] > 0 and counts['ref_kato'] > 0:
        logger.info("Справочники уже загружены, пропускаю")
        return
    logger.info("Загрузка справочников...")
    await seed_all_refs()


async def load_data():
    """Загружает основные данные."""
    from src.ingestion.data_loader import load_all_data
    counts = get_data_counts()
    if counts['announcements'] > 1000 and counts['lots'] > 1000 and counts['contracts'] > 1000:
        logger.info(f"Данные уже загружены (announcements={counts['announcements']:,}, "
                    f"lots={counts['lots']:,}, contracts={counts['contracts']:,}), пропускаю")
        return
    logger.info("Загрузка данных из API (это займёт 10-30 минут)...")
    await load_all_data()


def run_etl():
    """Запускает ETL пайплайн."""
    from src.etl.pipeline import run_etl_pipeline
    logger.info("Запуск ETL пайплайна...")
    run_etl_pipeline()

def start_server():
    """Запускает FastAPI сервер."""
    from src.config import app_config
    import uvicorn
    logger.info(f"Запуск сервера на http://localhost:{app_config.port}")
    logger.info(f"Откройте в браузере: http://localhost:{app_config.port}")
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=app_config.port, reload=False)

def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "full"

    if command == "status":
        show_status()
        return

    if not check_docker_postgres():
        sys.exit(1)

    if command == "server":
        start_server()

    elif command == "load":
        asyncio.run(load_refs())
        asyncio.run(load_data())
        logger.info("Загрузка завершена!")
        show_status()

    elif command == "etl":
        run_etl()

    elif command == "full":
        asyncio.run(load_refs())
        asyncio.run(load_data())
        run_etl()
        show_status()
        start_server()

    else:
        print(__doc__)


if __name__ == "__main__":
    main()
