"""
Airflow DAG: Ежедневная инкрементальная синхронизация данных goszakup.gov.kz.

Запускается каждый день в 06:00 UTC (12:00 по Астане):
  1. incremental_sync — журнал изменений → дозагрузка обновлённых записей
  2. run_etl          — очистка + обогащение данных
  3. refresh_views    — обновление materialized views
"""
import asyncio
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2

logger = logging.getLogger(__name__)


default_args = {
    "owner": "goszakup",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

dag = DAG(
    dag_id="goszakup_daily_sync",
    default_args=default_args,
    description="Daily incremental sync from goszakup.gov.kz OWS v3 API",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["goszakup", "etl", "sync", "daily"],
)

def _get_conn():
    """Creates a psycopg2 connection using Airflow environment variables."""
    import os
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "goszakup"),
        user=os.environ.get("POSTGRES_USER", "goszakup"),
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _ensure_sync_journal(conn):
    """Creates sync_journal table if it does not exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_journal (
                id          SERIAL PRIMARY KEY,
                sync_ts     TIMESTAMP NOT NULL DEFAULT NOW(),
                status      VARCHAR(20) NOT NULL DEFAULT 'running',
                entities    JSONB,
                total_count INTEGER DEFAULT 0,
                error_msg   TEXT,
                created_at  TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
    conn.commit()


def _get_last_sync_ts(conn) -> str:
    """
    Reads last successful sync timestamp from sync_journal table.
    Falls back to 24 hours ago if no record exists.
    """
    fallback = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        _ensure_sync_journal(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(sync_ts)
                FROM sync_journal
                WHERE status = 'completed'
            """)
            row = cur.fetchone()
            if row and row[0]:
                return row[0].strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        conn.rollback()
    return fallback


def _record_sync(conn, status: str, entities: dict, total: int, error: str | None = None):
    """Write a sync record into sync_journal."""
    import json
    try:
        _ensure_sync_journal(conn)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sync_journal (status, entities, total_count, error_msg)
                   VALUES (%s, %s, %s, %s)""",
                (status, json.dumps(entities, ensure_ascii=False), total, error),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to record sync journal: {e}")


def _run_async(coro):
    """
    Runs an async coroutine safely — compatible with Airflow's
    execution model where an event loop may or may not exist.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    else:
        return asyncio.run(coro)

ENTITY_MAP = {
    "TrdBuy": {
        "fetch": "fetch_announcements",
        "load": "load_announcements",
        "filter_key": "org_bin",
    },
    "Lots": {
        "fetch": "fetch_lots",
        "load": "load_lots",
        "filter_key": "customer_bin",
    },
    "Contract": {
        "fetch": "fetch_contracts",
        "load": "load_contracts",
        "filter_key": "customer_bin",
    },
    "Plans": {
        "fetch": "fetch_plans",
        "load": "load_plans",
        "filter_key": "subject_bin",
    },
    "Subjects": {
        "fetch": "fetch_subjects",
        "load": "load_subjects",
        "filter_key": "bin",
    },
}

JOURNAL_QUERY = """
query($limit: Int, $after: Int) {
    Journal(limit: $limit, after: $after, filter: {indexDate: "%s"}) {
        id entityType entityId
        indexDate
    }
}
"""

MATERIALIZED_VIEWS = [
    "mv_price_statistics",
    "mv_volume_trends",
    "mv_supplier_stats",
    "mv_regional_coefficients",
    "mv_data_overview",
]

def _incremental_sync(**context):
    """
    Reads the API journal for changes since last sync, fetches
    updated records for each changed entity, and upserts them
    into PostgreSQL.
    """
    from src.config import TARGET_BINS
    from src.ingestion.api_client import GoszakupClient
    from src.ingestion import data_loader

    conn = _get_conn()

    try:
        since = _get_last_sync_ts(conn)
        logger.info(f"Incremental sync since: {since}")

        async def _do_sync():
            client = GoszakupClient()
            try:
                journal_query = JOURNAL_QUERY % since
                try:
                    journal_entries = await client.fetch_all(
                        "Journal", journal_query, max_records=10000
                    )
                except Exception as e:
                    logger.warning(
                        f"Journal endpoint unavailable ({e}). "
                        "Falling back to full re-fetch."
                    )
                    journal_entries = None

                counts = {}

                if journal_entries:
                    changed: dict[str, set[int]] = {}
                    for entry in journal_entries:
                        entity_type = entry.get("entityType", "")
                        entity_id = entry.get("entityId")
                        if entity_type and entity_id is not None:
                            changed.setdefault(entity_type, set()).add(entity_id)

                    logger.info(
                        f"Journal: {len(journal_entries)} changes across "
                        f"{len(changed)} types: {list(changed.keys())}"
                    )

                    for entity_type, entity_ids in changed.items():
                        mapping = ENTITY_MAP.get(entity_type)
                        if not mapping:
                            logger.warning(f"Unknown entity type: {entity_type}")
                            continue

                        fetch_method = getattr(client, mapping["fetch"])
                        load_fn = getattr(data_loader, mapping["load"])
                        filter_key = mapping["filter_key"]
                        total = 0

                        for bin_code in TARGET_BINS:
                            kwargs = {filter_key: bin_code}
                            records = await fetch_method(**kwargs)
                            relevant = [
                                r for r in records
                                if r.get("id") in entity_ids
                            ]
                            if relevant:
                                cnt = load_fn(conn, relevant)
                                total += cnt
                                logger.info(
                                    f"  {entity_type} BIN={bin_code}: "
                                    f"upserted {cnt}/{len(relevant)}"
                                )

                        counts[entity_type] = total
                        logger.info(f"{entity_type}: {total} records synced")

                else:
                    logger.info("Full re-fetch for all TARGET_BINS")

                    for entity_type, mapping in ENTITY_MAP.items():
                        fetch_method = getattr(client, mapping["fetch"])
                        load_fn = getattr(data_loader, mapping["load"])
                        filter_key = mapping["filter_key"]
                        total = 0

                        for bin_code in TARGET_BINS:
                            kwargs = {filter_key: bin_code, "max_records": 500}
                            records = await fetch_method(**kwargs)
                            if records:
                                cnt = load_fn(conn, records)
                                total += cnt

                        counts[entity_type] = total
                        logger.info(f"{entity_type}: {total} records (fallback)")

                return counts
            finally:
                await client.close()

        counts = _run_async(_do_sync())
        total_all = sum(counts.values())
        _record_sync(conn, "completed", counts, total_all)

        logger.info(f"Incremental sync completed. Total upserted: {total_all}")
        logger.info(f"  Breakdown: {counts}")

    except Exception as e:
        logger.error(f"Incremental sync failed: {e}")
        _record_sync(conn, "failed", {}, 0, str(e))
        raise
    finally:
        conn.close()


def _run_etl(**context):
    """Runs the full ETL pipeline (clean + enrich)."""
    from src.etl.pipeline import run_etl_pipeline

    logger.info("Starting ETL pipeline...")
    run_etl_pipeline()
    logger.info("ETL pipeline completed.")


def _refresh_views(**context):
    """Refreshes all materialized views concurrently-safely."""
    conn = _get_conn()
    try:
        for view_name in MATERIALIZED_VIEWS:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}"
                    )
                conn.commit()
                logger.info(f"Refreshed: {view_name}")
            except psycopg2.errors.ObjectNotInPrerequisiteState:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                conn.commit()
                logger.info(f"Refreshed (blocking): {view_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to refresh {view_name}: {e}")
                raise
    finally:
        conn.close()


incremental_sync = PythonOperator(
    task_id="incremental_sync",
    python_callable=_incremental_sync,
    dag=dag,
)

run_etl = PythonOperator(
    task_id="run_etl",
    python_callable=_run_etl,
    dag=dag,
)

refresh_views = PythonOperator(
    task_id="refresh_views",
    python_callable=_refresh_views,
    dag=dag,
)

incremental_sync >> run_etl >> refresh_views
