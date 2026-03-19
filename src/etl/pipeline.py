"""
ETL Pipeline: Оркестрация полного цикла обработки данных.
Последовательность: Clean → Enrich → Refresh Views.
"""
import logging

from src.etl.cleaner import run_cleaning
from src.etl.enricher import run_enrichment

logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """Запускает полный ETL пайплайн."""
    logger.info("=" * 60)
    logger.info("ETL PIPELINE START")
    logger.info("=" * 60)

    # 1. Очистка данных
    run_cleaning()

    # 2. Обогащение + обновление views
    run_enrichment()

    logger.info("=" * 60)
    logger.info("ETL PIPELINE COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_etl_pipeline()
