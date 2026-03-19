"""
Конфигурация проекта. Все настройки читаются из .env файла.
"""
import os
from pathlib import Path
from dataclasses import dataclass

from dotenv import load_dotenv

# Корень проекта
ROOT_DIR = Path(__file__).parent.parent
load_dotenv(ROOT_DIR / ".env")


@dataclass
class APIConfig:
    token: str = os.getenv("GOSZAKUP_API_TOKEN", "")
    base_url: str = os.getenv("GOSZAKUP_API_BASE_URL", "https://ows.goszakup.gov.kz")
    page_size: int = 50
    max_retries: int = 3
    retry_delay: float = 2.0
    requests_per_second: float = 5.0


@dataclass
class DBConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "goszakup")
    user: str = os.getenv("POSTGRES_USER", "goszakup")
    password: str = os.getenv("POSTGRES_PASSWORD", "")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def async_dsn(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class LLMConfig:
    provider: str = os.getenv("LLM_PROVIDER", "openai_compatible")
    base_url: str = os.getenv("LLM_BASE_URL", "https://nitec-ai.kz/api")
    api_key: str = os.getenv("LLM_API_KEY", "")
    model: str = os.getenv("LLM_MODEL", "openai/gpt-oss-120b")
    max_tokens: int = 4096
    temperature: float = 0.0


@dataclass
class AppConfig:
    port: int = int(os.getenv("APP_PORT", "8000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    api_key: str = os.getenv("APP_API_KEY", "")
    cors_origins: str = os.getenv("CORS_ORIGINS", "")


TARGET_BINS: list[str] = [
    "000740001307",
    "020240002363",
    "020440003656",
    "030440003698",
    "050740004819",
    "051040005150",
    "100140011059",
    "120940001946",
    "140340016539",
    "150540000186",
    "171041003124",
    "210240019348",
    "210240033968",
    "210941010761",
    "230740013340",
    "231040023028",
    "780140000023",
    "900640000128",
    "940740000911",
    "940940000384",
    "960440000220",
    "970940001378",
    "971040001050",
    "980440001034",
    "981140001551",
    "990340005977",
    "990740002243",
]

DATA_YEARS = [2024, 2025, 2026]

INVALID_CONTRACT_STATUS_CODES = {"330"}

LOT_WARNING_STATUS_CODES = {"370", "410"}
LOT_SUCCESS_STATUS_CODES = {"360"}

api_config = APIConfig()
db_config = DBConfig()
llm_config = LLMConfig()
app_config = AppConfig()
