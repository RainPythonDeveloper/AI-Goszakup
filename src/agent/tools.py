"""
AI Agent Tools — инструменты, которые LLM может вызывать.

Ключевой принцип ТЗ: LLM НЕ считает — она делегирует вычисления SQL/Python.
Каждый tool выполняет конкретную аналитическую задачу.

Улучшения:
- Connection pooling (ThreadedConnectionPool)
- Input validation на всех tool parameters
- Actionable error messages для LLM
- evaluate_lot_price разбит на приватные функции
"""
import logging
from datetime import date

import psycopg2
from psycopg2 import pool, sql as psycopg2_sql
from psycopg2.extras import RealDictCursor

from src.config import db_config, DATA_YEARS, INVALID_CONTRACT_STATUS_CODES, LOT_WARNING_STATUS_CODES
from src.analytics.fair_price import calculate_fair_price
from src.analytics.anomaly_detector import (
    detect_iqr_anomalies,
    detect_iforest_anomalies,
    detect_consensus_anomalies,
    detect_volume_anomalies as _detect_volume_anomalies,
    detect_supplier_concentration,
)

logger = logging.getLogger(__name__)

# ============================================================
# Connection Pool
# ============================================================

_connection_pool: pool.ThreadedConnectionPool | None = None


def _get_pool() -> pool.ThreadedConnectionPool:
    """Ленивая инициализация пула соединений."""
    global _connection_pool
    if _connection_pool is None or _connection_pool.closed:
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=db_config.host,
            port=db_config.port,
            dbname=db_config.database,
            user=db_config.user,
            password=db_config.password,
        )
    return _connection_pool


def get_conn():
    """Получает соединение из пула."""
    return _get_pool().getconn()


def put_conn(conn):
    """Возвращает соединение в пул."""
    try:
        _get_pool().putconn(conn)
    except Exception:
        pass

PORTAL_BASE = "https://goszakup.gov.kz/ru"


def announcement_url(number_anno: str) -> str:
    """Ссылка на объявление на портале госзакупок.

    number_anno может содержать суффикс версии (например '16204999-1').
    Портал использует только числовую часть без суффикса: /announce/index/16204999
    """
    base = number_anno.split("-")[0] if "-" in number_anno else number_anno
    return f"{PORTAL_BASE}/announce/index/{base}"


def lot_url(number_anno: str) -> str:
    """Ссылка на вкладку лотов объявления на портале госзакупок."""
    return f"{announcement_url(number_anno)}?tab=lots"


def contract_url(source_id) -> str:
    """Ссылка на договор на портале госзакупок (по source_id — ID из API OWS v3)."""
    return f"{PORTAL_BASE}/egzcontract/cpublic/show/{source_id}"


def _tool_error(message: str, suggestion: str | None = None) -> dict:
    """Формирует actionable error для LLM."""
    error = {"error": message}
    if suggestion:
        error["suggestion"] = suggestion
    return error


TOOL_DEFINITIONS = [
    {
        "name": "execute_sql",
        "description": (
            "Выполняет SELECT SQL-запрос к базе данных госзакупок и возвращает результат. "
            "Доступные таблицы: subjects, announcements, lots, contracts, contract_subjects, "
            "plans, applications, payments, contract_acts, "
            "ref_methods, ref_statuses, ref_units, ref_kato, ref_enstru, ref_currencies, "
            "inflation_index, "
            "mv_price_statistics, mv_volume_trends, mv_supplier_stats, "
            "mv_regional_coefficients, mv_data_overview. "
            "ТОЛЬКО SELECT запросы. Максимум 100 строк."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL SELECT запрос к базе данных"
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_fair_price",
        "description": (
            "Рассчитывает справедливую (рыночную) цену для товара/работы/услуги по коду ENSTRU. "
            "Использует формулу: FairPrice = MedianPrice × RegionalCoeff × InflationIndex × SeasonalCoeff. "
            "Возвращает: fair_price, median, Q1, Q3, sample_size, confidence."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "enstru_code": {
                    "type": "string",
                    "description": "Код ENSTRU (ТРУ), например '441130.000.000001'"
                },
                "region": {
                    "type": "string",
                    "description": "Регион доставки (название области), опционально"
                }
            },
            "required": ["enstru_code"]
        }
    },
    {
        "name": "evaluate_lot_price",
        "description": (
            "ПОЛНАЯ оценка адекватности цены конкретного лота по его номеру. "
            "Находит лот в БД, рассчитывает Fair Price, сравнивает с аналогичными контрактами "
            "в том же регионе (городе поставки), возвращает вердикт с ссылками. "
            "Используй этот инструмент когда пользователь просит оценить цену конкретного лота."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "lot_number": {
                    "type": "string",
                    "description": "Номер лота, например '39939053-ОЛ-ЗЦП1'"
                }
            },
            "required": ["lot_number"]
        }
    },
    {
        "name": "detect_anomalies",
        "description": (
            "Выявляет ценовые аномалии. КРИТИЧЕСКИ ВАЖНО: если пользователь указал конкретный ENSTRU код — "
            "ОБЯЗАТЕЛЬНО передай enstru_code! Без enstru_code инструмент ищет по ВСЕЙ базе. "
            "Методы: 'iqr' (IQR статистический), 'isolation_forest' (ML Isolation Forest), "
            "'consensus' (пересечение IQR и IF — высокая уверенность). "
            "Возвращает список аномалий с severity, deviation %, confidence, details и ссылками на контракты."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "enstru_code": {
                    "type": "string",
                    "description": "Код ENSTRU для фильтрации. ОБЯЗАТЕЛЬНО передай если пользователь указал ENSTRU! Без этого параметра ищет по ВСЕЙ базе"
                },
                "method": {
                    "type": "string",
                    "description": "Метод: 'consensus' (IQR+IF, по умолчанию), 'iqr', 'isolation_forest'"
                },
                "threshold": {
                    "type": "number",
                    "description": "IQR множитель (1.5=умеренные, 3.0=экстремальные). По умолчанию 1.5"
                }
            }
        }
    },
    {
        "name": "check_supplier_concentration",
        "description": (
            "Проверяет концентрацию поставщиков: выявляет заказчиков, "
            "у которых один поставщик получает более 80% от общей суммы закупок. "
            "Индикатор возможных коррупционных рисков."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "customer_bin": {
                    "type": "string",
                    "description": "БИН заказчика для проверки (если пусто — проверяет всех)"
                },
                "threshold_pct": {
                    "type": "number",
                    "description": "Порог концентрации в процентах (по умолчанию 80)"
                }
            }
        }
    },
    {
        "name": "detect_volume_anomalies",
        "description": (
            "Выявляет аномальные объёмы закупок: нетипичное завышение количества ТРУ "
            "по сравнению с предыдущими годами. Сравнивает годовой объём закупки "
            "с средним за все годы для каждого (ENSTRU, заказчик). "
            "Возвращает аномалии с деталями и ссылками на контракты."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "threshold": {
                    "type": "number",
                    "description": "Множитель порога (по умолчанию 2.0 — объём в 2+ раза больше среднего)"
                }
            }
        }
    },
    {
        "name": "get_data_overview",
        "description": (
            "Возвращает общую статистику по загруженным данным: "
            "количество записей в каждой таблице."
        ),
        "parameters": {
            "type": "object",
            "properties": {}
        }
    },
]


def execute_sql(query: str) -> dict:
    """Выполняет SQL SELECT запрос (только чтение)."""
    if not query or not query.strip():
        return _tool_error(
            "SQL запрос не может быть пустым",
            "Передай непустой SELECT запрос, например: SELECT COUNT(*) FROM contracts"
        )

    if len(query) > 5000:
        return _tool_error(
            "SQL запрос слишком длинный (максимум 5000 символов)",
            "Упрости запрос или разбей на несколько"
        )

    normalized = query.strip().upper()
    if not normalized.startswith("SELECT"):
        return _tool_error(
            "Разрешены только SELECT запросы",
            "Начни запрос с SELECT. Пример: SELECT * FROM contracts LIMIT 10"
        )

    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE",
                 "GRANT", "REVOKE", "COPY", "EXECUTE", "CALL"]
    for word in forbidden:
        if word in normalized:
            return _tool_error(f"Запрещённая операция: {word}")

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query + " LIMIT 100" if "LIMIT" not in normalized else query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []

        result_rows = []
        for row in rows:
            clean_row = {}
            for key, val in dict(row).items():
                if isinstance(val, (date,)):
                    clean_row[key] = str(val)
                elif val is None:
                    clean_row[key] = None
                else:
                    clean_row[key] = val
            if "number_anno" in clean_row and clean_row["number_anno"]:
                clean_row["portal_url"] = announcement_url(str(clean_row["number_anno"]))
            if "contract_id" in clean_row and clean_row["contract_id"]:
                clean_row["portal_url"] = contract_url(clean_row["contract_id"])
            result_rows.append(clean_row)

        return {
            "columns": columns,
            "rows": result_rows,
            "row_count": len(result_rows),
        }
    except Exception as e:
        error_str = str(e)
        if "does not exist" in error_str:
            return _tool_error(
                f"Таблица или колонка не найдена: {error_str[:150]}",
                "Проверь имена таблиц и колонок. "
                "Доступные таблицы: subjects, announcements, lots, contracts, "
                "contract_subjects, plans, applications, ref_methods, ref_statuses, "
                "ref_kato, ref_enstru, ref_units, mv_price_statistics, mv_volume_trends, "
                "mv_supplier_stats, mv_regional_coefficients, inflation_index"
            )
        if "syntax error" in error_str.lower():
            return _tool_error(
                f"SQL синтаксическая ошибка: {error_str[:150]}",
                "Проверь синтаксис SQL запроса. Убедись что все скобки и кавычки закрыты."
            )
        return _tool_error(f"Ошибка выполнения SQL: {error_str[:200]}")
    finally:
        put_conn(conn)


def get_fair_price(enstru_code: str, region: str | None = None) -> dict:
    """Вычисляет Fair Price для ENSTRU кода."""
    if not enstru_code or not enstru_code.strip():
        return _tool_error(
            "Код ENSTRU не может быть пустым",
            "Передай код ENSTRU, например: '441130.000.000001'. "
            "Чтобы найти код, используй execute_sql: "
            "SELECT DISTINCT enstru_code, name_ru FROM lots WHERE name_ru ILIKE '%ключевое_слово%'"
        )

    result = calculate_fair_price(enstru_code.strip(), region=region)

    if result is None:
        suggestion = (
            f"Попробуй другой регион или убери фильтр по региону. "
            f"Чтобы проверить наличие данных: "
            f"SELECT COUNT(*) FROM mv_price_statistics WHERE enstru_code = '{enstru_code}'"
        )
        return _tool_error(
            f"Нет ценовых данных для ENSTRU={enstru_code}, регион={region}",
            suggestion
        )

    return {
        "enstru_code": result.enstru_code,
        "region": result.region,
        "fair_price": result.fair_price,
        "formula": "FairPrice = Median × RegCoeff × CPI × SeasonCoeff",
        "median_price": result.median_price,
        "regional_coefficient": result.regional_coeff,
        "inflation_index": result.inflation_index,
        "seasonal_coefficient": result.seasonal_coeff,
        "confidence": result.confidence,
        "sample_size": result.sample_size,
        "price_range": {
            "q1": result.q1,
            "q3": result.q3,
            "min": result.min_price,
            "max": result.max_price,
        },
        "ci_lower": result.ci_lower,
        "ci_upper": result.ci_upper,
        "fallback_flags": result.fallback_flags,
    }


_LOT_QUERY = """
    SELECT l.id, l.lot_number, l.name_ru, l.name_kz, l.amount, l.count,
           l.price_per_unit, l.enstru_code, l.customer_bin,
           l.delivery_kato, l.status_id, l.unit_id,
           a.number_anno, a.name_ru as announcement_name, a.name_kz as announcement_name_kz,
           s.name_ru as customer_name, s.name_kz as customer_name_kz,
           rk.name_ru as kato_name, rk.name_kz as kato_name_kz, rk.region as kato_region,
           ru.name_ru as unit_name, ru.name_kz as unit_name_kz, ru.code as unit_code,
           rs.name_ru as status_name, rs.name_kz as status_name_kz, rs.code as status_code
    FROM lots l
    LEFT JOIN announcements a ON l.announcement_id = a.id
    LEFT JOIN subjects s ON l.customer_bin = s.bin
    LEFT JOIN ref_kato rk ON l.delivery_kato = rk.code
    LEFT JOIN ref_units ru ON l.unit_id = ru.id
    LEFT JOIN ref_statuses rs ON l.status_id = rs.id
"""

_INVALID_CODES_SQL = ", ".join(f"'{c}'" for c in INVALID_CONTRACT_STATUS_CODES)
_VALID_CONTRACT_FILTER = f"""
    AND c.status_id NOT IN (
        SELECT rs2.id FROM ref_statuses rs2
        WHERE rs2.code IN ({_INVALID_CODES_SQL}) AND rs2.entity_type = 'contract'
    )
"""


def _find_lot(conn, lot_number: str) -> dict | None:
    """Ищет лот: точное совпадение, затем fuzzy поиск."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"{_LOT_QUERY} WHERE l.lot_number = %s LIMIT 1", (lot_number,))
        lot = cur.fetchone()

    if lot:
        return dict(lot)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"{_LOT_QUERY} WHERE l.lot_number ILIKE %s LIMIT 5", (f"%{lot_number}%",))
        fuzzy_results = cur.fetchall()

    if not fuzzy_results:
        return None

    lot = dict(fuzzy_results[0])
    if len(fuzzy_results) > 1:
        lot["other_matches"] = [dict(r)["lot_number"] for r in fuzzy_results[1:]]
    return lot


def _assess_price(lot_price: float | None, fair_price: float,
                   is_unit_mixed: bool = False,
                   is_unit_converted: bool = False) -> dict | None:
    """Оценивает адекватность цены лота относительно справедливой.

    is_unit_mixed=True → единицы смешаны без конвертации, вердикт НЕНАДЁЖЕН.
    is_unit_converted=True → конвертация применена, вердикт с оговоркой.
    """
    if not lot_price or fair_price <= 0:
        return None

    deviation_pct = ((lot_price - fair_price) / fair_price) * 100

    if is_unit_mixed:
        if abs(deviation_pct) <= 15:
            verdict = "возможно адекватная (ОЦЕНКА НЕНАДЁЖНА — смешаны разные единицы)"
        elif deviation_pct > 0:
            verdict = "возможно завышена (ОЦЕНКА НЕНАДЁЖНА — смешаны разные единицы)"
        else:
            verdict = "возможно занижена (ОЦЕНКА НЕНАДЁЖНА — смешаны разные единицы)"
    elif is_unit_converted:
        base = (
            "адекватная" if abs(deviation_pct) <= 15
            else "незначительно завышена" if 15 < deviation_pct <= 30
            else "завышена" if 30 < deviation_pct <= 100
            else "значительно завышена" if deviation_pct > 100
            else "незначительно занижена" if -30 < deviation_pct <= -15
            else "занижена" if -100 < deviation_pct <= -30
            else "значительно занижена"
        )
        verdict = f"{base} (с учётом конвертации единиц)"
    else:
        verdict = (
            "адекватная" if abs(deviation_pct) <= 15
            else "незначительно завышена" if 15 < deviation_pct <= 30
            else "завышена" if 30 < deviation_pct <= 100
            else "значительно завышена" if deviation_pct > 100
            else "незначительно занижена" if -30 < deviation_pct <= -15
            else "занижена" if -100 < deviation_pct <= -30
            else "значительно занижена"
        )

    return {
        "lot_price": lot_price,
        "fair_price": fair_price,
        "deviation_pct": round(deviation_pct, 1),
        "verdict": verdict,
        "is_unit_mixed": is_unit_mixed,
        "is_unit_converted": is_unit_converted,
    }


def _format_analogue(r: dict, conversion_factors: dict | None = None,
                     target_unit_id: int | None = None) -> dict:
    """Форматирует аналогичный контракт для вывода.

    Если conversion_factors указаны и единица отличается от целевой —
    добавляет converted_price (цена в целевой единице).
    """
    price = float(r["price_per_unit"]) if r["price_per_unit"] else None
    result = {
        "contract_id": r.get("contract_id"),
        "contract_number": r["contract_number"],
        "price_per_unit": price,
        "quantity": float(r["quantity"]) if r["quantity"] else None,
        "total_price": float(tp) if (tp := r.get("total_price")) else None,
        "sign_date": str(r["sign_date"]) if r["sign_date"] else None,
        "customer_name": r["customer_name"],
        "customer_name_kz": r.get("customer_name_kz"),
        "portal_url": contract_url(r["contract_id"]) if r.get("contract_id") else None,
    }
    if r.get("unit_name"):
        result["unit_name"] = r["unit_name"]
    if r.get("unit_name_kz"):
        result["unit_name_kz"] = r["unit_name_kz"]

    if (conversion_factors and price and target_unit_id is not None
            and r.get("unit_id") is not None):
        uid = int(r["unit_id"])
        if uid != target_unit_id and uid in conversion_factors:
            factor = conversion_factors[uid]["factor"]
            result["converted_price"] = round(price / factor, 2)
            result["conversion_note"] = (
                f"1 {r.get('unit_name', 'ед.')} ≈ {factor:.1f} "
                f"{conversion_factors[uid].get('target_unit_name', 'целевых ед.')}"
            )

    return result


def _diversify_analogues(rows: list[dict], max_per_customer: int = 3) -> list[dict]:
    """Ограничивает количество записей от одного заказчика для репрезентативности."""
    customer_counts: dict[str, int] = {}
    result = []
    for r in rows:
        cbin = r.get("customer_bin", "")
        customer_counts[cbin] = customer_counts.get(cbin, 0) + 1
        if customer_counts[cbin] <= max_per_customer:
            result.append(r)
    return result


def _find_analogues(conn, enstru_code: str, customer_bin: str, kato_prefix: str,
                    unit_id: int | None = None,
                    conversion_factors: dict | None = None) -> dict:
    """
    Ищет аналогичные контракты: сначала в регионе, потом без фильтра.

    Применяет диверсификацию: max 3 записи от одного заказчика,
    чтобы избежать selection bias (доминирование одной организации).

    Если unit_id указан — фильтрует по единице измерения. Если аналогов < 3,
    делает fallback на все единицы + помечает в результате.
    """
    years = ", ".join(str(y) for y in DATA_YEARS)
    base_query = f"""
        SELECT cs.price_per_unit, cs.quantity, cs.total_price,
               c.source_id as contract_id, c.contract_number, c.contract_sum, c.sign_date,
               c.customer_bin, s.name_ru as customer_name, s.name_kz as customer_name_kz,
               ru.name_ru as unit_name, ru.name_kz as unit_name_kz, cs.unit_id
        FROM contract_subjects cs
        JOIN contracts c ON cs.contract_id = c.id
        LEFT JOIN subjects s ON c.customer_bin = s.bin
        LEFT JOIN ref_units ru ON cs.unit_id = ru.id
        WHERE cs.enstru_code = %s
          AND c.customer_bin != %s
          AND cs.price_per_unit IS NOT NULL
          AND cs.price_per_unit > 0
          AND EXTRACT(YEAR FROM c.sign_date) IN ({years})
          {_VALID_CONTRACT_FILTER}
    """

    unit_filter = ""
    unit_bypassed = False
    if unit_id is not None:
        unit_filter = " AND cs.unit_id = %s"

    def _run_query(q_suffix: str, params: tuple) -> list[dict]:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(base_query + unit_filter + q_suffix, params)
            return [dict(r) for r in cur.fetchall()]

    def _build_result(analogues_raw: list[dict], region_label: str) -> dict:
        diversified = _diversify_analogues(analogues_raw)[:10]
        unique_customers = len({r["customer_bin"] for r in diversified})
        res = {
            "count": len(diversified),
            "unique_customers": unique_customers,
            "region_filter": region_label,
            "items": [
                _format_analogue(r, conversion_factors=conversion_factors,
                                 target_unit_id=unit_id)
                for r in diversified
            ],
        }
        if unit_bypassed:
            if conversion_factors:
                res["unit_filter_note"] = (
                    "Аналоги включают записи с другими единицами измерения. "
                    "Для каждого аналога указана converted_price — цена, "
                    "приведённая к единице измерения лота."
                )
            else:
                res["unit_filter_note"] = (
                    "Аналоги найдены БЕЗ фильтра по единице измерения "
                    "(менее 3 аналогов с той же единицей). Конвертация единиц "
                    "невозможна — цены в РАЗНЫХ единицах, сравнение НЕНАДЁЖНО."
                )
        return res

    base_params = (enstru_code, customer_bin)
    if unit_id is not None:
        base_params = (enstru_code, customer_bin, unit_id)

    if kato_prefix:
        region_params = base_params + (f"{kato_prefix}%",)
        analogues_raw = _run_query(
            " AND cs.delivery_kato LIKE %s ORDER BY c.sign_date DESC LIMIT 30",
            region_params
        )

        if unit_id is not None and len(analogues_raw) < 3:
            unit_filter = ""
            unit_bypassed = True
            analogues_raw = _run_query(
                " AND cs.delivery_kato LIKE %s ORDER BY c.sign_date DESC LIMIT 30",
                (enstru_code, customer_bin, f"{kato_prefix}%")
            )

        if analogues_raw:
            return _build_result(analogues_raw, f"КАТО начинается с {kato_prefix}")

    analogues_raw = _run_query(
        " ORDER BY c.sign_date DESC LIMIT 30",
        base_params if not unit_bypassed else (enstru_code, customer_bin)
    )
    if unit_id is not None and not unit_bypassed and len(analogues_raw) < 3:
        unit_filter = ""
        unit_bypassed = True
        analogues_raw = _run_query(
            " ORDER BY c.sign_date DESC LIMIT 30",
            (enstru_code, customer_bin)
        )

    region_label = (
        "без фильтра по региону (в целевом регионе аналогов нет)" if kato_prefix
        else "все регионы"
    )
    return _build_result(analogues_raw, region_label)


def _resolve_enstru_by_name(conn, name_ru: str | None) -> str | None:
    """Находит ENSTRU-код по названию лота через contract_subjects.

    Ищет наиболее частый enstru_code среди предметов договоров
    с похожим названием (trigram similarity).
    """
    if not name_ru or len(name_ru.strip()) < 5:
        return None

    with conn.cursor() as cur:
        cur.execute("""
            SELECT cs.enstru_code, COUNT(*) as cnt
            FROM contract_subjects cs
            JOIN contracts c ON cs.contract_id = c.id
            WHERE cs.enstru_code IS NOT NULL
              AND cs.enstru_code != '0'
              AND cs.enstru_code != ''
              AND EXTRACT(YEAR FROM c.sign_date) = ANY(%s::int[])
              AND cs.name_ru %% %s
            GROUP BY cs.enstru_code
            ORDER BY cnt DESC
            LIMIT 1
        """, (list(DATA_YEARS), name_ru.strip()))
        row = cur.fetchone()
        if row:
            return row[0]
    return None


def evaluate_lot_price(lot_number: str) -> dict:
    """
    Полная оценка адекватности цены лота.
    Оркестрирует: _find_lot → calculate_fair_price → _assess_price → _find_analogues
    """
    if not lot_number or not lot_number.strip():
        return _tool_error(
            "Номер лота не может быть пустым",
            "Передай номер лота, например: '39939053-ОЛ-ЗЦП1'. "
            "Чтобы найти лот: execute_sql(\"SELECT lot_number, name_ru FROM lots LIMIT 10\")"
        )

    conn = get_conn()
    try:
        lot = _find_lot(conn, lot_number.strip())
        if not lot:
            return _tool_error(
                f"Лот с номером '{lot_number}' не найден",
                "Проверь номер лота. Для поиска используй: "
                "execute_sql(\"SELECT lot_number, name_ru FROM lots WHERE lot_number ILIKE '%часть_номера%'\")"
            )

        result = {
            "lot": {
                "lot_number": lot["lot_number"],
                "name_ru": lot["name_ru"],
                "name_kz": lot.get("name_kz"),
                "planned_sum": float(lot["amount"]) if lot["amount"] else None,
                "quantity": float(lot["count"]) if lot["count"] else None,
                "price_per_unit": float(lot["price_per_unit"]) if lot["price_per_unit"] else None,
                "enstru_code": lot["enstru_code"],
                "customer_bin": lot["customer_bin"],
                "customer_name": lot["customer_name"],
                "customer_name_kz": lot.get("customer_name_kz"),
                "delivery_kato": lot["delivery_kato"],
                "kato_name": lot["kato_name"],
                "kato_name_kz": lot.get("kato_name_kz"),
                "kato_region": lot["kato_region"],
                "unit_id": lot.get("unit_id"),
                "unit_name": lot.get("unit_name"),
                "unit_name_kz": lot.get("unit_name_kz"),
                "unit_code": lot.get("unit_code"),
                "status_name": lot.get("status_name"),
                "status_name_kz": lot.get("status_name_kz"),
                "status_code": lot.get("status_code"),
            },
        }

        if lot.get("status_code") in LOT_WARNING_STATUS_CODES:
            result["lot"]["status_warning"] = (
                f"Статус лота: «{lot.get('status_name', 'N/A')}». "
                "Закупка не состоялась — цена может быть неактуальной."
            )

        if lot.get("other_matches"):
            result["lot"]["other_matches"] = lot["other_matches"]

        result["lot"]["pricing_note"] = (
            "Все цены из API без НДС. На портале (страница договора) цены отображаются с НДС (+12%). "
            "Сравнение лота и аналогов корректно — обе стороны без НДС."
        )

        if lot.get("number_anno"):
            number = str(lot["number_anno"])
            result["lot"]["announcement_url"] = announcement_url(number)
            result["lot"]["lot_url"] = lot_url(number)
            result["portal_links"] = {
                "announcement_url": announcement_url(number),
                "lot_url": lot_url(number),
                "note": "КОПИРУЙ эти ссылки в ответ ДОСЛОВНО. НЕ формируй URL самостоятельно.",
            }

        enstru_code = lot.get("enstru_code")
        if not enstru_code or enstru_code == "0":
            enstru_code = _resolve_enstru_by_name(conn, lot.get("name_ru"))
            if enstru_code:
                result["lot"]["enstru_code"] = enstru_code
                result["lot"]["enstru_source"] = "resolved_from_contract_subjects"

        region = lot.get("kato_region")

        if enstru_code:
            fair_result = calculate_fair_price(
                enstru_code, region=region, unit_id=lot.get("unit_id"), conn=conn,
                exclude_customer_bin=lot.get("customer_bin"),
            )
            if fair_result:
                median_records_out = []
                for mr in fair_result.median_records:
                    rec = {
                        "contract_id": mr.get("contract_id"),
                        "contract_number": mr.get("contract_number"),
                        "price_per_unit": mr.get("price_per_unit"),
                        "unit_name": mr.get("unit_name"),
                        "sign_date": mr.get("sign_date"),
                        "customer_name": mr.get("customer_name"),
                        "portal_url": contract_url(mr["contract_id"]) if mr.get("contract_id") else None,
                    }
                    if mr.get("converted"):
                        rec["original_price"] = mr.get("original_price")
                        rec["converted"] = True
                        rec["conversion_factor"] = mr.get("conversion_factor")
                    median_records_out.append(rec)

                conversion_info_out = None
                if fair_result.conversion_info:
                    conversion_info_out = {
                        str(uid): {
                            "factor": v["factor"],
                            "unit_name": v["unit_name"],
                            "interpretation": (
                                f"1 {v['unit_name']} ≈ {v['factor']:.1f} "
                                f"{fair_result.unit_name or 'целевых ед.'} по цене"
                            ),
                        }
                        for uid, v in fair_result.conversion_info.items()
                    }

                result["fair_price"] = {
                    "value": fair_result.fair_price,
                    "formula": "FairPrice = Median × RegCoeff × CPI × SeasonCoeff",
                    "median_price": fair_result.median_price,
                    "regional_coeff": fair_result.regional_coeff,
                    "inflation_index": fair_result.inflation_index,
                    "seasonal_coeff": fair_result.seasonal_coeff,
                    "confidence": fair_result.confidence,
                    "sample_size": fair_result.sample_size,
                    "q1": fair_result.q1,
                    "q3": fair_result.q3,
                    "ci_lower": fair_result.ci_lower,
                    "ci_upper": fair_result.ci_upper,
                    "unit_id": fair_result.unit_id,
                    "unit_name": fair_result.unit_name,
                    "fallback_flags": fair_result.fallback_flags,
                    "median_records": median_records_out,
                    "is_unit_converted": fair_result.is_unit_converted,
                    "is_unit_mixed": fair_result.is_unit_mixed,
                    "conversion_info": conversion_info_out,
                }

                lot_price = float(lot["price_per_unit"]) if lot.get("price_per_unit") else None
                assessment = _assess_price(
                    lot_price, fair_result.fair_price,
                    is_unit_mixed=fair_result.is_unit_mixed,
                    is_unit_converted=fair_result.is_unit_converted,
                )
                if assessment:
                    result["price_assessment"] = assessment
            else:
                result["fair_price"] = _tool_error(
                    f"Нет ценовых данных для ENSTRU={enstru_code}",
                    "Попробуй get_fair_price с другим регионом или без региона"
                )

            kato_prefix = (lot.get("delivery_kato") or "")[:2]
            conv_factors = fair_result.conversion_info if fair_result else None
            if conv_factors and fair_result and fair_result.unit_name:
                for v in conv_factors.values():
                    v["target_unit_name"] = fair_result.unit_name
            result["analogues"] = _find_analogues(
                conn, enstru_code, lot.get("customer_bin", ""), kato_prefix,
                unit_id=lot.get("unit_id"),
                conversion_factors=conv_factors,
            )
        else:
            result["fair_price"] = _tool_error(
                "Код ENSTRU не определён для данного лота",
                "Без ENSTRU-кода невозможно рассчитать справедливую цену. "
                "Попробуй найти аналоги вручную: "
                "execute_sql(\"SELECT cs.enstru_code, cs.name_ru, cs.price_per_unit "
                "FROM contract_subjects cs WHERE cs.name_ru ILIKE '%ключевое_слово%'\")"
            )

        return result

    except Exception as e:
        logger.error(f"evaluate_lot_price failed: {e}")
        return _tool_error(
            f"Ошибка оценки лота: {str(e)[:150]}",
            "Попробуй найти лот через execute_sql и затем вызвать get_fair_price отдельно"
        )
    finally:
        put_conn(conn)


def _get_data_period(conn) -> dict:
    """Возвращает фактический период данных в рамках скоупа проекта (DATA_YEARS)."""
    try:
        years = list(DATA_YEARS)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MIN(sign_date), MAX(sign_date), COUNT(*) "
                "FROM contracts "
                "WHERE sign_date IS NOT NULL "
                "  AND EXTRACT(YEAR FROM sign_date) = ANY(%s::int[])",
                (years,)
            )
            row = cur.fetchone()
            if row and row[0]:
                return {
                    "period_start": str(row[0]),
                    "period_end": str(row[1]),
                    "total_contracts": row[2],
                }
    except Exception:
        pass
    return {}


def detect_anomalies(enstru_code: str | None = None, method: str = "consensus",
                     threshold: float = 1.5) -> dict:
    """Выявляет ценовые аномалии (IQR, Isolation Forest, или consensus)."""
    valid_methods = ("iqr", "isolation_forest", "consensus")
    if method not in valid_methods:
        return _tool_error(
            f"Неизвестный метод: '{method}'",
            f"Доступные методы: {', '.join(valid_methods)}. Рекомендуется 'consensus'."
        )

    if threshold <= 0 or threshold > 10:
        return _tool_error(
            f"Некорректный threshold: {threshold}",
            "Threshold должен быть от 0.1 до 10.0. Рекомендуется 1.5 (умеренный) или 3.0 (только экстремальные)."
        )

    conn = get_conn()
    try:
        if method == "isolation_forest":
            anomalies = detect_iforest_anomalies(conn, enstru_code)
        elif method == "consensus":
            anomalies = detect_consensus_anomalies(conn, enstru_code, iqr_threshold=threshold)
        else:
            anomalies = detect_iqr_anomalies(conn, enstru_code, threshold)

        anomaly_details = []
        for a in sorted(anomalies, key=lambda x: abs(x.deviation_pct), reverse=True)[:20]:
            detail = {
                "record_id": a.record_id,
                "enstru_code": a.enstru_code,
                "price": a.price,
                "median_price": a.median_price,
                "deviation_pct": a.deviation_pct,
                "type": a.anomaly_type,
                "severity": a.severity,
                "confidence": a.confidence,
                "details": a.details,
            }

            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT c.source_id, c.contract_number, c.sign_date,
                               COALESCE(cs.name_ru, re.name_ru) AS product_name,
                               COALESCE(cs.name_kz, re.name_kz) AS product_name_kz
                        FROM contract_subjects cs
                        JOIN contracts c ON cs.contract_id = c.id
                        LEFT JOIN ref_enstru re ON cs.enstru_code = re.code
                        WHERE cs.id = %s
                        LIMIT 1
                    """, (a.record_id,))
                    row = cur.fetchone()
                    if row:
                        detail["contract_id"] = row[0]
                        detail["contract_number"] = row[1]
                        detail["sign_date"] = str(row[2]) if row[2] else None
                        detail["product_name"] = row[3]
                        detail["product_name_kz"] = row[4]
                        if row[0]:
                            detail["portal_url"] = contract_url(row[0])
            except Exception:
                pass

            anomaly_details.append(detail)

        data_period = _get_data_period(conn)

        return {
            "method": method,
            "total": len(anomalies),
            "showing": len(anomaly_details),
            "data_period": data_period,
            "anomalies": anomaly_details,
        }
    except Exception as e:
        logger.error(f"detect_anomalies failed: {e}")
        return _tool_error(
            f"Ошибка детекции аномалий: {str(e)[:150]}",
            "Попробуй другой метод или укажи конкретный enstru_code"
        )
    finally:
        put_conn(conn)


def check_supplier_concentration(customer_bin: str | None = None,
                                  threshold_pct: float = 80.0) -> dict:
    """Проверяет концентрацию поставщиков."""
    if threshold_pct < 1 or threshold_pct > 100:
        return _tool_error(
            f"Некорректный порог: {threshold_pct}%",
            "Порог должен быть от 1 до 100. Рекомендуется 80% (стандартный)."
        )

    if customer_bin and len(customer_bin) != 12:
        return _tool_error(
            f"Некорректный БИН: '{customer_bin}' (должен быть 12 цифр)",
            "БИН состоит из 12 цифр, например '000740001307'. "
            "Для поиска БИН: execute_sql(\"SELECT bin, name_ru FROM subjects WHERE is_customer = true LIMIT 10\")"
        )

    conn = get_conn()
    try:
        results = detect_supplier_concentration(conn, threshold_pct)

        if customer_bin:
            results = [r for r in results if r['customer_bin'] == customer_bin]

        return {
            "total": len(results),
            "alerts": results[:20],
        }
    except Exception as e:
        logger.error(f"check_supplier_concentration failed: {e}")
        return _tool_error(f"Ошибка проверки концентрации: {str(e)[:150]}")
    finally:
        put_conn(conn)


def detect_volume_anomalies_tool(threshold: float = 2.0) -> dict:
    """Выявляет аномальные объёмы закупок по годам."""
    if threshold < 1.0 or threshold > 100:
        return _tool_error(
            f"Некорректный порог: {threshold}",
            "Порог должен быть от 1.0 до 100. Рекомендуется 2.0 (объём в 2+ раза больше среднего)."
        )

    conn = get_conn()
    try:
        results = _detect_volume_anomalies(conn, year_threshold=threshold)
        sorted_results = sorted(results, key=lambda x: x.get('ratio', 0), reverse=True)[:20]

        for item in sorted_results:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(f"""
                        SELECT DISTINCT c.source_id as contract_id, c.contract_number,
                               c.contract_sum, c.sign_date
                        FROM contracts c
                        JOIN contract_subjects cs ON cs.contract_id = c.id
                        WHERE c.customer_bin = %s
                          AND EXTRACT(YEAR FROM c.sign_date) = %s
                          AND cs.enstru_code = %s
                          {_VALID_CONTRACT_FILTER}
                        ORDER BY c.contract_sum DESC
                        LIMIT 3
                    """, (item.get('customer_bin'), item.get('year'), item.get('enstru_code')))
                    contracts = cur.fetchall()

                if contracts:
                    item["top_contracts"] = [
                        {
                            "contract_id": r["contract_id"],
                            "contract_number": r["contract_number"],
                            "contract_sum": float(r["contract_sum"]) if r["contract_sum"] else None,
                            "sign_date": str(r["sign_date"]) if r["sign_date"] else None,
                            "portal_url": contract_url(r["contract_id"]) if r["contract_id"] else None,
                        }
                        for r in contracts
                    ]
            except Exception:
                pass

        data_period = _get_data_period(conn)

        return {
            "total": len(results),
            "showing": len(sorted_results),
            "data_period": data_period,
            "anomalies": sorted_results,
        }
    except Exception as e:
        logger.error(f"detect_volume_anomalies failed: {e}")
        return _tool_error(f"Ошибка детекции объёмных аномалий: {str(e)[:150]}")
    finally:
        put_conn(conn)


def get_data_overview() -> dict:
    """Возвращает обзор данных."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT entity, cnt FROM mv_data_overview ORDER BY entity")
            rows = cur.fetchall()
        return {entity: int(cnt) for entity, cnt in rows}
    except Exception:
        _SAFE_TABLES = ['subjects', 'announcements', 'lots', 'contracts',
                        'contract_subjects', 'plans', 'applications']
        result = {}
        try:
            with conn.cursor() as cur:
                for table in _SAFE_TABLES:
                    cur.execute(
                        psycopg2_sql.SQL("SELECT COUNT(*) FROM {}").format(
                            psycopg2_sql.Identifier(table)
                        )
                    )
                    result[table] = cur.fetchone()[0]
        except Exception as e:
            logger.error(f"get_data_overview fallback failed: {e}")
            return _tool_error("Не удалось получить статистику данных")
        return result
    finally:
        put_conn(conn)


TOOL_MAP = {
    "execute_sql": execute_sql,
    "get_fair_price": get_fair_price,
    "evaluate_lot_price": evaluate_lot_price,
    "detect_anomalies": detect_anomalies,
    "detect_volume_anomalies": detect_volume_anomalies_tool,
    "check_supplier_concentration": check_supplier_concentration,
    "get_data_overview": get_data_overview,
}


def call_tool(name: str, arguments: dict) -> dict:
    """Вызывает tool по имени с аргументами."""
    if name not in TOOL_MAP:
        return _tool_error(
            f"Неизвестный инструмент: '{name}'",
            f"Доступные инструменты: {', '.join(TOOL_MAP.keys())}"
        )

    try:
        return TOOL_MAP[name](**arguments)
    except TypeError as e:
        logger.error(f"Tool {name} argument error: {e}")
        return _tool_error(
            f"Неверные параметры для {name}: {str(e)[:150]}",
            f"Проверь параметры инструмента {name}"
        )
    except Exception as e:
        logger.error(f"Tool {name} failed: {e}")
        return _tool_error(f"Ошибка инструмента {name}: {str(e)[:150]}")
