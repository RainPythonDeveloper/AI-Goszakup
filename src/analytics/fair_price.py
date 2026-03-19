"""
Fair Price Engine — расчёт справедливой цены ТРУ.

Формула: FairPrice = MedianPrice × RegionalCoeff × InflationIndex × SeasonCoeff

Источники данных:
- contract_subjects: прямой расчёт медианы (PERCENTILE_CONT)
- mv_regional_coefficients: региональные коэффициенты
- inflation_index: кумулятивный ИПЦ (от midpoint выборки до target_date)

Исправления v2:
- Медиана вычисляется напрямую из contract_subjects (не weighted mean of medians)
- CPI корректируется от midpoint выборки, а не от базового 2024-01
- Добавлен data quality filter
- Добавлен доверительный интервал (IQR-based)
- Добавлены флаги fallback-значений
"""
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

import psycopg2

from src.config import db_config, DATA_YEARS, INVALID_CONTRACT_STATUS_CODES

logger = logging.getLogger(__name__)

# SQL фильтр: только данные за целевые годы (2024-2026)
_YEAR_FILTER = f"AND EXTRACT(YEAR FROM c.sign_date) IN ({', '.join(str(y) for y in DATA_YEARS)})"

# SQL фильтр: исключаем контракты с невалидным статусом (330 = «Не заключен»)
_INVALID_CODES_SQL = ", ".join(f"'{c}'" for c in INVALID_CONTRACT_STATUS_CODES)
_VALID_CONTRACT_FILTER = f"""
    AND c.status_id NOT IN (
        SELECT rs.id FROM ref_statuses rs
        WHERE rs.code IN ({_INVALID_CODES_SQL}) AND rs.entity_type = 'contract'
    )
"""


@dataclass
class FairPriceResult:
    enstru_code: str
    region: str | None
    fair_price: float
    median_price: float
    regional_coeff: float
    inflation_index: float
    seasonal_coeff: float
    confidence: str
    sample_size: int
    q1: float | None
    q3: float | None
    min_price: float | None
    max_price: float | None
    ci_lower: float | None = None
    ci_upper: float | None = None
    unit_id: int | None = None
    unit_name: str | None = None
    fallback_flags: list[str] = field(default_factory=list)
    conversion_info: dict | None = None
    median_records: list[dict] = field(default_factory=list)
    is_unit_converted: bool = False
    is_unit_mixed: bool = False


def get_conn():
    return psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )


def calculate_fair_price(
    enstru_code: str,
    region: str | None = None,
    target_date: date | None = None,
    unit_id: int | None = None,
    conn=None,
    exclude_customer_bin: str | None = None,
) -> FairPriceResult | None:
    """
    Рассчитывает справедливую цену для ТРУ.

    Args:
        enstru_code: код ENSTRU (ТРУ)
        region: регион доставки (из КАТО)
        target_date: дата для расчёта ИПЦ (по умолчанию — сегодня)
        conn: соединение с БД (опционально)

    Returns:
        FairPriceResult или None если нет данных
    """
    should_close = conn is None
    if conn is None:
        conn = get_conn()

    try:
        if target_date is None:
            target_date = date.today()

        fallback_flags: list[str] = []
        conversion_info: dict | None = None
        is_unit_converted = False
        is_unit_mixed = False
        median_records: list[dict] = []

        (
            median_price, sample_size, q1, q3, min_p, max_p,
            earliest_date, latest_date,
        ) = _get_price_stats(conn, enstru_code, region, unit_id=unit_id,
                            exclude_customer_bin=exclude_customer_bin)

        if unit_id is not None and (median_price is None or sample_size < 3):
            factors = _compute_unit_conversion_factors(conn, enstru_code, unit_id)

            if factors:
                (
                    median_price, sample_size, q1, q3, min_p, max_p,
                    earliest_date, latest_date,
                    converted_count, median_records,
                ) = _get_price_stats_normalized(
                    conn, enstru_code, region, unit_id, factors,
                    exclude_customer_bin=exclude_customer_bin,
                )
                if median_price is not None and sample_size >= 3:
                    is_unit_converted = True
                    conversion_info = factors
                    factor_desc = ", ".join(
                        f"{v['unit_name']}→целевая: ÷{v['factor']:.1f}"
                        for v in factors.values()
                    )
                    fallback_flags.append(
                        f"unit_conversion: {converted_count} из {sample_size} записей "
                        f"конвертированы в целевую единицу. Коэффициенты: {factor_desc}"
                    )

            if not is_unit_converted and (median_price is None or sample_size < 3):
                fallback_flags.append(
                    f"unit_mixed_UNRELIABLE: unit_id={unit_id} — {sample_size} записей. "
                    "Конвертация единиц невозможна (< 3 записей для целевой единицы "
                    "на национальном уровне). Медиана по ВСЕМ единицам — "
                    "ВЕРДИКТ НЕНАДЁЖЕН."
                )
                is_unit_mixed = True
                (
                    median_price, sample_size, q1, q3, min_p, max_p,
                    earliest_date, latest_date,
                ) = _get_price_stats(conn, enstru_code, region, unit_id=None,
                                    exclude_customer_bin=exclude_customer_bin)

        if not median_records and median_price is not None:
            used_unit = unit_id if not is_unit_mixed else None
            median_records = _get_median_records(
                conn, enstru_code, region, unit_id=used_unit,
                exclude_customer_bin=exclude_customer_bin,
            )

        if median_price is None or median_price <= 0:
            logger.warning(f"No price data for ENSTRU={enstru_code}, region={region}")
            return None

        resolved_unit_name = None
        if unit_id:
            with conn.cursor() as cur:
                cur.execute("SELECT name_ru FROM ref_units WHERE id = %s", (unit_id,))
                row = cur.fetchone()
                if row:
                    resolved_unit_name = row[0]

        regional_coeff = _get_regional_coefficient(conn, enstru_code, region, unit_id=unit_id)
        if not region:
            fallback_flags.append("regional_coeff=1.0: регион не указан, использованы национальные данные")
        elif regional_coeff == 1.0 and region:
            fallback_flags.append(f"regional_coeff=1.0: нет региональных данных для {region}")

        if earliest_date and latest_date:
            sample_midpoint = earliest_date + (latest_date - earliest_date) / 2
        else:
            sample_midpoint = target_date
            fallback_flags.append("inflation_index: не удалось определить период выборки")

        inflation_idx = _get_inflation_index(conn, sample_midpoint, target_date)

        seasonal_coeff = _get_seasonal_coefficient(conn, enstru_code, target_date)
        if seasonal_coeff == 1.0:
            fallback_flags.append(
                "seasonal_coeff=1.0: недостаточно данных "
                "(нужно ≥5 записей в квартале и ≥20 всего)"
            )

        # 5. Формула Fair Price = Median × RegCoeff × CPI × SeasonCoeff
        fair_price = median_price * regional_coeff * inflation_idx * seasonal_coeff

        # 6. Доверительный интервал (IQR-based)
        ci_lower = None
        ci_upper = None
        if q1 and q3:
            ci_lower = round(q1 * regional_coeff * inflation_idx * seasonal_coeff, 2)
            ci_upper = round(q3 * regional_coeff * inflation_idx * seasonal_coeff, 2)

        # 7. Уровень доверия
        if sample_size >= 30:
            confidence = 'high'
        elif sample_size >= 10:
            confidence = 'medium'
        else:
            confidence = 'low'

        return FairPriceResult(
            enstru_code=enstru_code,
            region=region,
            fair_price=round(fair_price, 2),
            median_price=round(median_price, 2),
            regional_coeff=round(regional_coeff, 4),
            inflation_index=round(inflation_idx, 4),
            seasonal_coeff=round(seasonal_coeff, 4),
            confidence=confidence,
            sample_size=sample_size,
            q1=round(q1, 2) if q1 else None,
            q3=round(q3, 2) if q3 else None,
            min_price=round(min_p, 2) if min_p else None,
            max_price=round(max_p, 2) if max_p else None,
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            unit_id=unit_id,
            unit_name=resolved_unit_name,
            fallback_flags=fallback_flags,
            conversion_info=conversion_info,
            median_records=median_records,
            is_unit_converted=is_unit_converted,
            is_unit_mixed=is_unit_mixed,
        )

    finally:
        if should_close:
            conn.close()


def _get_price_stats(conn, enstru_code: str, region: str | None,
                     unit_id: int | None = None,
                     exclude_customer_bin: str | None = None):
    """
    Получает ценовую статистику напрямую из contract_subjects.

    Вычисляет истинную медиану через PERCENTILE_CONT (не weighted mean of medians).
    Применяет data quality filter для исключения некорректных price_per_unit.
    Если unit_id указан — фильтрует по единице измерения.

    Returns:
        (median, sample_size, q1, q3, min_price, max_price, earliest_date, latest_date)
    """
    data_quality = """
        AND (
            cs.total_price IS NULL OR cs.total_price <= 0
            OR cs.quantity IS NULL OR cs.quantity <= 1
            OR cs.price_per_unit <= cs.total_price * 1.1
        )
    """

    params: list = [enstru_code]
    region_filter = ""
    if region:
        region_filter = "AND rk.region = %s"
        params.append(region)

    unit_filter = ""
    if unit_id is not None:
        unit_filter = "AND cs.unit_id = %s"
        params.append(unit_id)

    customer_filter = ""
    if exclude_customer_bin:
        customer_filter = "AND c.customer_bin != %s"
        params.append(exclude_customer_bin)

    query = f"""
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) AS median_price,
            COUNT(*) AS sample_size,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cs.price_per_unit) AS q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cs.price_per_unit) AS q3,
            MIN(cs.price_per_unit) AS min_price,
            MAX(cs.price_per_unit) AS max_price,
            MIN(c.sign_date) AS earliest_date,
            MAX(c.sign_date) AS latest_date
        FROM contract_subjects cs
        JOIN contracts c ON cs.contract_id = c.id
        LEFT JOIN ref_kato rk ON cs.delivery_kato = rk.code
        WHERE cs.enstru_code = %s
          AND cs.price_per_unit IS NOT NULL
          AND cs.price_per_unit > 0
          AND c.sign_date IS NOT NULL
          {_YEAR_FILTER}
          {_VALID_CONTRACT_FILTER}
          {data_quality}
          {region_filter}
          {unit_filter}
          {customer_filter}
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        if row and row[0]:
            return (
                float(row[0]),   # median
                int(row[1]),     # sample_size
                row[2],          # q1
                row[3],          # q3
                row[4],          # min_price
                row[5],          # max_price
                row[6],          # earliest_date
                row[7],          # latest_date
            )
        return None, 0, None, None, None, None, None, None


def _percentile_cont(sorted_values: list[float], p: float) -> float:
    """Python-эквивалент PostgreSQL PERCENTILE_CONT (линейная интерполяция)."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_values[0]
    pos = p * (n - 1)
    lo = int(pos)
    hi = lo + 1
    if hi >= n:
        return sorted_values[-1]
    frac = pos - lo
    return sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])


def _compute_unit_conversion_factors(
    conn, enstru_code: str, target_unit_id: int,
) -> dict[int, dict] | None:
    """
    Вычисляет коэффициенты конвертации из других единиц в целевую.

    Использует НАЦИОНАЛЬНЫЕ медианы (без фильтра по региону) для надёжности.
    Логика: если национальная медиана Упаковки = 95 000, а Штуки = 10 000,
    то 1 Упаковка ≈ 9.5 Штук (factor = 9.5).
    Для конвертации: цена_в_штуках = цена_упаковки / factor.

    Returns:
        {source_unit_id: {"factor": float, "unit_name": str, "sample_size": int}}
        factor = source_median / target_median
        None если целевая единица имеет < 3 национальных записей.
    """
    data_quality = """
        AND (
            cs.total_price IS NULL OR cs.total_price <= 0
            OR cs.quantity IS NULL OR cs.quantity <= 1
            OR cs.price_per_unit <= cs.total_price * 1.1
        )
    """

    query = f"""
        SELECT cs.unit_id,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) AS median_price,
               COUNT(*) AS cnt,
               MIN(ru.name_ru) AS unit_name
        FROM contract_subjects cs
        JOIN contracts c ON cs.contract_id = c.id
        LEFT JOIN ref_units ru ON cs.unit_id = ru.id
        WHERE cs.enstru_code = %s
          AND cs.price_per_unit IS NOT NULL
          AND cs.price_per_unit > 0
          AND cs.unit_id IS NOT NULL
          AND c.sign_date IS NOT NULL
          {_YEAR_FILTER}
          {_VALID_CONTRACT_FILTER}
          {data_quality}
        GROUP BY cs.unit_id
        HAVING COUNT(*) >= 3
    """

    with conn.cursor() as cur:
        cur.execute(query, (enstru_code,))
        rows = cur.fetchall()

    if not rows:
        return None

    target_median = None
    unit_data: dict[int, tuple[float, int, str]] = {}

    for unit_id, median, cnt, unit_name in rows:
        uid = int(unit_id)
        med = float(median)
        unit_data[uid] = (med, int(cnt), unit_name or f"unit_{uid}")
        if uid == target_unit_id:
            target_median = med

    if target_median is None or target_median <= 0:
        return None

    factors: dict[int, dict] = {}
    for uid, (median, cnt, uname) in unit_data.items():
        if uid != target_unit_id and median > 0:
            factor = median / target_median
            if 0.01 <= factor <= 10000:
                factors[uid] = {
                    "factor": round(factor, 4),
                    "unit_name": uname,
                    "sample_size": cnt,
                    "source_median": round(median, 2),
                    "target_median": round(target_median, 2),
                }

    return factors if factors else None


def _get_price_stats_normalized(
    conn, enstru_code: str, region: str | None,
    target_unit_id: int, conversion_factors: dict[int, dict],
    exclude_customer_bin: str | None = None,
) -> tuple:
    """
    Получает ценовую статистику с нормализацией единиц измерения.

    Загружает все записи, конвертирует цены в целевую единицу через
    conversion_factors, вычисляет медиану/Q1/Q3 в Python.

    Returns:
        (median, sample_size, q1, q3, min_p, max_p, earliest_date, latest_date,
         converted_count, records)
    """
    data_quality = """
        AND (
            cs.total_price IS NULL OR cs.total_price <= 0
            OR cs.quantity IS NULL OR cs.quantity <= 1
            OR cs.price_per_unit <= cs.total_price * 1.1
        )
    """

    params: list = [enstru_code]
    region_filter = ""
    if region:
        region_filter = "AND rk.region = %s"
        params.append(region)

    customer_filter = ""
    if exclude_customer_bin:
        customer_filter = "AND c.customer_bin != %s"
        params.append(exclude_customer_bin)

    query = f"""
        SELECT cs.price_per_unit, cs.unit_id, cs.quantity, cs.total_price,
               c.source_id, c.contract_number, c.sign_date,
               c.customer_bin, s.name_ru AS customer_name,
               ru.name_ru AS unit_name
        FROM contract_subjects cs
        JOIN contracts c ON cs.contract_id = c.id
        LEFT JOIN ref_kato rk ON cs.delivery_kato = rk.code
        LEFT JOIN subjects s ON c.customer_bin = s.bin
        LEFT JOIN ref_units ru ON cs.unit_id = ru.id
        WHERE cs.enstru_code = %s
          AND cs.price_per_unit IS NOT NULL
          AND cs.price_per_unit > 0
          AND c.sign_date IS NOT NULL
          {_YEAR_FILTER}
          {_VALID_CONTRACT_FILTER}
          {data_quality}
          {region_filter}
          {customer_filter}
        ORDER BY cs.price_per_unit
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        return None, 0, None, None, None, None, None, None, 0, []

    normalized_prices: list[float] = []
    converted_count = 0
    records: list[dict] = []

    for (price, unit_id, qty, total_price,
         source_id, contract_number, sign_date,
         customer_bin, customer_name, unit_name) in rows:
        price_f = float(price)
        uid = int(unit_id) if unit_id else None

        if uid == target_unit_id or uid is None:
            normalized_prices.append(price_f)
            records.append({
                "price_per_unit": round(price_f, 2),
                "original_price": round(price_f, 2),
                "unit_id": uid,
                "unit_name": unit_name,
                "converted": False,
                "sign_date": str(sign_date) if sign_date else None,
                "contract_id": source_id,
                "contract_number": contract_number,
                "customer_name": customer_name,
                "quantity": float(qty) if qty else None,
                "total_price": float(total_price) if total_price else None,
            })
        elif uid in conversion_factors:
            factor = conversion_factors[uid]["factor"]
            converted_price = price_f / factor
            normalized_prices.append(converted_price)
            converted_count += 1
            records.append({
                "price_per_unit": round(converted_price, 2),
                "original_price": round(price_f, 2),
                "unit_id": uid,
                "unit_name": unit_name,
                "converted": True,
                "conversion_factor": factor,
                "sign_date": str(sign_date) if sign_date else None,
                "contract_id": source_id,
                "contract_number": contract_number,
                "customer_name": customer_name,
                "quantity": float(qty) if qty else None,
                "total_price": float(total_price) if total_price else None,
            })

    if len(normalized_prices) < 3:
        return None, len(normalized_prices), None, None, None, None, None, None, converted_count, records
    sorted_prices = sorted(normalized_prices)
    median = _percentile_cont(sorted_prices, 0.5)
    q1 = _percentile_cont(sorted_prices, 0.25)
    q3 = _percentile_cont(sorted_prices, 0.75)
    min_p = sorted_prices[0]
    max_p = sorted_prices[-1]

    earliest = None
    latest = None
    for r in records:
        sd = r.get("sign_date")
        if sd:
            try:
                d = date.fromisoformat(sd)
                if earliest is None or d < earliest:
                    earliest = d
                if latest is None or d > latest:
                    latest = d
            except (ValueError, TypeError):
                pass

    records.sort(key=lambda r: r["price_per_unit"])

    return (
        round(median, 2), len(normalized_prices),
        round(q1, 2), round(q3, 2),
        round(min_p, 2), round(max_p, 2),
        earliest, latest,
        converted_count, records[:10],
    )


def _get_median_records(
    conn, enstru_code: str, region: str | None,
    unit_id: int | None = None, limit: int = 10,
    exclude_customer_bin: str | None = None,
) -> list[dict]:
    """
    Возвращает фактические записи, использованные для расчёта медианы.
    Те же фильтры, что и _get_price_stats — для прозрачности.
    """
    data_quality = """
        AND (
            cs.total_price IS NULL OR cs.total_price <= 0
            OR cs.quantity IS NULL OR cs.quantity <= 1
            OR cs.price_per_unit <= cs.total_price * 1.1
        )
    """

    params: list = [enstru_code]
    region_filter = ""
    if region:
        region_filter = "AND rk.region = %s"
        params.append(region)

    unit_filter = ""
    if unit_id is not None:
        unit_filter = "AND cs.unit_id = %s"
        params.append(unit_id)

    customer_filter = ""
    if exclude_customer_bin:
        customer_filter = "AND c.customer_bin != %s"
        params.append(exclude_customer_bin)

    query = f"""
        SELECT cs.price_per_unit, cs.quantity, cs.total_price,
               c.source_id AS contract_id, c.contract_number, c.sign_date,
               s.name_ru AS customer_name, ru.name_ru AS unit_name
        FROM contract_subjects cs
        JOIN contracts c ON cs.contract_id = c.id
        LEFT JOIN ref_kato rk ON cs.delivery_kato = rk.code
        LEFT JOIN subjects s ON c.customer_bin = s.bin
        LEFT JOIN ref_units ru ON cs.unit_id = ru.id
        WHERE cs.enstru_code = %s
          AND cs.price_per_unit IS NOT NULL
          AND cs.price_per_unit > 0
          AND c.sign_date IS NOT NULL
          {_YEAR_FILTER}
          {_VALID_CONTRACT_FILTER}
          {data_quality}
          {region_filter}
          {unit_filter}
          {customer_filter}
        ORDER BY cs.price_per_unit
        LIMIT %s
    """
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [
        {
            "price_per_unit": round(float(price), 2),
            "quantity": float(qty) if qty else None,
            "total_price": float(total) if total else None,
            "contract_id": contract_id,
            "contract_number": contract_number,
            "sign_date": str(sign_date) if sign_date else None,
            "customer_name": customer_name,
            "unit_name": unit_name,
            "converted": False,
        }
        for price, qty, total, contract_id, contract_number, sign_date, customer_name, unit_name
        in rows
    ]


def _get_regional_coefficient(conn, enstru_code: str, region: str | None,
                              unit_id: int | None = None) -> float:
    """
    Вычисляет региональный коэффициент.
    Коэффициент = regional_median / national_median.
    Если данных нет — возвращает 1.0.

    Агрегирует по unit_id если указан, иначе по всем единицам (weighted avg).
    """
    if not region:
        return 1.0

    unit_cond = ""
    reg_params: list = [enstru_code, region]
    nat_params: list = [enstru_code]
    if unit_id is not None:
        unit_cond = " AND unit_id = %s"
        reg_params.append(unit_id)
        nat_params.append(unit_id)

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT SUM(regional_median * sample_size) / NULLIF(SUM(sample_size), 0),
                   SUM(sample_size)
            FROM mv_regional_coefficients
            WHERE enstru_code = %s AND region = %s{unit_cond}
        """, reg_params)
        regional = cur.fetchone()

        cur.execute(f"""
            SELECT SUM(regional_median * sample_size) / NULLIF(SUM(sample_size), 0)
            FROM mv_regional_coefficients
            WHERE enstru_code = %s{unit_cond}
        """, nat_params)
        national = cur.fetchone()

    if regional and national and national[0] and national[0] > 0 and regional[0]:
        return float(regional[0]) / float(national[0])

    return 1.0


def _get_cpi_for_date(conn, target_date: date) -> float | None:
    """Получает CPI для конкретной даты (точное совпадение или ближайший месяц)."""
    year = target_date.year
    month = target_date.month

    with conn.cursor() as cur:
        cur.execute(
            "SELECT cpi FROM inflation_index WHERE year = %s AND month = %s",
            (year, month),
        )
        row = cur.fetchone()
        if row:
            return float(row[0])

        cur.execute("""
            SELECT cpi FROM inflation_index
            ORDER BY ABS((year - %s) * 12 + (month - %s))
            LIMIT 1
        """, (year, month))
        nearest = cur.fetchone()
        if nearest:
            return float(nearest[0])

    return None


def _get_inflation_index(
    conn,
    sample_midpoint: date,
    target_date: date,
) -> float:
    """
    Вычисляет инфляционный индекс от midpoint выборки до целевой даты.

    Формула: CPI(target) / CPI(sample_midpoint)

    Это корректирует «двойной учёт»: медиана построена на ценах, уже
    включающих инфляцию до момента подписания контракта. Поэтому CPI
    считается ТОЛЬКО от midpoint выборки до сегодня, а не от 2024-01.

    Returns:
        float >= 1.0 (обычно), или 1.0 если данных нет.
    """
    cpi_target = _get_cpi_for_date(conn, target_date)
    cpi_sample = _get_cpi_for_date(conn, sample_midpoint)

    if cpi_target and cpi_sample and cpi_sample > 0:
        return float(cpi_target) / float(cpi_sample)

    return 1.0


def _get_seasonal_coefficient(conn, enstru_code: str, target_date: date) -> float:
    """
    Вычисляет сезонный коэффициент на основе исторических данных.

    Сравнивает медиану цен за целевой квартал с общей годовой медианой
    для данного ENSTRU кода. Например, стройматериалы летом дороже.

    Returns:
        Коэффициент > 1.0 если квартал дороже среднего, < 1.0 если дешевле.
        1.0 если данных недостаточно.
    """
    quarter = (target_date.month - 1) // 3 + 1

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) AS quarter_median,
                COUNT(*) AS quarter_count
            FROM contract_subjects cs
            JOIN contracts c ON cs.contract_id = c.id
            WHERE cs.enstru_code = %s
              AND cs.price_per_unit > 0
              AND EXTRACT(QUARTER FROM c.sign_date) = %s
              {_YEAR_FILTER}
              {_VALID_CONTRACT_FILTER}
        """, (enstru_code, quarter))
        q_row = cur.fetchone()

        cur.execute(f"""
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) AS annual_median,
                COUNT(*) AS annual_count
            FROM contract_subjects cs
            JOIN contracts c ON cs.contract_id = c.id
            WHERE cs.enstru_code = %s
              AND cs.price_per_unit > 0
              {_YEAR_FILTER}
              {_VALID_CONTRACT_FILTER}
        """, (enstru_code,))
        a_row = cur.fetchone()

    if (q_row and a_row
            and q_row[0] and a_row[0]
            and float(a_row[0]) > 0
            and int(q_row[1] or 0) >= 5
            and int(a_row[1] or 0) >= 20):
        coeff = float(q_row[0]) / float(a_row[0])
        return max(0.7, min(1.5, coeff))

    return 1.0


def batch_fair_prices(enstru_codes: list[str], region: str | None = None) -> list[FairPriceResult]:
    """Пакетный расчёт Fair Price для списка ENSTRU кодов."""
    conn = get_conn()
    results = []
    try:
        for code in enstru_codes:
            result = calculate_fair_price(code, region=region, conn=conn)
            if result:
                results.append(result)
    finally:
        conn.close()
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT enstru_code, SUM(sample_size) as total
                FROM mv_price_statistics
                WHERE enstru_code IS NOT NULL
                GROUP BY enstru_code
                ORDER BY total DESC
                LIMIT 10
            """)
            rows = cur.fetchall()

        if rows:
            print("Top ENSTRU codes with price data:")
            for code, total in rows:
                result = calculate_fair_price(code, conn=conn)
                if result:
                    print(f"  {code}: FairPrice={result.fair_price:,.2f} "
                          f"(median={result.median_price:,.2f}, "
                          f"samples={result.sample_size}, "
                          f"confidence={result.confidence})")
        else:
            print("No price data available yet. Run ETL pipeline first.")
    finally:
        conn.close()
