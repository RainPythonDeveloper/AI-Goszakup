-- ============================================================
-- Materialized Views: Витрины данных для AI-агента
-- ============================================================

-- 1. Ценовая статистика по ENSTRU + регион + квартал
-- Основная витрина для Fair Price и детекции аномалий
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_price_statistics AS
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
  AND EXTRACT(YEAR FROM c.sign_date) IN (2024, 2025, 2026)
  AND c.status_id NOT IN (
      SELECT rs.id FROM ref_statuses rs
      WHERE rs.code IN ('330') AND rs.entity_type = 'contract'
  )
GROUP BY cs.enstru_code, cs.unit_id, cs.delivery_kato, rk.region,
         date_trunc('quarter', c.sign_date), EXTRACT(YEAR FROM c.sign_date)
WITH DATA;

-- 2. Тренды объёмов по ТРУ по годам (для выявления завышений)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_volume_trends AS
SELECT
    l.enstru_code,
    l.customer_bin,
    s.name_ru AS customer_name,
    s.name_kz AS customer_name_kz,
    EXTRACT(YEAR FROM a.publish_date)::SMALLINT AS year,
    COUNT(DISTINCT l.id) AS lot_count,
    SUM(l.count) AS total_quantity,
    SUM(l.amount) AS total_amount,
    AVG(l.price_per_unit) AS avg_price_per_unit
FROM lots l
JOIN announcements a ON l.announcement_id = a.id
LEFT JOIN subjects s ON l.customer_bin = s.bin
WHERE l.enstru_code IS NOT NULL
  AND a.publish_date IS NOT NULL
  AND l.status_id IN (
      SELECT rs.id FROM ref_statuses rs
      WHERE rs.code = '360' AND rs.entity_type = 'lot'
  )
GROUP BY l.enstru_code, l.customer_bin, s.name_ru, s.name_kz,
         EXTRACT(YEAR FROM a.publish_date)
WITH DATA;

-- 3. Статистика поставщиков
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_supplier_stats AS
SELECT
    c.supplier_bin,
    s.name_ru AS supplier_name,
    s.name_kz AS supplier_name_kz,
    COUNT(DISTINCT c.id) AS contract_count,
    SUM(c.contract_sum) AS total_sum,
    AVG(c.contract_sum) AS avg_contract_sum,
    COUNT(DISTINCT c.customer_bin) AS unique_customers,
    MIN(c.sign_date) AS first_contract,
    MAX(c.sign_date) AS last_contract
FROM contracts c
LEFT JOIN subjects s ON c.supplier_bin = s.bin
WHERE c.supplier_bin IS NOT NULL
GROUP BY c.supplier_bin, s.name_ru, s.name_kz
WITH DATA;

-- 4. Региональные ценовые коэффициенты (для Fair Price)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_regional_coefficients AS
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
  AND EXTRACT(YEAR FROM c.sign_date) IN (2024, 2025, 2026)
  AND c.status_id NOT IN (
      SELECT rs.id FROM ref_statuses rs
      WHERE rs.code IN ('330') AND rs.entity_type = 'contract'
  )
GROUP BY cs.enstru_code, cs.unit_id, rk.region
WITH DATA;

-- 5. Обзор данных (для healthcheck и отчётов)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_data_overview AS
SELECT
    'subjects' AS entity, COUNT(*) AS cnt FROM subjects
UNION ALL SELECT 'announcements', COUNT(*) FROM announcements
UNION ALL SELECT 'lots', COUNT(*) FROM lots
UNION ALL SELECT 'contracts', COUNT(*) FROM contracts
UNION ALL SELECT 'contract_subjects', COUNT(*) FROM contract_subjects
UNION ALL SELECT 'plans', COUNT(*) FROM plans
UNION ALL SELECT 'applications', COUNT(*) FROM applications
UNION ALL SELECT 'payments', COUNT(*) FROM payments
UNION ALL SELECT 'contract_acts', COUNT(*) FROM contract_acts
WITH DATA;
