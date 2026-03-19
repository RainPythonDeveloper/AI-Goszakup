-- ============================================================
-- AI-Агент Госзакупок РК: Схема БД
-- PostgreSQL 16
-- ============================================================

-- Расширения
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- нечёткий текстовый поиск

-- ============================================================
-- СПРАВОЧНИКИ (ref_*)
-- ============================================================

CREATE TABLE ref_statuses (
    id          SERIAL PRIMARY KEY,
    code        VARCHAR(50) NOT NULL,
    name_ru     VARCHAR(500),
    name_kz     VARCHAR(500),
    entity_type VARCHAR(50) NOT NULL,  -- 'lot', 'contract', 'announcement', 'plan'
    UNIQUE (code, entity_type)
);

CREATE TABLE ref_methods (
    id      SERIAL PRIMARY KEY,
    code    VARCHAR(50) UNIQUE NOT NULL,
    name_ru VARCHAR(500),
    name_kz VARCHAR(500)
);

CREATE TABLE ref_units (
    id      SERIAL PRIMARY KEY,
    code    VARCHAR(50) UNIQUE NOT NULL,
    name_ru VARCHAR(500),
    name_kz VARCHAR(500)
);

CREATE TABLE ref_kato (
    id      SERIAL PRIMARY KEY,
    code    VARCHAR(20) UNIQUE NOT NULL,
    name_ru VARCHAR(500),
    name_kz VARCHAR(500),
    region  VARCHAR(200),  -- уровень области (для региональных коэффициентов)
    level   SMALLINT       -- 1=область, 2=район, 3=город/село
);

CREATE TABLE ref_enstru (
    id          SERIAL PRIMARY KEY,
    code        VARCHAR(50) UNIQUE NOT NULL,
    name_ru     VARCHAR(1000),
    name_kz     VARCHAR(1000),
    parent_code VARCHAR(50),
    level       SMALLINT
);

CREATE TABLE ref_currencies (
    id      SERIAL PRIMARY KEY,
    code    VARCHAR(10) UNIQUE NOT NULL,
    name_ru VARCHAR(100)
);

-- ============================================================
-- ОСНОВНЫЕ ТАБЛИЦЫ
-- ============================================================

-- Участники (заказчики / поставщики / организаторы)
CREATE TABLE subjects (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,        -- ID из API
    bin             VARCHAR(12) NOT NULL,
    iin             VARCHAR(12),
    name_ru         VARCHAR(1000),
    name_kz         VARCHAR(1000),
    is_customer     BOOLEAN DEFAULT FALSE,
    is_organizer    BOOLEAN DEFAULT FALSE,
    is_supplier     BOOLEAN DEFAULT FALSE,
    kato_code       VARCHAR(20),
    address         TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_subjects_bin ON subjects(bin);

-- Годовые планы
CREATE TABLE plans (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,
    bin             VARCHAR(12) NOT NULL,
    enstru_code     VARCHAR(50),
    plan_sum        NUMERIC(20, 2),
    plan_count      NUMERIC(20, 4),
    method_id       INTEGER REFERENCES ref_methods(id),
    year            SMALLINT,
    month           SMALLINT,
    source_funding  VARCHAR(500),
    delivery_kato   VARCHAR(20),
    status          VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Объявления о закупках
CREATE TABLE announcements (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,
    number_anno     VARCHAR(100),
    name_ru         TEXT,
    total_sum       NUMERIC(20, 2),
    method_id       INTEGER REFERENCES ref_methods(id),
    status_id       INTEGER REFERENCES ref_statuses(id),
    organizer_bin   VARCHAR(12),
    customer_bin    VARCHAR(12),
    start_date      TIMESTAMP,
    end_date        TIMESTAMP,
    publish_date    TIMESTAMP,
    is_construction BOOLEAN DEFAULT FALSE,
    lot_count       INTEGER,
    source_system   SMALLINT,  -- 1=Mitwork, 2=Samruk, 3=Goszakup
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Лоты
CREATE TABLE lots (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,
    announcement_id BIGINT REFERENCES announcements(id),
    lot_number      VARCHAR(50),
    name_ru         TEXT,
    name_ru_clean   TEXT,  -- очищенное название (после ETL)
    description     TEXT,
    amount          NUMERIC(20, 2),       -- общая сумма лота
    count           NUMERIC(20, 4),       -- количество
    unit_id         INTEGER REFERENCES ref_units(id),
    price_per_unit  NUMERIC(20, 4),       -- расчётное: amount / count
    enstru_code     VARCHAR(50),
    customer_bin    VARCHAR(12),
    delivery_kato   VARCHAR(20),
    status_id       INTEGER REFERENCES ref_statuses(id),
    plan_id         BIGINT REFERENCES plans(id),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Договоры
CREATE TABLE contracts (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,
    contract_number VARCHAR(100),
    announcement_id BIGINT REFERENCES announcements(id),
    supplier_bin    VARCHAR(12),
    customer_bin    VARCHAR(12),
    contract_sum    NUMERIC(20, 2),
    sign_date       DATE,
    ec_end_date     DATE,
    status_id       INTEGER REFERENCES ref_statuses(id),
    contract_type   VARCHAR(200),
    lot_id          BIGINT REFERENCES lots(id),
    source_system   SMALLINT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Предметы договора (конкретные ТРУ)
CREATE TABLE contract_subjects (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,
    contract_id     BIGINT REFERENCES contracts(id),
    enstru_code     VARCHAR(50),
    name_ru         TEXT,
    quantity        NUMERIC(20, 4),
    unit_id         INTEGER REFERENCES ref_units(id),
    price_per_unit  NUMERIC(20, 4),
    total_price     NUMERIC(20, 2),
    delivery_kato   VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Заявки поставщиков
CREATE TABLE applications (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,
    announcement_id BIGINT REFERENCES announcements(id),
    supplier_bin    VARCHAR(12),
    lot_id          BIGINT REFERENCES lots(id),
    price_offer     NUMERIC(20, 2),
    discount        NUMERIC(10, 4),
    submission_date TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Казначейские платежи
CREATE TABLE payments (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,
    contract_id     BIGINT REFERENCES contracts(id),
    amount          NUMERIC(20, 2),
    payment_date    DATE,
    status          VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Электронные акты
CREATE TABLE contract_acts (
    id              BIGSERIAL PRIMARY KEY,
    source_id       BIGINT UNIQUE,
    contract_id     BIGINT REFERENCES contracts(id),
    act_number      VARCHAR(100),
    status          VARCHAR(100),
    approval_date   DATE,
    amount          NUMERIC(20, 2),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- СЛУЖЕБНЫЕ ТАБЛИЦЫ
-- ============================================================

-- Сырые данные (аудит / re-processing)
CREATE TABLE raw_data (
    id              BIGSERIAL PRIMARY KEY,
    source_endpoint VARCHAR(200) NOT NULL,
    source_id       BIGINT,
    raw_json        JSONB NOT NULL,
    loaded_at       TIMESTAMP DEFAULT NOW(),
    processed       BOOLEAN DEFAULT FALSE
);

-- Лог качества данных
CREATE TABLE data_quality_log (
    id              BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(100) NOT NULL,
    record_id       BIGINT,
    source_id       BIGINT,
    issue           TEXT NOT NULL,
    severity        VARCHAR(20) DEFAULT 'warning',  -- 'info', 'warning', 'error'
    detected_at     TIMESTAMP DEFAULT NOW()
);

-- Журнал синхронизации (отслеживание инкрементальных обновлений)
CREATE TABLE sync_journal (
    id              BIGSERIAL PRIMARY KEY,
    entity_type     VARCHAR(50) NOT NULL,   -- 'trd-buy', 'contract', 'lot', ...
    entity_id       BIGINT NOT NULL,
    action          CHAR(1) NOT NULL,       -- 'U' = update, 'D' = delete
    event_date      TIMESTAMP NOT NULL,
    api_url         TEXT,
    processed       BOOLEAN DEFAULT FALSE,
    processed_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Метаданные загрузки (прогресс Full Load)
CREATE TABLE load_metadata (
    id              BIGSERIAL PRIMARY KEY,
    entity_type     VARCHAR(50) NOT NULL,
    last_source_id  BIGINT,               -- последний загруженный ID (для search_after)
    total_loaded    INTEGER DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'pending',  -- 'pending', 'running', 'completed', 'failed'
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    error_message   TEXT
);

-- ИПЦ (индексы потребительских цен) для Fair Price
CREATE TABLE inflation_index (
    id              SERIAL PRIMARY KEY,
    year            SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    cpi             NUMERIC(10, 4) NOT NULL,  -- кумулятивный ИПЦ (база = 2024-01 = 100.0)
    UNIQUE (year, month)
);

-- Предзаполнение ИПЦ (приблизительные значения, уточнить из stat.gov.kz)
INSERT INTO inflation_index (year, month, cpi) VALUES
    (2024, 1,  100.00), (2024, 2,  100.80), (2024, 3,  101.20),
    (2024, 4,  101.60), (2024, 5,  102.00), (2024, 6,  102.50),
    (2024, 7,  103.00), (2024, 8,  103.40), (2024, 9,  103.80),
    (2024, 10, 104.20), (2024, 11, 104.70), (2024, 12, 105.20),
    (2025, 1,  105.80), (2025, 2,  106.30), (2025, 3,  106.80),
    (2025, 4,  107.20), (2025, 5,  107.70), (2025, 6,  108.20),
    (2025, 7,  108.70), (2025, 8,  109.10), (2025, 9,  109.60),
    (2025, 10, 110.10), (2025, 11, 110.60), (2025, 12, 111.10),
    (2026, 1,  111.60), (2026, 2,  112.10), (2026, 3,  112.60);
