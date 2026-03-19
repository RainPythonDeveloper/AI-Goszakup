# Архитектурное описание системы

## AI-агент анализа государственных закупок Республики Казахстан

---

## 1. Обзор системы

**Назначение**: Аналитический AI-агент, который на основе данных API OWS v3 портала goszakup.gov.kz формирует проверяемые ответы, выявляет аномалии и оценивает эффективность закупочных процессов.

**Охват данных**:
- 27 целевых организаций (заказчиков)
- Период: 2024 — 2026 годы
- Источник: https://ows.goszakup.gov.kz (GraphQL + REST)

**Стек технологий**:

| Компонент | Технология |
|-----------|-----------|
| Язык | Python 3.11 |
| База данных | PostgreSQL 16 |
| API-сервер | FastAPI + Uvicorn + WebSocket |
| HTTP-клиент | httpx (async) |
| ML | scikit-learn (Isolation Forest) |
| LLM | OpenAI-совместимый API (nitec-ai.kz) |
| Оркестрация | Apache Airflow 2.10 |
| Контейнеризация | Docker, Docker Compose |
| Деплой | Kubernetes |

---

## 2. Архитектурная схема

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ВНЕШНИЕ СИСТЕМЫ                              │
│                                                                     │
│  ┌──────────────────────┐          ┌──────────────────────────┐     │
│  │  goszakup.gov.kz     │          │  nitec-ai.kz             │     │
│  │  OWS v3 API          │          │  LLM API                 │     │
│  │  (GraphQL + REST)    │          │  (openai/gpt-oss-120b)   │     │
│  └──────────┬───────────┘          └─────────────┬────────────┘     │
└─────────────┼────────────────────────────────────┼──────────────────┘
              │                                    │
              ▼                                    │
┌─────────────────────────────────┐                │
│  СЛОЙ 1: DATA INGESTION         │                │
│                                 │                │
│  api_client.py                  │                │
│  ├─ GraphQL (7 сущностей)       │                │
│  ├─ REST (8 справочников)       │                │
│  ├─ Rate limiter (5 req/s)      │                │
│  └─ Retry + exp. backoff        │                │
│                                 │                │
│  data_loader.py                 │                │
│  ├─ UPSERT (ON CONFLICT)        │                │
│  └─ FK resolution               │                │
│                                 │                │
│  seed_refs.py                   │                │
│  └─ Справочники (16 764 КАТО,   │                │
│     621 единица, 148 статусов)  │                │
└────────────────┬────────────────┘                │
                 │                                 │
                 ▼                                 │
┌─────────────────────────────────┐                │
│  СЛОЙ 2: POSTGRESQL 16          │                │
│                                 │                │
│  20 таблиц:                     │                │
│  ├─ 9 основных (subjects,       │                │
│  │  announcements, lots,        │                │
│  │  contracts, contract_subjects,│                │
│  │  plans, applications,        │                │
│  │  payments, contract_acts)    │                │
│  ├─ 6 справочных (ref_*)        │                │
│  └─ 5 служебных (raw_data,      │                │
│     data_quality_log,           │                │
│     sync_journal, load_metadata,│                │
│     inflation_index)            │                │
│                                 │                │
│  5 materialized views:          │                │
│  ├─ mv_price_statistics         │                │
│  ├─ mv_volume_trends            │                │
│  ├─ mv_supplier_stats           │                │
│  ├─ mv_regional_coefficients    │                │
│  └─ mv_data_overview            │                │
│                                 │                │
│  20+ индексов (B-tree + GIN     │                │
│  trigram для нечёткого поиска)   │                │
└──────┬─────────────┬────────────┘                │
       │             │                             │
       ▼             ▼                             │
┌──────────┐  ┌──────────────────────┐             │
│ СЛОЙ 3:  │  │  СЛОЙ 4:             │             │
│ ETL      │  │  АНАЛИТИКА           │             │
│          │  │                      │             │
│ cleaner  │  │ fair_price.py        │             │
│ ├─ Очист-│  │ ├─ FairPrice =       │             │
│ │ ка тек-│  │ │  Median ×          │             │
│ │ ста    │  │ │  RegCoeff ×        │             │
│ ├─ Норма-│  │ │  CPI ×             │             │
│ │ лизация│  │ │  SeasonCoeff       │             │
│ └─ Каче- │  │ ├─ CI range (IQR)    │             │
│   ство   │  │ └─ Unit conversion   │             │
│          │  │                      │             │
│ enricher │  │ anomaly_detector.py  │             │
│ ├─ FK    │  │ ├─ IQR (статистика)  │             │
│ │ linking│  │ ├─ Isolation Forest   │             │
│ ├─ Обога-│  │ │  (ML)              │             │
│ │ щение  │  │ ├─ Consensus         │             │
│ └─ MV    │  │ ├─ Volume anomalies  │             │
│   refresh│  │ └─ Supplier concen-  │             │
│          │  │    tration           │             │
└──────────┘  └──────────┬───────────┘             │
                         │                         │
                         ▼                         │
              ┌──────────────────────┐             │
              │  СЛОЙ 5: AI-АГЕНТ    │◄────────────┘
              │                      │   LLM API calls
              │  react_agent.py      │
              │  ├─ ReAct паттерн    │
              │  │  (Reason → Act →  │
              │  │   Observe)        │
              │  ├─ 7 инструментов   │
              │  ├─ KZ/RU детекция   │
              │  ├─ 5 типов вопросов │
              │  └─ AgentTrace       │
              │                      │
              │  prompts.py          │
              │  ├─ XML-промпт       │
              │  ├─ 6-блочный формат │
              │  ├─ Self-check (17)  │
              │  └─ Few-shot (RU/KK) │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  СЛОЙ 6: API +       │
              │  ИНТЕРФЕЙС           │
              │                      │
              │  FastAPI (port 8000) │
              │  ├─ WebSocket        │
              │  │  /ws/chat/{id}    │
              │  ├─ REST endpoints   │
              │  │  /api/health      │
              │  │  /api/overview    │
              │  │  /api/fair-price  │
              │  │  /api/chat        │
              │  └─ Rate limit       │
              │     (20 msg/min)     │
              │                      │
              │  Frontend            │
              │  ├─ index.html       │
              │  ├─ WebSocket client │
              │  ├─ Markdown render  │
              │  └─ RU ↔ KK toggle   │
              └──────────────────────┘
```

---

## 3. Слой 1: Источники данных и Data Ingestion

### 3.1 API OWS v3

Система использует два протокола для получения данных с портала goszakup.gov.kz:

**GraphQL** (`https://ows.goszakup.gov.kz/v3/graphql`) — основные сущности:

| Сущность | GraphQL-тип | Фильтр | Описание |
|----------|------------|--------|----------|
| Объявления | TrdBuy | `orgBin` | Закупочные процедуры |
| Лоты | Lots | `customerBin` | Позиции в закупках |
| Договоры | Contract | `customerBin` | Заключённые контракты |
| Предметы договоров | ContractUnits | вложенный | Товары/услуги в контрактах |
| Планы | Plans | `subjectBiin` | Годовые планы закупок |
| Организации | Subject | `bin` | Заказчики и поставщики |
| Заявки | TrdApp | `customerBin` | Заявки поставщиков |

**REST** (`https://ows.goszakup.gov.kz/v3/refs/...`) — справочники:

| Справочник | Записей | Назначение |
|-----------|---------|-----------|
| ref_trade_methods | 35 | Способы закупки |
| ref_buy_status | 148 | Статусы (лоты, контракты, объявления) |
| ref_units | 621 | Единицы измерения |
| ref_kato | 16 764 | Классификатор территорий (КАТО) |
| ref_currency | 50 | Валюты |
| ref_finsource | — | Источники финансирования |
| ref_subject_type | — | Типы организаций |
| ref_contract_type | — | Типы контрактов |

### 3.2 Клиент API (`src/ingestion/api_client.py`)

- **Аутентификация**: Bearer token (переменная окружения `GOSZAKUP_API_TOKEN`)
- **Rate Limiter**: Token bucket, 5 запросов/сек (настраивается)
- **Retry**: Exponential backoff, максимум 3 попытки (429 → ожидание, 5xx → повтор)
- **Таймаут**: 60 сек (connect: 10 сек)
- **Пагинация**: cursor-based (GraphQL `after`/`limit`), REST `next_page`
- **Защита от зацикливания**: отслеживание `prev_url` для REST-пагинации

### 3.3 Загрузка данных (`src/ingestion/data_loader.py`)

Порядок загрузки (с учётом FK-зависимостей):

```
seed_refs (справочники)
    → subjects (организации)
        → announcements (объявления)
            → lots (лоты)
                → contracts (договоры)
                    → contract_subjects (предметы)
                        → plans (планы)
                            → applications (заявки)
```

**Стратегия записи**: UPSERT (`ON CONFLICT (source_id) DO UPDATE SET ...`) — обновляет существующие, вставляет новые.

**Обработка особенностей API**:
- Булевы поля приходят как `0/1` → приведение через `bool()`
- КАТО лотов: основной источник `plnPointKatoList`, fallback — `Plans.PlansKato.refKatoCode`
- Единицы измерения: извлечение из `Plans.refUnitsCode` с кэшированием `{code: id}`

---

## 4. Слой 2: Хранение данных (PostgreSQL 16)

### 4.1 Структура БД

**20 таблиц** в трёх группах:

```
СПРАВОЧНИКИ (6)                ОСНОВНЫЕ (9)               СЛУЖЕБНЫЕ (5)
├─ ref_statuses               ├─ subjects                ├─ raw_data
├─ ref_methods                ├─ announcements            ├─ data_quality_log
├─ ref_units                  ├─ lots                    ├─ sync_journal
├─ ref_kato                   ├─ contracts               ├─ load_metadata
├─ ref_enstru                 ├─ contract_subjects       └─ inflation_index
└─ ref_currencies             ├─ plans
                              ├─ applications
                              ├─ payments
                              └─ contract_acts
```

**Ключевые связи** (все таблицы имеют `id` BIGSERIAL + `source_id` BIGINT UNIQUE из API):

```
announcements ←──── lots
     │                │
     └── contracts ───┘
           │
           ├── contract_subjects
           ├── payments
           └── contract_acts

subjects (bin) ←── contracts.supplier_bin / customer_bin
ref_kato (code) ←── lots.delivery_kato / contract_subjects.delivery_kato
ref_units (id) ←── lots.unit_id / contract_subjects.unit_id
ref_enstru (code) ←── lots.enstru_code / contract_subjects.enstru_code
```

### 4.2 Materialized Views (витрины данных)

5 предвычисленных витрин для аналитики:

| Витрина | Группировка | Метрики | Назначение |
|---------|------------|---------|-----------|
| `mv_price_statistics` | enstru, unit, kato, регион, квартал, год | COUNT, медиана, Q1, Q3, средневзвешенная, min/max, stddev | Fair Price, аномалии |
| `mv_volume_trends` | enstru, customer_bin, год | кол-во лотов, объём, сумма, средняя цена | Аномалии объёмов |
| `mv_supplier_stats` | supplier_bin | кол-во контрактов, сумма, уникальные заказчики | Концентрация поставщиков |
| `mv_regional_coefficients` | enstru, unit, регион | региональная медиана, размер выборки | Региональный коэффициент |
| `mv_data_overview` | — | кол-во записей по каждой таблице | Health check |

### 4.3 Индексы

**20+ индексов**, включая:
- **B-tree**: customer_bin, supplier_bin, enstru_code, delivery_kato, sign_date, status_id
- **GIN trigram**: `idx_lots_name_trgm` на `lots.name_ru_clean` — нечёткий поиск товаров по названию
- **Partial**: `idx_sync_journal_processed` (`WHERE NOT processed`) — быстрый поиск необработанных записей

Расширение `pg_trgm` установлено для поддержки нечёткого текстового поиска.

---

## 5. Слой 3: ETL Pipeline

### 5.1 Очистка (`src/etl/cleaner.py`)

- **Нормализация текста**: удаление спецсимволов, двойных пробелов, лишних кавычек → поле `name_ru_clean`
- **Вычисление цены за единицу**: `price_per_unit = amount / count` для лотов без этого поля
- **Контроль качества**: логирование проблем (нулевые суммы, отсутствующие ENSTRU-коды) в `data_quality_log`

### 5.2 Обогащение (`src/etl/enricher.py`)

- **Связывание лотов с объявлениями**: восстановление FK `lots.announcement_id`
- **Расчёт итогов**: `contract_subjects.total_price = price_per_unit × quantity`
- **Дополнение организаций**: добавление новых поставщиков из контрактов
- **Обновление витрин**: `REFRESH MATERIALIZED VIEW` для всех 5 витрин

### 5.3 Оркестрация (`src/etl/pipeline.py`)

Последовательное выполнение: очистка → обогащение → обновление витрин → валидация.

---

## 6. Слой 4: Аналитический слой

### 6.1 Справедливая цена (`src/analytics/fair_price.py`)

**Формула**:

```
FairPrice = Median × RegionalCoeff × InflationIndex × SeasonalCoeff
```

| Компонент | Источник | Метод |
|-----------|---------|-------|
| **Median** | contract_subjects | PERCENTILE_CONT(0.5) по enstru_code (глобально) |
| **RegionalCoeff** | mv_regional_coefficients | regional_median / global_median по КАТО-региону |
| **InflationIndex** | inflation_index | CPI на целевую дату / CPI на медианную дату |
| **SeasonalCoeff** | contract_subjects | квартальная_медиана / годовая_медиана, диапазон [0.7, 1.5] |

**Дополнительно**:
- Доверительный интервал (CI) через IQR: `[Q1 - 1.5×IQR, Q3 + 1.5×IQR]`
- Уровни уверенности: high (≥30 записей), medium (10-29), low (<10)
- Конвертация единиц измерения (национальные медианы как коэффициенты)
- Флаги fallback: `is_unit_converted`, `is_unit_mixed`, список применённых упрощений

### 6.2 Детекция аномалий (`src/analytics/anomaly_detector.py`)

**Метод 1: IQR (статистический)**
- Пороги: 1.5 × IQR (умеренные) или 3.0 × IQR (экстремальные)
- Группировка: по enstru_code + unit_id (глобально, не по региону/кварталу)
- Severity: critical (>100%), high (>50%), medium (>30%)

**Метод 2: Isolation Forest (ML)**
- Алгоритм: `sklearn.ensemble.IsolationForest`
- Признаки: log(price), deviation, z-score, log(quantity), log(total)
- Обнаруживает многомерные аномалии (низкая цена + высокий объём)

**Метод 3: Consensus (IQR ∩ IF)**
- Запись считается аномалией, только если обнаружена обоими методами
- Максимальная надёжность (высокая специфичность)

**Дополнительные детекторы**:
- **Аномалии объёмов**: текущий_год_qty > 2× средний_qty_все_годы (по mv_volume_trends)
- **Концентрация поставщиков**: топ-поставщик > 80% суммы заказчика

**Фильтры качества данных**:
- Исключение статуса 330 ("Не заключен")
- Проверка: `price_per_unit ≤ total_price × 1.1` при `quantity > 1`
- Ограничение отклонения: ±99 999% (для читаемости)

---

## 7. Слой 5: AI-агент

### 7.1 Паттерн ReAct (`src/agent/react_agent.py`)

Агент работает по паттерну **ReAct** (Reason → Act → Observe → Repeat):

```
Вопрос пользователя
    │
    ▼
┌─ Классификация вопроса (5 типов) ──────────────┐
│  поиск | сравнение | аналитика | аномалии |      │
│  справедливость цены                            │
└─────────────────────┬───────────────────────────┘
                      │
    ┌─────────────────▼──────────────────┐
    │  ИТЕРАЦИЯ ReAct (макс. 20)         │
    │                                    │
    │  1. Reason: LLM анализирует задачу │
    │  2. Act: LLM выбирает инструмент   │
    │  3. Observe: результат инструмента │
    │  4. Повтор, пока не готов ответ    │
    └─────────────────┬──────────────────┘
                      │
                      ▼
    Финальный ответ (6-блочный формат)
```

**Конфигурация**:
- Макс. итераций: 20
- Макс. tool calls per message: 10
- Таймаут: 300 сек (5 мин) на вопрос
- LLM retry: 3 попытки с exponential backoff

**Детекция языка**: автоматическое определение казахского (по символам ә, ғ, қ, ң, ө, ұ, ү, і и ключевым словам) или русского.

**Tracing**: каждый вопрос записывается в `AgentTrace` (вопрос → итерации → tool calls → финальный ответ → метрики).

### 7.2 Инструменты агента (`src/agent/tools.py`)

**7 инструментов**, через которые LLM работает с данными (LLM НЕ считает сама):

| # | Инструмент | Назначение |
|---|-----------|-----------|
| 1 | `execute_sql` | SELECT-запросы к БД (макс. 100 строк, SQL injection защита) |
| 2 | `get_fair_price` | Расчёт справедливой цены по ENSTRU-коду |
| 3 | `evaluate_lot_price` | Комплексная оценка цены лота (аналоги, вердикт, ссылки) |
| 4 | `detect_anomalies` | Детекция ценовых аномалий (IQR / IF / Consensus) |
| 5 | `check_supplier_concentration` | Анализ концентрации поставщиков |
| 6 | `detect_volume_anomalies` | Обнаружение аномалий объёмов закупок |
| 7 | `get_data_overview` | Обзор данных (количество записей по таблицам) |

**Защита от SQL injection**: блокировка INSERT/UPDATE/DELETE/DROP/ALTER/CREATE + ограничение длины запроса (5000 символов).

**Генерация ссылок на портал**:
- Контракт: `https://goszakup.gov.kz/ru/egzcontract/cpublic/show/{source_id}`
- Объявление: `https://goszakup.gov.kz/ru/announce/index/{number_anno_base}`
- Лот: `https://goszakup.gov.kz/ru/announce/index/{number_anno_base}?tab=lots`

### 7.3 Системный промпт (`src/agent/prompts.py`)

**XML-структура**: role → boundaries → rules → anti-injection → reasoning → tools → examples → format → self-check → error handling.

**6-блочный формат ответа** (обязательный):
1. **Вердикт** — 1-3 предложения с конкретными цифрами
2. **Использованные данные** — период, фильтры, сущности, размер выборки
3. **Аналитика** — сравнение, медианы, отклонения, top-K с ссылками
4. **Метод оценки** — какой метод применён
5. **Уверенность и ограничения** — качество данных, риски, fallback-флаги
6. **Рекомендации** — действия по результатам

**Self-check**: 17 проверок перед отправкой ответа (вызван ли инструмент, все ли числа из результатов, ссылки скопированы из tool results, CI-диапазон для Fair Price и т.д.).

### 7.4 LLM-интеграция

- **Провайдер**: OpenAI-совместимый API
- **URL**: `https://nitec-ai.kz/api` (настраивается)
- **Модель**: `openai/gpt-oss-120b`
- **Параметры**: temperature=0.0, max_tokens=4096
- **Протокол**: `/v1/chat/completions` с tool calls
- **HTTP-клиент**: httpx.AsyncClient (persistent, connection pooling)

---

## 8. Слой 6: API и интерфейс

### 8.1 FastAPI-сервер (`src/api/main.py`)

**Endpoints**:

| Метод | Путь | Аутентификация | Назначение |
|-------|------|---------------|-----------|
| GET | `/api/health` | — | Health check (K8s probes) |
| GET | `/api/overview` | X-API-Key | Обзор данных в БД |
| POST | `/api/query` | X-API-Key | SQL-запросы |
| POST | `/api/fair-price` | X-API-Key | Расчёт справедливой цены |
| POST | `/api/chat` | X-API-Key | REST-чат |
| WS | `/ws/chat/{client_id}` | — | WebSocket-чат (основной) |
| GET | `/` | — | Веб-интерфейс чата |

**Ограничения**:
- WebSocket: 20 сообщений/мин на клиента, максимум 50 одновременных подключений
- API-key аутентификация: опциональная (timing-safe сравнение через `secrets.compare_digest`)

### 8.2 Frontend

Одностраничное веб-приложение (HTML + CSS + JS):
- **WebSocket-клиент** с автоматическим переподключением (3 сек)
- **Markdown-рендеринг** ответов (marked.js)
- **Двуязычность**: переключатель RU ↔ KK (сохраняется в localStorage)
- **Safety timeout**: 5.5 мин на ожидание ответа
- **Примеры вопросов**: готовые кнопки для типичных запросов

---

## 9. Оркестрация: Airflow

**DAG**: `goszakup_daily_sync`
**Расписание**: ежедневно в 06:00 UTC (12:00 по Астане)
**Макс. параллельных запусков**: 1

```
incremental_sync ──► run_etl ──► refresh_views
```

| Задача | Что делает | Таймаут |
|--------|-----------|---------|
| `incremental_sync` | Дозагрузка изменённых записей из API (sync_journal) | 2 часа |
| `run_etl` | Очистка + обогащение данных | 2 часа |
| `refresh_views` | Обновление 5 materialized views | 2 часа |

**Retry**: 2 попытки с задержкой 5 мин.

Обеспечивает актуальность данных с задержкой **< 24 часа** от момента публикации на портале (требование ТЗ).

---

## 10. Деплой

### 10.1 Docker Compose (5 сервисов)

```
┌────────────────────────────────────────────────────┐
│                 docker-compose.yml                  │
│                                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ postgres │  │   app    │  │  airflow-init    │ │
│  │ :5433    │  │  :8000   │  │  (one-shot)      │ │
│  └────┬─────┘  └────┬─────┘  └──────────────────┘ │
│       │              │                              │
│       │         ┌────┴─────┐  ┌──────────────────┐ │
│       │         │ depends  │  │ airflow-webserver │ │
│       └─────────┤   on     │  │  :8080           │ │
│                 │ postgres │  └──────────────────┘ │
│                 └──────────┘                        │
│                              ┌──────────────────┐  │
│                              │ airflow-scheduler │  │
│                              │  (background)     │  │
│                              └──────────────────┘  │
└────────────────────────────────────────────────────┘
```

- **postgres**: PostgreSQL 16-Alpine, порт 5433, persistent volume, init-скрипты (schema + indexes + views)
- **app**: Python 3.11-slim, non-root user, healthcheck `/api/health`
- **airflow-init**: миграция БД + создание admin-пользователя (одноразовый)
- **airflow-webserver**: UI Airflow на порту 8080
- **airflow-scheduler**: выполнение DAG-задач

### 10.2 Kubernetes (5 манифестов)

```
k8s/
├── namespace.yaml       # Пространство имён goszakup
├── configmap.yaml       # Несекретная конфигурация (хост, порт, модель LLM)
├── secret.yaml          # Секреты (пароли, токены API) — base64
├── postgres.yaml        # StatefulSet + Service + PVC (10Gi)
└── app.yaml             # Deployment (2 реплики) + Service
```

**PostgreSQL**: StatefulSet (1 реплика), PVC 10Gi, readiness/liveness probes, resource limits (256Mi-1Gi RAM, 250m-1000m CPU).

**App**: Deployment (2 реплики), init-контейнер (ожидание PostgreSQL), readiness/liveness probes, resource limits.

---

## 11. Безопасность

| Мера | Реализация |
|------|-----------|
| Секреты | Переменные окружения через `.env` (не в коде) |
| SQL injection | Blacklist ключевых слов + ограничение длины + только SELECT |
| Rate limiting | API: 5 req/s (token bucket), WebSocket: 20 msg/min |
| Аутентификация | API-key (X-API-Key header), timing-safe сравнение |
| Docker | Non-root user (`appuser`), минимальный образ (slim) |
| CORS | Настраиваемый whitelist origins |
| Anti-fabrication | Self-check в промпте, данные только из tool results |

---

## 12. Схема потока обработки запроса

Полный путь от вопроса пользователя до ответа:

```
Пользователь: "Оцени справедливость цены лота №2845731"
    │
    ▼
[Frontend] WebSocket → JSON {message: "..."}
    │
    ▼
[FastAPI] /ws/chat/{client_id}
    │  Rate limit check (20 msg/min)
    │  Create/get ReActAgent instance
    ▼
[ReActAgent] ask(question)
    │  1. Detect language → RU
    │  2. Classify → "справедливость_цены"
    │  3. Build system prompt + hint
    ▼
[LLM] nitec-ai.kz /v1/chat/completions
    │  Iteration 1:
    │  Thought: "Нужно найти лот и оценить цену"
    │  Action: evaluate_lot_price(lot_number="2845731")
    ▼
[Tools] evaluate_lot_price()
    │  1. SQL: найти лот в lots
    │  2. get_fair_price(enstru_code, region, unit_id)
    │  3. _find_analogues() → аналогичные контракты
    │  4. _assess_price() → вердикт (завышена/адекватна/занижена)
    ▼
[LLM] Iteration 2:
    │  Observation: {lot, fair_price, analogues, verdict}
    │  Thought: "Данных достаточно для ответа"
    │  Final answer (6 блоков):
    │
    ▼
[FastAPI] WebSocket → JSON {type: "response", content: "..."}
    │
    ▼
[Frontend] Markdown render → показать пользователю
```
