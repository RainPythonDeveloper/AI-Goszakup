# Схема хранения данных

**Проект:** AI-Агент анализа госзакупок РК
**СУБД:** PostgreSQL 16
**Расширения:** `pg_trgm` (нечёткий текстовый поиск)

---

## 1. Обзор

База данных состоит из **20 таблиц**, сгруппированных в три категории:

| Категория        | Кол-во | Назначение                                   |
|------------------|--------|----------------------------------------------|
| Справочники      | 6      | Нормативные справочные данные из API          |
| Основные таблицы | 9      | Сущности госзакупок (планы → лоты → договоры) |
| Служебные        | 5      | Аудит, синхронизация, метаданные              |

Дополнительно: **5 материализованных представлений** (витрины данных для аналитики) и **30+ индексов**.

---

## 2. ER-диаграмма (связи между таблицами)

```
                         ┌──────────────┐
                         │ ref_methods  │
                         │  (35 записей)│
                         └──────┬───────┘
                                │ method_id
          ┌─────────────────────┼──────────────────────┐
          │                     │                      │
          ▼                     ▼                      │
   ┌─────────────┐     ┌───────────────┐               │
   │    plans     │     │ announcements │               │
   │ (год. планы) │     │ (объявления)  │               │
   └──────┬──────┘     └───────┬───────┘               │
          │ plan_id            │ announcement_id        │
          │                    ├───────────────┐        │
          ▼                    ▼               ▼        │
   ┌─────────────┐     ┌─────────────┐ ┌──────────────┐│
   │    lots      │◄────│ applications│ │  contracts   ││
   │   (лоты)    │     │  (заявки)   │ │  (договоры)  ││
   └──────┬──────┘     └─────────────┘ └──────┬───────┘│
          │                                    │        │
          │         ┌──────────────────────────┤        │
          │         │              │            │        │
          │         ▼              ▼            ▼        │
          │  ┌──────────────┐ ┌─────────┐ ┌──────────┐  │
          │  │contract_subj.│ │payments │ │cont._acts│  │
          │  │(предметы дог)│ │(платежи)│ │  (акты)  │  │
          │  └──────────────┘ └─────────┘ └──────────┘  │
          │                                              │
          │         ┌────────────────────────────────────┘
          │         │
          ▼         ▼
   ┌─────────────────────────────────────────────────────┐
   │              СПРАВОЧНИКИ (ref_*)                     │
   ├──────────────┬──────────┬──────────┬────────────────┤
   │ ref_statuses │ ref_units│ ref_kato │  ref_enstru    │
   │  (148 зап.)  │(621 зап.)│(16764)   │  (коды ТРУ)   │
   └──────────────┴──────────┴──────────┴────────────────┘
                                              │
                                    ┌─────────┘
                                    ▼
                            ┌──────────────┐
                            │ref_currencies│
                            │  (50 зап.)   │
                            └──────────────┘

   ┌─────────────────────────────────────────────────────┐
   │              СЛУЖЕБНЫЕ ТАБЛИЦЫ                       │
   ├──────────┬──────────────┬──────────────┬────────────┤
   │ raw_data │data_quality_ │ sync_journal │ load_meta- │
   │          │    log       │              │   data     │
   └──────────┴──────────────┴──────────────┴────────────┘
                                    ┌────────────────────┐
                                    │ inflation_index    │
                                    │ (ИПЦ: 27 мес.)    │
                                    └────────────────────┘
```

---

## 3. Справочные таблицы (ref_*)

Загружаются из OWS v3 REST API на этапе инициализации.

### 3.1. ref_statuses — Справочник статусов

| Колонка       | Тип           | Ограничения                  | Описание                           |
|---------------|---------------|------------------------------|------------------------------------|
| `id`          | SERIAL        | PK                           | Автоинкремент                      |
| `code`        | VARCHAR(50)   | NOT NULL, UNIQUE(code, entity_type) | Код статуса из API         |
| `name_ru`     | VARCHAR(500)  |                              | Название на русском                |
| `name_kz`     | VARCHAR(500)  |                              | Название на казахском              |
| `entity_type` | VARCHAR(50)   | NOT NULL                     | Тип сущности: lot, contract, announcement, plan |

**Записей:** 148 | **Источник:** REST `/v3/refs/ref_trade_status`

### 3.2. ref_methods — Способы закупок

| Колонка   | Тип           | Ограничения    | Описание              |
|-----------|---------------|----------------|-----------------------|
| `id`      | SERIAL        | PK             | Автоинкремент         |
| `code`    | VARCHAR(50)   | UNIQUE, NOT NULL | Код метода            |
| `name_ru` | VARCHAR(500)  |                | Название (рус)        |
| `name_kz` | VARCHAR(500)  |                | Название (каз)        |

**Записей:** 35 | **Источник:** REST `/v3/refs/ref_buy_trade_methods`

### 3.3. ref_units — Единицы измерения

| Колонка   | Тип           | Ограничения    | Описание              |
|-----------|---------------|----------------|-----------------------|
| `id`      | SERIAL        | PK             | Автоинкремент         |
| `code`    | VARCHAR(50)   | UNIQUE, NOT NULL | Код ЕИ (796=Штука, 778=Упаковка) |
| `name_ru` | VARCHAR(500)  |                | Название (рус)        |
| `name_kz` | VARCHAR(500)  |                | Название (каз)        |

**Записей:** 621 | **Источник:** REST `/v3/refs/ref_units`

### 3.4. ref_kato — Классификатор территорий (КАТО)

| Колонка   | Тип           | Ограничения    | Описание              |
|-----------|---------------|----------------|-----------------------|
| `id`      | SERIAL        | PK             | Автоинкремент         |
| `code`    | VARCHAR(20)   | UNIQUE, NOT NULL | Код КАТО              |
| `name_ru` | VARCHAR(500)  |                | Название (рус)        |
| `name_kz` | VARCHAR(500)  |                | Название (каз)        |
| `region`  | VARCHAR(200)  |                | Область (для региональных коэффициентов) |
| `level`   | SMALLINT      |                | 1=область, 2=район, 3=город/село |

**Записей:** 16 764 | **Источник:** REST `/v3/refs/ref_kato`

### 3.5. ref_enstru — Единый номенклатурный справочник ТРУ

| Колонка       | Тип            | Ограничения    | Описание              |
|---------------|----------------|----------------|-----------------------|
| `id`          | SERIAL         | PK             | Автоинкремент         |
| `code`        | VARCHAR(50)    | UNIQUE, NOT NULL | Код ENSTRU            |
| `name_ru`     | VARCHAR(1000)  |                | Название (рус)        |
| `name_kz`     | VARCHAR(1000)  |                | Название (каз)        |
| `parent_code` | VARCHAR(50)    |                | Код родительской категории |
| `level`       | SMALLINT       |                | Уровень иерархии      |

**Источник:** REST `/v3/refs/ref_enstru`

### 3.6. ref_currencies — Справочник валют

| Колонка   | Тип           | Ограничения    | Описание              |
|-----------|---------------|----------------|-----------------------|
| `id`      | SERIAL        | PK             | Автоинкремент         |
| `code`    | VARCHAR(10)   | UNIQUE, NOT NULL | ISO-код валюты        |
| `name_ru` | VARCHAR(100)  |                | Название (рус)        |

**Записей:** 50 | **Источник:** REST `/v3/refs/ref_currency`

---

## 4. Основные таблицы

Все основные таблицы имеют двойную идентификацию:
- `id` — BIGSERIAL, внутренний автоинкрементный PK
- `source_id` — BIGINT UNIQUE, оригинальный ID из API (используется для ссылок на портал)

### 4.1. subjects — Участники закупок

Заказчики, поставщики и организаторы.

| Колонка        | Тип            | Ограничения    | Описание                     |
|----------------|----------------|----------------|------------------------------|
| `id`           | BIGSERIAL      | PK             | Внутренний ID                |
| `source_id`    | BIGINT         | UNIQUE         | ID из API                    |
| `bin`          | VARCHAR(12)    | NOT NULL, UNIQUE | БИН организации              |
| `iin`          | VARCHAR(12)    |                | ИИН (для ИП)                 |
| `name_ru`      | VARCHAR(1000)  |                | Наименование (рус)           |
| `name_kz`      | VARCHAR(1000)  |                | Наименование (каз)           |
| `is_customer`  | BOOLEAN        | DEFAULT FALSE  | Является заказчиком          |
| `is_organizer` | BOOLEAN        | DEFAULT FALSE  | Является организатором       |
| `is_supplier`  | BOOLEAN        | DEFAULT FALSE  | Является поставщиком         |
| `kato_code`    | VARCHAR(20)    |                | КАТО-код местоположения      |
| `address`      | TEXT           |                | Адрес                        |
| `created_at`   | TIMESTAMP      | DEFAULT NOW()  | Дата создания записи         |
| `updated_at`   | TIMESTAMP      | DEFAULT NOW()  | Дата обновления записи       |

**Источник:** GraphQL `Subject`

### 4.2. plans — Годовые планы закупок

| Колонка          | Тип            | Ограничения    | Описание                    |
|------------------|----------------|----------------|-----------------------------|
| `id`             | BIGSERIAL      | PK             | Внутренний ID               |
| `source_id`      | BIGINT         | UNIQUE         | ID из API                   |
| `bin`            | VARCHAR(12)    | NOT NULL       | БИН заказчика               |
| `enstru_code`    | VARCHAR(50)    |                | Код ENSTRU (ТРУ)            |
| `plan_sum`       | NUMERIC(20,2)  |                | Плановая сумма              |
| `plan_count`     | NUMERIC(20,4)  |                | Плановое количество         |
| `method_id`      | INTEGER        | FK → ref_methods | Способ закупки             |
| `year`           | SMALLINT       |                | Плановый год                |
| `month`          | SMALLINT       |                | Плановый месяц              |
| `source_funding` | VARCHAR(500)   |                | Источник финансирования     |
| `delivery_kato`  | VARCHAR(20)    |                | КАТО места поставки         |
| `status`         | VARCHAR(100)   |                | Статус плана                |
| `created_at`     | TIMESTAMP      | DEFAULT NOW()  | Дата создания               |
| `updated_at`     | TIMESTAMP      | DEFAULT NOW()  | Дата обновления             |

**Источник:** GraphQL `Plans`

### 4.3. announcements — Объявления о закупках

| Колонка           | Тип            | Ограничения        | Описание                   |
|-------------------|----------------|--------------------|-----------------------------|
| `id`              | BIGSERIAL      | PK                 | Внутренний ID               |
| `source_id`       | BIGINT         | UNIQUE             | ID из API                   |
| `number_anno`     | VARCHAR(100)   |                    | Номер объявления            |
| `name_ru`         | TEXT           |                    | Название (рус)              |
| `total_sum`       | NUMERIC(20,2)  |                    | Общая сумма                 |
| `method_id`       | INTEGER        | FK → ref_methods   | Способ закупки              |
| `status_id`       | INTEGER        | FK → ref_statuses  | Статус объявления           |
| `organizer_bin`   | VARCHAR(12)    |                    | БИН организатора            |
| `customer_bin`    | VARCHAR(12)    |                    | БИН заказчика               |
| `start_date`      | TIMESTAMP      |                    | Дата начала приёма заявок   |
| `end_date`        | TIMESTAMP      |                    | Дата окончания              |
| `publish_date`    | TIMESTAMP      |                    | Дата публикации             |
| `is_construction` | BOOLEAN        | DEFAULT FALSE      | Строительные работы         |
| `lot_count`       | INTEGER        |                    | Количество лотов            |
| `source_system`   | SMALLINT       |                    | 1=Mitwork, 2=Samruk, 3=Goszakup |
| `created_at`      | TIMESTAMP      | DEFAULT NOW()      | Дата создания               |
| `updated_at`      | TIMESTAMP      | DEFAULT NOW()      | Дата обновления             |

**Источник:** GraphQL `TrdBuy` (фильтр по `orgBin`)
**URL портала:** `https://goszakup.gov.kz/ru/announce/index/{number_anno_base}`

### 4.4. lots — Лоты

| Колонка           | Тип            | Ограничения         | Описание                  |
|-------------------|----------------|---------------------|---------------------------|
| `id`              | BIGSERIAL      | PK                  | Внутренний ID             |
| `source_id`       | BIGINT         | UNIQUE              | ID из API                 |
| `announcement_id` | BIGINT         | FK → announcements  | Родительское объявление   |
| `lot_number`      | VARCHAR(50)    |                     | Номер лота                |
| `name_ru`         | TEXT           |                     | Название (рус)            |
| `name_ru_clean`   | TEXT           |                     | Очищенное название (ETL)  |
| `description`     | TEXT           |                     | Описание                  |
| `amount`          | NUMERIC(20,2)  |                     | Общая сумма лота          |
| `count`           | NUMERIC(20,4)  |                     | Количество                |
| `unit_id`         | INTEGER        | FK → ref_units      | Единица измерения         |
| `price_per_unit`  | NUMERIC(20,4)  |                     | Расчётная: amount / count |
| `enstru_code`     | VARCHAR(50)    |                     | Код ENSTRU (ТРУ)          |
| `customer_bin`    | VARCHAR(12)    |                     | БИН заказчика             |
| `delivery_kato`   | VARCHAR(20)    |                     | КАТО места поставки       |
| `status_id`       | INTEGER        | FK → ref_statuses   | Статус лота               |
| `plan_id`         | BIGINT         | FK → plans          | Связанный план            |
| `created_at`      | TIMESTAMP      | DEFAULT NOW()       | Дата создания             |
| `updated_at`      | TIMESTAMP      | DEFAULT NOW()       | Дата обновления           |

**Источник:** GraphQL `Lots` (фильтр по `customerBin`)
**URL портала:** `https://goszakup.gov.kz/ru/announce/index/{number_anno_base}?tab=lots`

### 4.5. contracts — Договоры

| Колонка           | Тип            | Ограничения         | Описание                   |
|-------------------|----------------|---------------------|-----------------------------|
| `id`              | BIGSERIAL      | PK                  | Внутренний ID               |
| `source_id`       | BIGINT         | UNIQUE              | ID из API                   |
| `contract_number` | VARCHAR(100)   |                     | Номер договора              |
| `announcement_id` | BIGINT         | FK → announcements  | Родительское объявление     |
| `supplier_bin`    | VARCHAR(12)    |                     | БИН поставщика              |
| `customer_bin`    | VARCHAR(12)    |                     | БИН заказчика               |
| `contract_sum`    | NUMERIC(20,2)  |                     | Сумма договора              |
| `sign_date`       | DATE           |                     | Дата подписания             |
| `ec_end_date`     | DATE           |                     | Дата окончания              |
| `status_id`       | INTEGER        | FK → ref_statuses   | Статус договора             |
| `contract_type`   | VARCHAR(200)   |                     | Тип договора                |
| `lot_id`          | BIGINT         | FK → lots           | Связанный лот               |
| `source_system`   | SMALLINT       |                     | Источник: 1/2/3             |
| `created_at`      | TIMESTAMP      | DEFAULT NOW()       | Дата создания               |
| `updated_at`      | TIMESTAMP      | DEFAULT NOW()       | Дата обновления             |

**Источник:** GraphQL `Contract`
**URL портала:** `https://goszakup.gov.kz/ru/egzcontract/cpublic/show/{source_id}`

### 4.6. contract_subjects — Предметы договора (ТРУ)

Ключевая таблица для аналитики Fair Price и детекции аномалий.

| Колонка          | Тип            | Ограничения     | Описание                  |
|------------------|----------------|-----------------|---------------------------|
| `id`             | BIGSERIAL      | PK              | Внутренний ID             |
| `source_id`      | BIGINT         | UNIQUE          | ID из API                 |
| `contract_id`    | BIGINT         | FK → contracts  | Родительский договор      |
| `enstru_code`    | VARCHAR(50)    |                 | Код ENSTRU (ТРУ)          |
| `name_ru`        | TEXT           |                 | Название (рус)            |
| `quantity`       | NUMERIC(20,4)  |                 | Количество                |
| `unit_id`        | INTEGER        | FK → ref_units  | Единица измерения         |
| `price_per_unit` | NUMERIC(20,4)  |                 | Цена за единицу           |
| `total_price`    | NUMERIC(20,2)  |                 | Общая стоимость           |
| `delivery_kato`  | VARCHAR(20)    |                 | КАТО места поставки       |
| `created_at`     | TIMESTAMP      | DEFAULT NOW()   | Дата создания             |

**Источник:** GraphQL `ContractUnits` → вложенный объект `Plans`

### 4.7. applications — Заявки поставщиков

| Колонка           | Тип            | Ограничения         | Описание                  |
|-------------------|----------------|---------------------|---------------------------|
| `id`              | BIGSERIAL      | PK                  | Внутренний ID             |
| `source_id`       | BIGINT         | UNIQUE              | ID из API                 |
| `announcement_id` | BIGINT         | FK → announcements  | Объявление                |
| `supplier_bin`    | VARCHAR(12)    |                     | БИН поставщика            |
| `lot_id`          | BIGINT         | FK → lots           | Лот                       |
| `price_offer`     | NUMERIC(20,2)  |                     | Ценовое предложение       |
| `discount`        | NUMERIC(10,4)  |                     | Скидка (%)                |
| `submission_date` | TIMESTAMP      |                     | Дата подачи               |
| `created_at`      | TIMESTAMP      | DEFAULT NOW()       | Дата создания             |

**Источник:** GraphQL `Application`

### 4.8. payments — Казначейские платежи

| Колонка        | Тип            | Ограничения     | Описание                  |
|----------------|----------------|-----------------|---------------------------|
| `id`           | BIGSERIAL      | PK              | Внутренний ID             |
| `source_id`    | BIGINT         | UNIQUE          | ID из API                 |
| `contract_id`  | BIGINT         | FK → contracts  | Родительский договор      |
| `amount`       | NUMERIC(20,2)  |                 | Сумма платежа             |
| `payment_date` | DATE           |                 | Дата платежа              |
| `status`       | VARCHAR(100)   |                 | Статус                    |
| `created_at`   | TIMESTAMP      | DEFAULT NOW()   | Дата создания             |

**Источник:** GraphQL `Payment`

### 4.9. contract_acts — Электронные акты

| Колонка         | Тип            | Ограничения     | Описание                  |
|-----------------|----------------|-----------------|---------------------------|
| `id`            | BIGSERIAL      | PK              | Внутренний ID             |
| `source_id`     | BIGINT         | UNIQUE          | ID из API                 |
| `contract_id`   | BIGINT         | FK → contracts  | Родительский договор      |
| `act_number`    | VARCHAR(100)   |                 | Номер акта                |
| `status`        | VARCHAR(100)   |                 | Статус                    |
| `approval_date` | DATE           |                 | Дата утверждения          |
| `amount`        | NUMERIC(20,2)  |                 | Сумма акта                |
| `created_at`    | TIMESTAMP      | DEFAULT NOW()   | Дата создания             |

**Источник:** GraphQL `ContractAct`

---

## 5. Служебные таблицы

### 5.1. raw_data — Сырые данные (аудит)

Хранит оригинальный JSON из API для возможного ре-процессинга.

| Колонка           | Тип            | Ограничения    | Описание                  |
|-------------------|----------------|----------------|---------------------------|
| `id`              | BIGSERIAL      | PK             | Внутренний ID             |
| `source_endpoint` | VARCHAR(200)   | NOT NULL       | Эндпоинт API             |
| `source_id`       | BIGINT         |                | ID сущности из API        |
| `raw_json`        | JSONB          | NOT NULL       | Оригинальный JSON         |
| `loaded_at`       | TIMESTAMP      | DEFAULT NOW()  | Дата загрузки             |
| `processed`       | BOOLEAN        | DEFAULT FALSE  | Обработано ETL?           |

### 5.2. data_quality_log — Лог качества данных

| Колонка       | Тип            | Ограничения                   | Описание              |
|---------------|----------------|-------------------------------|-----------------------|
| `id`          | BIGSERIAL      | PK                            | Внутренний ID         |
| `table_name`  | VARCHAR(100)   | NOT NULL                      | Имя таблицы           |
| `record_id`   | BIGINT         |                               | ID записи             |
| `source_id`   | BIGINT         |                               | ID из API             |
| `issue`       | TEXT           | NOT NULL                      | Описание проблемы     |
| `severity`    | VARCHAR(20)    | DEFAULT 'warning'             | info / warning / error |
| `detected_at` | TIMESTAMP      | DEFAULT NOW()                 | Дата обнаружения      |

### 5.3. sync_journal — Журнал синхронизации

Отслеживает инкрементальные обновления от API (Change Data Capture).

| Колонка        | Тип           | Ограничения    | Описание                       |
|----------------|---------------|----------------|--------------------------------|
| `id`           | BIGSERIAL     | PK             | Внутренний ID                  |
| `entity_type`  | VARCHAR(50)   | NOT NULL       | Тип: trd-buy, contract, lot... |
| `entity_id`    | BIGINT        | NOT NULL       | ID сущности                    |
| `action`       | CHAR(1)       | NOT NULL       | U = update, D = delete         |
| `event_date`   | TIMESTAMP     | NOT NULL       | Дата события в API             |
| `api_url`      | TEXT          |                | URL для запроса                |
| `processed`    | BOOLEAN       | DEFAULT FALSE  | Обработано?                    |
| `processed_at` | TIMESTAMP     |                | Дата обработки                 |
| `created_at`   | TIMESTAMP     | DEFAULT NOW()  | Дата создания                  |

### 5.4. load_metadata — Метаданные загрузки

Хранит прогресс Full Load для возобновления при сбоях.

| Колонка          | Тип           | Ограничения        | Описание                     |
|------------------|---------------|--------------------|-----------------------------|
| `id`             | BIGSERIAL     | PK                 | Внутренний ID               |
| `entity_type`    | VARCHAR(50)   | NOT NULL           | Тип сущности                |
| `last_source_id` | BIGINT        |                    | Последний загруженный ID    |
| `total_loaded`   | INTEGER       | DEFAULT 0          | Всего загружено записей     |
| `status`         | VARCHAR(20)   | DEFAULT 'pending'  | pending/running/completed/failed |
| `started_at`     | TIMESTAMP     |                    | Начало загрузки             |
| `completed_at`   | TIMESTAMP     |                    | Окончание загрузки          |
| `error_message`  | TEXT          |                    | Сообщение об ошибке         |

### 5.5. inflation_index — Индексы потребительских цен (ИПЦ)

Используется в формуле Fair Price для инфляционной корректировки.

| Колонка | Тип            | Ограничения           | Описание                          |
|---------|----------------|-----------------------|-----------------------------------|
| `id`    | SERIAL         | PK                    | Автоинкремент                     |
| `year`  | SMALLINT       | NOT NULL, UNIQUE(year,month) | Год                        |
| `month` | SMALLINT       | NOT NULL              | Месяц                             |
| `cpi`   | NUMERIC(10,4)  | NOT NULL              | Кумулятивный ИПЦ (база = 2024-01 = 100.0) |

**Предзаполнена:** 27 записей (2024-01 — 2026-03), данные из stat.gov.kz.

---

## 6. Связи между таблицами (Foreign Keys)

```
plans.method_id              → ref_methods.id
announcements.method_id      → ref_methods.id
announcements.status_id      → ref_statuses.id
lots.announcement_id         → announcements.id
lots.unit_id                 → ref_units.id
lots.status_id               → ref_statuses.id
lots.plan_id                 → plans.id
contracts.announcement_id    → announcements.id
contracts.status_id          → ref_statuses.id
contracts.lot_id             → lots.id
contract_subjects.contract_id → contracts.id
contract_subjects.unit_id    → ref_units.id
applications.announcement_id → announcements.id
applications.lot_id          → lots.id
payments.contract_id         → contracts.id
contract_acts.contract_id    → contracts.id
```

Всего: **16 внешних ключей**.

---

## 7. Стратегия индексирования

30+ индексов, оптимизированных под запросы AI-агента.

### 7.1. Справочники

| Индекс                      | Таблица      | Колонка(и)     | Тип     |
|------------------------------|-------------|----------------|---------|
| `idx_ref_kato_region`        | ref_kato    | region         | B-tree  |
| `idx_ref_enstru_parent`      | ref_enstru  | parent_code    | B-tree  |

### 7.2. Основные таблицы

| Индекс                          | Таблица            | Колонка(и)        | Тип       | Назначение                        |
|----------------------------------|--------------------|--------------------|-----------|-----------------------------------|
| `idx_plans_bin`                  | plans              | bin                | B-tree    | Поиск планов по организации       |
| `idx_plans_enstru`               | plans              | enstru_code        | B-tree    | Поиск по ТРУ                      |
| `idx_plans_year`                 | plans              | year               | B-tree    | Фильтр по году                    |
| `idx_announcements_number`       | announcements      | number_anno        | B-tree    | Поиск по номеру                   |
| `idx_announcements_organizer`    | announcements      | organizer_bin      | B-tree    | Поиск по организатору             |
| `idx_announcements_customer`     | announcements      | customer_bin       | B-tree    | Поиск по заказчику                |
| `idx_announcements_publish_date` | announcements      | publish_date       | B-tree    | Фильтр по дате                    |
| `idx_announcements_status`       | announcements      | status_id          | B-tree    | Фильтр по статусу                 |
| `idx_lots_announcement`          | lots               | announcement_id    | B-tree    | JOIN с объявлениями               |
| `idx_lots_enstru`                | lots               | enstru_code        | B-tree    | Поиск по ENSTRU                   |
| `idx_lots_customer`              | lots               | customer_bin       | B-tree    | Поиск по заказчику                |
| `idx_lots_delivery_kato`         | lots               | delivery_kato      | B-tree    | Фильтр по региону                 |
| `idx_lots_status`                | lots               | status_id          | B-tree    | Фильтр по статусу                 |
| `idx_lots_price`                 | lots               | price_per_unit     | B-tree    | Ценовой анализ                    |
| `idx_lots_name_trgm`            | lots               | name_ru_clean      | **GIN (pg_trgm)** | Нечёткий текстовый поиск   |
| `idx_contracts_announcement`     | contracts          | announcement_id    | B-tree    | JOIN с объявлениями               |
| `idx_contracts_supplier`         | contracts          | supplier_bin       | B-tree    | Поиск по поставщику               |
| `idx_contracts_customer`         | contracts          | customer_bin       | B-tree    | Поиск по заказчику                |
| `idx_contracts_sign_date`        | contracts          | sign_date          | B-tree    | Временной анализ                  |
| `idx_contracts_status`           | contracts          | status_id          | B-tree    | Фильтр по статусу                 |
| `idx_contracts_lot`              | contracts          | lot_id             | B-tree    | JOIN с лотами                     |
| `idx_contract_subjects_contract` | contract_subjects  | contract_id        | B-tree    | JOIN с договорами                 |
| `idx_contract_subjects_enstru`   | contract_subjects  | enstru_code        | B-tree    | Ценовая аналитика                 |
| `idx_contract_subjects_kato`     | contract_subjects  | delivery_kato      | B-tree    | Региональный анализ               |
| `idx_applications_announcement`  | applications       | announcement_id    | B-tree    | JOIN                              |
| `idx_applications_supplier`      | applications       | supplier_bin       | B-tree    | Поиск по поставщику               |
| `idx_applications_lot`           | applications       | lot_id             | B-tree    | JOIN                              |
| `idx_payments_contract`          | payments           | contract_id        | B-tree    | JOIN с договорами                 |
| `idx_payments_date`              | payments           | payment_date       | B-tree    | Временной анализ                  |
| `idx_contract_acts_contract`     | contract_acts      | contract_id        | B-tree    | JOIN с договорами                 |

### 7.3. Служебные (частичные индексы)

| Индекс                        | Таблица          | Тип                   | Условие               |
|-------------------------------|------------------|------------------------|------------------------|
| `idx_raw_data_endpoint`       | raw_data         | B-tree                 | —                      |
| `idx_raw_data_processed`      | raw_data         | **Partial** B-tree     | `WHERE NOT processed`  |
| `idx_sync_journal_processed`  | sync_journal     | **Partial** B-tree     | `WHERE NOT processed`  |
| `idx_sync_journal_entity`     | sync_journal     | B-tree (composite)     | (entity_type, entity_id) |
| `idx_dq_log_table`            | data_quality_log | B-tree                 | —                      |
| `idx_dq_log_severity`         | data_quality_log | B-tree                 | —                      |

**Partial indexes** — индексируют только необработанные записи, экономя место и ускоряя инкрементальную синхронизацию.

---

## 8. Материализованные представления (Витрины данных)

Обновляются ETL-пайплайном после каждой загрузки (`REFRESH MATERIALIZED VIEW CONCURRENTLY`).

### 8.1. mv_price_statistics — Ценовая статистика

Основная витрина для Fair Price и детекции аномалий.

**Группировка:** `(enstru_code, unit_id, delivery_kato, region, quarter, year)`

| Колонка             | Тип           | Описание                                   |
|---------------------|---------------|---------------------------------------------|
| `enstru_code`       | VARCHAR(50)   | Код ТРУ                                     |
| `unit_id`           | INTEGER       | Единица измерения                            |
| `delivery_kato`     | VARCHAR(20)   | КАТО места поставки                          |
| `delivery_region`   | VARCHAR(200)  | Область (из ref_kato)                        |
| `period`            | DATE          | Начало квартала                              |
| `year`              | SMALLINT      | Год                                          |
| `sample_size`       | BIGINT        | Количество записей                           |
| `weighted_avg_price`| NUMERIC       | Средневзвешенная цена                        |
| `median_price`      | NUMERIC       | Медианная цена                               |
| `q1`                | NUMERIC       | 1-й квартиль (25%)                           |
| `q3`                | NUMERIC       | 3-й квартиль (75%)                           |
| `min_price`         | NUMERIC       | Минимальная цена                             |
| `max_price`         | NUMERIC       | Максимальная цена                            |
| `std_dev`           | NUMERIC       | Стандартное отклонение                       |
| `total_quantity`    | NUMERIC       | Суммарное количество                         |
| `total_sum`         | NUMERIC       | Суммарная стоимость                          |

**Фильтры:** `price_per_unit > 0`, `sign_date IS NOT NULL`, год ∈ {2024,2025,2026}, статус ≠ 330 (расторгнутые).

### 8.2. mv_volume_trends — Тренды объёмов

Для выявления завышения объёмов закупок.

**Группировка:** `(enstru_code, customer_bin, year)`

| Колонка            | Тип           | Описание                    |
|--------------------|---------------|-----------------------------|
| `enstru_code`      | VARCHAR(50)   | Код ТРУ                     |
| `customer_bin`     | VARCHAR(12)   | БИН заказчика               |
| `customer_name`    | VARCHAR(1000) | Наименование (рус)          |
| `customer_name_kz` | VARCHAR(1000) | Наименование (каз)          |
| `year`             | SMALLINT      | Год                         |
| `lot_count`        | BIGINT        | Кол-во лотов                |
| `total_quantity`   | NUMERIC       | Суммарное количество        |
| `total_amount`     | NUMERIC       | Суммарная сумма             |
| `avg_price_per_unit` | NUMERIC     | Средняя цена за единицу     |

**Фильтр:** только лоты со статусом 360 (завершённые), с ENSTRU и датой публикации.

### 8.3. mv_supplier_stats — Статистика поставщиков

Для анализа концентрации и монополизации.

**Группировка:** `(supplier_bin)`

| Колонка            | Тип           | Описание                    |
|--------------------|---------------|-----------------------------|
| `supplier_bin`     | VARCHAR(12)   | БИН поставщика              |
| `supplier_name`    | VARCHAR(1000) | Наименование (рус)          |
| `supplier_name_kz` | VARCHAR(1000) | Наименование (каз)          |
| `contract_count`   | BIGINT        | Кол-во договоров            |
| `total_sum`        | NUMERIC       | Общая сумма                 |
| `avg_contract_sum` | NUMERIC       | Средняя сумма договора      |
| `unique_customers` | BIGINT        | Кол-во уникальных заказчиков|
| `first_contract`   | DATE          | Дата первого договора       |
| `last_contract`    | DATE          | Дата последнего договора    |

### 8.4. mv_regional_coefficients — Региональные коэффициенты

Для формулы Fair Price: учёт региональной разницы цен.

**Группировка:** `(enstru_code, unit_id, region)`

| Колонка           | Тип           | Описание                    |
|-------------------|---------------|-----------------------------|
| `enstru_code`     | VARCHAR(50)   | Код ТРУ                     |
| `unit_id`         | INTEGER       | Единица измерения           |
| `region`          | VARCHAR(200)  | Область                     |
| `regional_median` | NUMERIC       | Медианная цена в регионе    |
| `sample_size`     | BIGINT        | Размер выборки              |

**Фильтры:** аналогичны `mv_price_statistics`.

### 8.5. mv_data_overview — Обзор данных

Для healthcheck и отчётов о состоянии БД.

| Колонка  | Тип           | Описание           |
|----------|---------------|--------------------|
| `entity` | VARCHAR       | Имя таблицы        |
| `cnt`    | BIGINT        | Кол-во записей     |

Содержит counts для всех 9 основных таблиц.

---

## 9. Объёмы данных (актуальные)

| Таблица              | Записей   | Описание                 |
|----------------------|-----------|--------------------------|
| `subjects`           | ~10 000   | Участники закупок        |
| `plans`              | ~50 000   | Годовые планы            |
| `announcements`      | ~66 600   | Объявления               |
| `lots`               | ~247 600  | Лоты                     |
| `contracts`          | ~32 500   | Договоры (2024–2026)     |
| `contract_subjects`  | ~94 000   | Предметы договоров       |
| `applications`       | ~30 000   | Заявки поставщиков       |
| `payments`           | ~20 000   | Казначейские платежи     |
| `contract_acts`      | ~15 000   | Электронные акты         |
| **ref_statuses**     | 148       | Справочник статусов      |
| **ref_methods**      | 35        | Способы закупок          |
| **ref_units**        | 621       | Единицы измерения        |
| **ref_kato**         | 16 764    | КАТО                     |
| **ref_currencies**   | 50        | Валюты                   |
| **inflation_index**  | 27        | ИПЦ (2024-01 — 2026-03) |

**Общий объём:** ~570 000 записей в основных таблицах, ~18 000 в справочниках.
**Период данных:** 2024–2026 (старые данные удалены).
**Организации:** 27 БИНов (казахстанские гос. организации).

---

## 10. Паттерн UPSERT

Все основные таблицы используют `INSERT ... ON CONFLICT (source_id) DO UPDATE SET ...` — идемпотентная загрузка:

```sql
INSERT INTO contracts (source_id, contract_number, supplier_bin, ...)
VALUES ($1, $2, $3, ...)
ON CONFLICT (source_id)
DO UPDATE SET
    contract_number = EXCLUDED.contract_number,
    supplier_bin    = EXCLUDED.supplier_bin,
    ...
    updated_at      = NOW();
```

Это позволяет безопасно перезапускать загрузку без дублирования данных.

---

## 11. Инициализация БД

При запуске PostgreSQL в Docker выполняются скрипты в порядке:

1. `01_schema.sql` — создание таблиц + расширение pg_trgm + предзаполнение inflation_index
2. `02_indexes.sql` — создание 30+ индексов
3. `03_views.sql` — создание 5 материализованных представлений
4. `04_airflow_db.sh` — создание отдельной БД `airflow` для Apache Airflow
