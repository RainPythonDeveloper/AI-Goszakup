# AI Госзакупки РК

AI-агент для анализа государственных закупок Республики Казахстан.

Система загружает данные 27 организаций из API [goszakup.gov.kz](https://ows.goszakup.gov.kz) (OWS v3), обрабатывает их и предоставляет чат-интерфейс с аналитикой на русском и казахском языках.

---

## Что умеет

- Отвечает на вопросы по закупкам через чат (WebSocket)
- Рассчитывает справедливую цену (медиана, региональный коэффициент, инфляция, сезонность)
- Обнаруживает ценовые аномалии (IQR + Isolation Forest)
- Анализирует концентрацию поставщиков
- Выявляет отклонения объёмов закупок
- Формирует прямые ссылки на портал goszakup.gov.kz

---

## Архитектура

```
API goszakup.gov.kz (REST + GraphQL)
        │
        ▼
  Data Ingestion  ─── загрузка справочников и данных
        │
        ▼
  ETL Pipeline    ─── очистка, нормализация, обогащение
        │
        ▼
  PostgreSQL 16   ─── хранение + 5 materialized views
        │
        ▼
  Analytics       ─── IQR, Isolation Forest, Fair Price
        │
        ▼
  AI Agent (LLM)  ─── ReAct-агент с 7 инструментами
        │
        ▼
  Chat Interface  ─── FastAPI + WebSocket
```

---

## Быстрый старт

### Требования

- Docker и Docker Compose
- API-токен goszakup.gov.kz ([получить здесь](https://ows.goszakup.gov.kz))
- API-ключ LLM (nitec-ai.kz или другой OpenAI-совместимый провайдер)

### 1. Клонируйте и настройте

```bash
git clone https://github.com/RainPythonDeveloper/AiGoszakup.git
cd AiGoszakup
cp .env.example .env
```

Откройте `.env` и заполните три обязательных поля:

```dotenv
GOSZAKUP_API_TOKEN=ваш_токен
POSTGRES_PASSWORD=ваш_пароль
LLM_API_KEY=ваш_ключ
```

### 2. Запустите

```bash
docker-compose up -d
```

Будут запущены:

- **PostgreSQL 16** (порт 5433) — с автоматическим созданием схемы БД
- **Приложение** (порт 8080) — FastAPI-сервер + чат
- **Airflow** (порт 8081) — автоматическая синхронизация данных

### 3. Откройте

```
http://localhost:8080
```

### Остановка

```bash
docker-compose down        # остановить
docker-compose down -v     # остановить + удалить данные
```

---

## Локальный запуск (без Docker для приложения)

Если нужно запустить приложение локально, а PostgreSQL — через Docker:

```bash
# 1. Python-окружение
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Настройте .env
cp .env.example .env
# заполните GOSZAKUP_API_TOKEN, POSTGRES_PASSWORD, LLM_API_KEY

# 3. Поднимите только PostgreSQL
docker-compose up -d postgres

# 4. Запустите
python3 run.py
```

Команда `python3 run.py` выполнит полный цикл:

1. Проверит подключение к PostgreSQL
2. Загрузит справочники (КАТО, единицы измерения, способы закупки, статусы)
3. Загрузит данные из API (объявления, лоты, договоры, планы) — **10-30 минут при первом запуске**
4. Запустит ETL (очистка, нормализация, обогащение, materialized views)
5. Запустит сервер на `http://localhost:8080`

---

## CLI-команды

| Команда            | Что делает                                                    |
| ------------------------- | ---------------------------------------------------------------------- |
| `python3 run.py`        | Полный цикл: БД → загрузка → ETL → сервер |
| `python3 run.py server` | Только сервер (данные уже загружены)     |
| `python3 run.py load`   | Только загрузка данных из API                    |
| `python3 run.py etl`    | Только ETL (очистка + обновление views)         |
| `python3 run.py status` | Показать статус данных в БД                     |

---

## Стек технологий

| Компонент             | Технология              |
| ------------------------------ | --------------------------------- |
| Язык                       | Python 3.11                       |
| База данных          | PostgreSQL 16                     |
| API-сервер               | FastAPI + Uvicorn + WebSocket     |
| HTTP-клиент              | httpx (async)                     |
| ML                             | scikit-learn (Isolation Forest)   |
| LLM                            | OpenAI-совместимый API |
| Оркестрация         | Apache Airflow 2.10               |
| Контейнеризация | Docker, Docker Compose            |
| Деплой                   | Kubernetes                        |

---

## Структура проекта

```
AiGoszakup/
├── run.py                      # Точка запуска (CLI)
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── .dockerignore
├── .gitignore
├── introspect_api.py           # Утилита интроспекции API
├── schema.json                 # JSON-схема API
│
├── src/
│   ├── config.py               # Конфигурация (27 BIN-ов, API, БД, LLM)
│   ├── ingestion/              # Загрузка данных из API
│   │   ├── api_client.py       #   GraphQL + REST клиент
│   │   ├── data_loader.py      #   Загрузка сущностей (UPSERT)
│   │   ├── seed_refs.py        #   Загрузка справочников
│   │   └── rate_limiter.py     #   Ограничение запросов (token bucket)
│   ├── etl/                    # ETL-пайплайн
│   │   ├── pipeline.py         #   Оркестрация
│   │   ├── cleaner.py          #   Очистка данных
│   │   └── enricher.py         #   Обогащение + MV refresh
│   ├── db/                     # SQL
│   │   ├── schema.sql          #   Таблицы
│   │   ├── indexes.sql         #   Индексы
│   │   └── views.sql           #   5 materialized views
│   ├── analytics/              # Аналитика
│   │   ├── fair_price.py       #   Справедливая цена (Median × RegCoeff × CPI × Season)
│   │   └── anomaly_detector.py #   Детекция аномалий (IQR + Isolation Forest)
│   ├── agent/                  # AI-агент
│   │   ├── prompts.py          #   Системный промпт (RU/KZ)
│   │   ├── react_agent.py      #   ReAct-цикл
│   │   └── tools.py            #   7 инструментов агента
│   ├── api/
│   │   └── main.py             #   FastAPI + WebSocket
│   └── evaluation/
│       └── eval_pipeline.py    #   40 тест-кейсов (точность >= 85%)
│
├── frontend/                   # Веб-интерфейс чата
│   ├── index.html
│   ├── css/style.css
│   └── js/app.js
│
├── dags/
│   └── goszakup_sync.py        # Airflow DAG (ежедневная синхронизация)
│
├── k8s/                        # Kubernetes-манифесты
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── postgres.yaml
│   └── app.yaml
│
└── scripts/
    ├── init-airflow-db.sh      # Инициализация БД Airflow
    └── reimport_with_units.py  # Переимпорт данных с единицами измерения
```

---

## Airflow

DAG `goszakup_daily_sync` запускается ежедневно в 06:00 UTC (12:00 Астана):

1. **incremental_sync** — дозагрузка изменённых записей из API
2. **run_etl** — очистка и обогащение
3. **refresh_views** — обновление materialized views

Airflow UI: `http://localhost:8081` (admin / admin)

---

## Kubernetes

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/app.yaml
```
