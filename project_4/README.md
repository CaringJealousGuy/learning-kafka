# Инструкции по запуску решения через Docker Compose.
Выполнить команду в корневой папке проекта
```
docker compose up -d
```

# Компоненты
PostgreSQL — база app_db, в ней находятся таблицы users и orders.
Debezium и Kafka Connect — читают изменения в этих таблицах и публикуют события в Kafka.
Kafka — хранит события в топиках.
JMX Exporter — делает метрики Kafka Connect доступными для Prometheus.
Prometheus — собирает метрики.
Grafana — показывает их на dashboard.

# Регистрация Debezium-коннектора
После запуска Kafka Connect проверьте, зарегистрирован ли коннектор:
```PowerShell
Invoke-RestMethod http://localhost:8083/connectors
Если postgres-connector в списке нет, зарегистрируйте его из каталога с файлом connector.json:
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -InFile ".\connector.json"
```
Проверить состояние коннектора:
```PowerShell
Invoke-RestMethod http://localhost:8083/connectors/postgres-connector/status
```
Коннектор и его задача должны иметь статус RUNNING.

# Настройки Debezium
Основные параметры находятся в connector.json:
- name: postgres-connector — имя коннектора.
- connector.class: io.debezium.connector.postgresql.PostgresConnector — коннектор для PostgreSQL.
- database.dbname: app_db — база-источник.
- topic.prefix: dbserver1 — префикс имён Kafka-топиков.
- plugin.name: pgoutput — плагин логической репликации PostgreSQL.
- slot.name: debezium_slot — replication slot.
- publication.name: dbz_publication — publication PostgreSQL.
- publication.autocreate.mode: filtered — создание publication с учётом фильтра таблиц.
- schema.include.list: public — схема, которую читает коннектор.
- table.include.list: public.users,public.orders — таблицы, за которыми следим.
- snapshot.mode: initial — при первом запуске коннектор делает первоначальный снимок данных.
Параметры подключения к PostgreSQL (адрес, порт, пользователь и пароль) также должны быть указаны в connector.json и соответствовать настройкам Docker Compose.
Для таблиц users и orders ожидаются топики:
dbserver1.public.users
dbserver1.public.orders

# Мониторинг
Prometheus
В prometheus/prometheus.yml настроен сбор метрик с Kafka Connect:
```yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "kafka-connect"
    metrics_path: /metrics
    static_configs:
      - targets: ["kafka-connect:9404"]
```
Проверьте, что JMX Exporter отвечает:
```PowerShell
(Invoke-WebRequest http://localhost:9404/metrics -UseBasicParsing).StatusCode
```
Ожидаемый код — 200. Затем откройте http://localhost:9090/targets. У цели kafka-connect должен быть статус UP.
Grafana
Откройте http://localhost:3000 и добавьте источник данных Prometheus с URL:
http://prometheus:9090
Нажмите Save & test. После успешной проверки можно создавать панели dashboard.

# Как проверить, что всё работает
1. Проверить PostgreSQL
Подключитесь к базе внутри контейнера:
docker compose exec postgres psql -U postgres -d app_db
В консоли PostgreSQL посмотрите таблицы и структуру users:
\dt
\d users
SELECT * FROM users;
Выйти можно командой \q.
2. Добавить запись
Если у таблицы users есть столбцы name и email, выполните в psql:
INSERT INTO users (name, email)
VALUES ('Test User', 'test@example.com');
Если столбцы отличаются, сначала проверьте структуру таблицы командой \d users и измените запрос.
3. Убедиться, что событие дошло до Kafka
Проверьте статус коннектора через REST API, как описано выше, и проверьте получение события в Kafka. В проекте для этого также можно запустить Python consumer, настроенный на внешний адрес Kafka localhost:9094.
4. Проверить метрики в Grafana
Для созданных панелей используются следующие запросы PromQL.
Доступность сбора метрик — ожидается 1:
up{job="kafka-connect"}
Количество запущенных задач — ожидается 1:
kafka_connect_connect_worker_metrics_connector_running_task_count{connector="postgres-connector"}
Количество упавших задач — ожидается 0:
kafka_connect_connect_worker_metrics_connector_failed_task_count{connector="postgres-connector"}
Скорость записи событий:
kafka_connect_source_task_metrics_source_record_write_rate{connector="postgres-connector",task="0"}
После вставки записи график может ненадолго вырасти. Когда новых событий нет, значение может быть нулевым.
Ошибочные события Debezium — ожидается 0:
debezium_postgres_connector_metrics_NumberOfErroneousEvents{context="streaming",server="dbserver1"}
Задержка относительно источника:
debezium_postgres_connector_metrics_MilliSecondsBehindSource{context="streaming",server="dbserver1"} >= 0
Фильтр >= 0 убирает значения -1, которые не показывают обычную неотрицательную задержку.
Если Grafana показывает No data, проверьте временной диапазон панели и убедитесь, что имя метрики совпадает с тем, что доступно в Prometheus.