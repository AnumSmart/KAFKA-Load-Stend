# Kafka_stend_go

Локальный нагрузочный стенд Apache Kafka 4.x (KRaft) + franz-go.

Пошаговый старт, остановка и список эндпоинтов/метрик: [start.md](start.md).

Продюсер асинхронно отправляет 100 000 сообщений, **одна consumer group `load-test-group`
(три реплики)** читает топик `load-test`, метрики снимаются в Prometheus и Grafana.

Стенд не для production: без TLS/SASL/ACL. Настройки `DisableIdempotency=true` и `RequiredAcks=leader` ускоряют прогон и **не** являются production-семантикой.

## Требования

- Go 1.25+
- Docker Desktop / Docker Compose **v2** (`docker compose`, не `docker-compose`)
- Make (опционально)

## Состав

- 3 брокера Kafka (`localhost:9092`, `:9094`, `:9096`)
- топик `load-test`: 6 партиций, replication factor 3
- продюсер на franz-go (async `Produce` + `Flush`), метрики на `:8081`
- один бинарь консьюмера, **три реплики в одной группе `load-test-group`**
  (6 партиций делятся между ними по 2 на контейнер)
- JMX Exporter `:7071-7073` (`bitnamilegacy/jmx-exporter`), Kafka Exporter `:9308`, Prometheus `:9090`, Grafana `:3000`, Kafka UI `:8080`

Если JMX-таргеты в Prometheus красные, график MessagesInPerSec берёт запасной ряд `rate(kafka_topic_partition_current_offset)` из Kafka Exporter.
Лаг группы при этом всё равно виден.

## Быстрый старт

### Полный прогон (инфра + консьюмеры + продюсер):

```bash
cd Kafka_stend_go
make up
make status
make topic-describe
```

make up собирает образы, поднимает брокеров, ждёт healthy, создаёт топик,
стартует три реплики load-test-group и сразу же запускает продюсер (он
отработает MESSAGE_COUNT сообщений и выйдет — restart: 'no'). Продюсер
ждёт 8 секунд, чтобы группа успела стать Stable.

Повторить только отправку (инфра и консьюмеры уже подняты):

```bash
make producer
```

### Короткий прогон:

```bash
MESSAGE_COUNT=10000 make producer
```

## Открыть Grafana: http://localhost:3000 (admin/admin).

Дашборд Kafka load-test содержит:

- **MessagesInPerSec** — из JMX Exporter (при красных JMX-таргетах дашборд
  откатывается на `rate(kafka_topic_partition_current_offset)` из Kafka Exporter).
- **BytesInPerSec / BytesOutPerSec** — из JMX Exporter.
- **kafka_consumergroup_lag** по группе `load-test-group` (общий лаг группы) —
  **из Kafka Exporter** (`:9308`). Лаг снимается с брокера, а не из кода
  приложения-консьюмера.
- **consumer_*** (`consumer_messages_processed_total`, `consumer_batches_total`,
  `consumer_batch_size`) — из `/metrics` каждой реплики консьюмера
  (`:8082`/`:8083`/`:8084`). Label `group` берётся из `cfg.Consumer.GroupID`.
- **Скорость подтверждений продюсера** и `kgo_*` — из `/metrics` продюсера (`:8081`).

При скорости ~100 000 msg/sec отправка 100 000 сообщений занимает около 1 секунды.
Фактическое время зависит от батчинга, диска и машины.

| Команда                | Что делает                                                          |
| ---------------------- | ------------------------------------------------------------------- |
| `make up`              | Собрать и запустить всё: инфра + 3 консьюмера + продюсер (100k msg) |
| `make infra`           | Только инфра: брокеры, топик, экспортеры, Prometheus, Grafana, UI   |
| `make consumers`       | Поднять (или перезапустить) только три реплики консьюмера           |
| `make producer`        | Прогнать один продюсер (`docker compose run --rm --service-ports`)  |
| `make load`            | Поднять консьюмеров + прогнать продюсера (требует `make infra`)     |
| `make status`          | `docker compose ps`                                                 |
| `make topic-describe`  | Описание топика `load-test`                                         |
| `make logs`            | Follow логов продюсера и трёх консьюмеров                           |
| `make down`            | Остановить стенд (тома сохраняются)                                 |
| `make infra-down`      | Остановить только инфру (консьюмеры продолжат работать)             |
| `make lint`            | `golangci-lint`                                                     |
| `make test`            | `go test -race ./pkg/...`                                           |
| `make producer-local`  | Продюсер на хосте (Go, без контейнера)                              |
| `make consumer1-local` | Локальный консьюмер (группа `load-test-group`)                      |
| `make consumer2-local` | Ещё одна локальная реплика той же группы                            |
| `make consumer3-local` | Ещё одна локальная реплика той же группы                            |

## Ресурсные лимиты (CPU)

Каждому сервису в deployments/docker-compose.yml заданы жёсткий потолок
(deploy.resources.limits.cpus) и относительный приоритет при конкуренции
(cpu_shares, дефолт Docker — 1024):

| Сервис              | cpus (потолок) | cpu_shares (вес) |
| ------------------- | -------------- | ---------------- |
| `kafka1..3`         | `2.0`          | `4096` (×4)      |
| `consumer-1..3`     | `1.0`          | `1024` (×1)      |
| `producer`          | `1.0`          | `1024` (×1)      |
| `jmx-exporter-1..3` | `0.3`          | `256` (×0.25)    |
| `kafka-exporter`    | `0.5`          | `512` (×0.5)     |
| `prometheus`        | `1.0`          | `512` (×0.5)     |
| `grafana`           | `0.5`          | `256` (×0.25)    |
| `kafka-ui`          | `0.5`          | `256` (×0.25)    |
| `kafka-init`        | `0.5`          | `128` (×0.125)   |

cpus — это потолок, а не резервация: в простое контейнер использует меньше.
При пике нагрузки cpu_shares гарантирует, что брокеры получат процессорное
время первыми, а мониторинг и UI уступят.

Проверить, что лимиты применились (Makefile использует deployments/docker-compose.yml):

```bash
docker inspect kafka1 --format '{{.HostConfig.NanoCpus}} {{.HostConfig.CpuShares}}'
# ожидается: 2000000000 4096
```

Проверить, что JVM видит выделенные ядра (а не 1 по умолчанию):

```bash
docker exec kafka1 java -XshowSettings:system -version 2>&1 | grep 'Effective CPU'
# ожидается: Effective CPU Count: 2
```

## Тюнинг брокеров

В deployments/docker-compose.yml для каждого брокера задано:

- KAFKA_HEAP_OPTS=-Xmx512m -Xms512m — компактный heap, меньше пауз GC
- KAFKA_NUM_IO_THREADS=4 — под 2 выделенных ядра (дефолт 8 на 1–2 ядрах
  даёт лишние переключения контекста и «лаги»)
- KAFKA_NUM_NETWORK_THREADS=2
- KAFKA_NUM_REPLICA_FETCHERS=2

При увеличении cpus до 4+ имеет смысл поднять NUM_IO_THREADS до cpus × 2,
NUM_NETWORK_THREADS до cpus + 1 и heap до 1–2 ГБ

## Конфигурация экспериментов

Меняйте один параметр за прогон. Остальное фиксируйте.

Через env (удобнее, чем править YAML). Значения по умолчанию берутся из
deployments/.env.example (Makefile передаёт --env-file deployments/.env.example);
переменные на команду (MESSAGE_COUNT=10000 make producer) переопределяют их:

| Переменная                     | Пример             | Смысл                                      |
| ------------------------------ | ------------------ | ------------------------------------------ |
| `PRODUCER_LINGER`              | `100ms`            | Накопление батча                           |
| `PRODUCER_BATCH_MAX_BYTES`     | `2097152`          | Размер батча, 2 MB                         |
| `PRODUCER_COMPRESSION`         | `snappy`           | Кодек: `none`/`gzip`/`snappy`/`lz4`/`zstd` |
| `PRODUCER_REQUIRED_ACKS`       | `all` или `leader` | Надёжность vs throughput                   |
| `PRODUCER_DISABLE_IDEMPOTENCY` | `true`             | Снять лимит идемпотентного продюсера       |
| `PRODUCER_METRICS_ADDR`          | `:8081`            | Адрес `/metrics` продюсера                 |
| `PRODUCER_METRICS_HOLD`          | `20s`              | Пауза после Flush под scrape Prometheus    |
| `PRODUCER_MAX_BUFFERED_RECORDS`  | `1000000`          | Лимит буфера franz-go                      |
| `CONSUMER_METRICS_ADDR`          | `:8082`            | Адрес `/metrics` консьюмера                |
| `CONSUMER_START_OFFSET`          | `earliest`         | Старт новой группы: `earliest`/`latest`    |
| `MESSAGE_COUNT`                  | `100000`           | Число сообщений                            |
| `TOPIC_NAME`                     | `load-test`        | Имя топика                                 |
| `TOPIC_PARTITIONS`               | `6`                | Число партиций (только при создании)       |
| `TOPIC_RF`                       | `3`                | Replication factor (только при создании)   |

CONSUMER_GROUP_ID зашит в deployments/docker-compose.yml как load-test-group
для всех трёх Docker-реплик; тот же load-test-group используют локальные
цели make consumerN-local. Это даёт корректное партиционирование: 6 партиций
делятся между членами группы (по 2 на реплику при трёх членах). Если поменять
имя группы у всех реплик — при следующем запуске появится новая группа и чтение
начнётся с начала топика (`CONSUMER_START_OFFSET=earliest`).
Если задать разные имена — получите N× амплификацию чтения (это отдельный
эксперимент, он искажает картину нормального потребления).

## Полезные адреса

- Kafka UI: http://localhost:8080
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Метрики продюсера: http://localhost:8081/metrics
- Метрики консьюмера: http://localhost:8082/metrics (consumer-1),
  http://localhost:8083/metrics (consumer-2),
  http://localhost:8084/metrics (consumer-3)
- Kafka Exporter: http://localhost:9308/metrics

## Остановка

```bash
make down
```

make down останавливает контейнеры, но тома сохраняются — offsets и
сообщения топика остаются на месте. При следующем make up консьюмеры
продолжат с сохранённых позиций группы; `earliest`/`latest` применяются
только если у группы ещё нет закоммиченного оффсета.

## Полный сброс (удаляет тома Kafka, offsets и топик):

```bash
make down
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example down -v
make up
```

## Остановить только инфру (консьюмеры продолжат работать):

```bash
make infra-down
```
