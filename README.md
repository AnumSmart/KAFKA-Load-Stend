# Kafka_stend_go

Локальный нагрузочный стенд Apache Kafka 4.x (KRaft) + franz-go.

Пошаговый старт, остановка и список эндпоинтов/метрик: [start.md](start.md).

Продюсер асинхронно отправляет до 10 000 000 сообщений, три независимые consumer group читают топик `load-test`, метрики снимаются в Prometheus и Grafana.

Стенд не для production: без TLS/SASL/ACL. Настройки `DisableIdempotency=true` и `RequiredAcks=leader` ускоряют прогон и **не** являются production-семантикой.

## Требования

- Go 1.25+
- Docker Desktop / Docker Compose
- Make (опционально)

## Состав

- 3 брокера Kafka (`localhost:9092`, `:9094`, `:9096`)
- топик `load-test`: 6 партиций, replication factor 3
- продюсер на franz-go (async `Produce` + `Flush`), метрики на `:8081`
- один бинарь консьюмера, три группы: `group-1`, `group-2`, `group-3`
- JMX Exporter `:7071-7073` (`bitnamilegacy/jmx-exporter`), Kafka Exporter `:9308`, Prometheus `:9090`, Grafana `:3000`, Kafka UI `:8080`

Если JMX-таргеты в Prometheus красные, график MessagesInPerSec берёт запасной ряд `rate(kafka_topic_partition_current_offset)` из Kafka Exporter. Лаг групп при этом всё равно виден.

## Порядок запуска

Продюсер и три консьюмера поднимаются в Docker вместе с брокерами.

```bash
cd Kafka_stend_go
make up
make status
make topic-describe
make logs
```

`make up` собирает образы, ждёт healthy у брокеров, создаёт топик, стартует `group-1/2/3` и затем продюсер. Продюсер ждёт 8 секунд, чтобы группы успели стать Stable.

Повторить только отправку:

```bash
make producer
```

Короткий прогон:

```bash
MESSAGE_COUNT=10000 make producer
```

Или через `.env.example` / переменные compose.

Локальный запуск без контейнеров приложений: `make producer-local`, `make consumer1-local`.

Открыть Grafana: http://localhost:3000 (admin/admin).

Дашборд `Kafka load-test` содержит:

- `MessagesInPerSec`
- `BytesInPerSec` / `BytesOutPerSec`
- `kafka_consumergroup_lag` по `group-1/2/3`
- скорость подтверждений продюсера

При целевых 100 000 msg/sec отправка 10 000 000 сообщений занимает около 100 секунд. Фактическое время зависит от батчинга, диска и машины.

## Конфигурация экспериментов

Меняйте **один** параметр за прогон. Остальное фиксируйте.

Через env (удобнее, чем править YAML):

| Переменная                     | Пример             | Смысл                                |
| ------------------------------ | ------------------ | ------------------------------------ |
| `PRODUCER_LINGER`              | `100ms`            | накопление батча                     |
| `PRODUCER_BATCH_MAX_BYTES`     | `2097152`          | размер батча, 2 MB                   |
| `PRODUCER_REQUIRED_ACKS`       | `all` или `leader` | надёжность vs throughput             |
| `PRODUCER_DISABLE_IDEMPOTENCY` | `true`             | снять лимит идемпотентного продюсера |
| `MESSAGE_COUNT`                | `100000`           | число сообщений                      |
| `CONSUMER_GROUP_ID`            | `group-2`          | имя группы                           |

Таблица прогонов: [docs/experiments.md](docs/experiments.md).

Число партиций меняется пересозданием топика (`TOPIC_PARTITIONS` в `deployments/.env.example` + `make down` / `make up` с чистыми volume при необходимости).

## Полезные адреса

- Kafka UI: http://localhost:8080
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Метрики продюсера: http://localhost:8081/metrics
- Kafka Exporter: http://localhost:9308/metrics

## Остановка

```bash
make down
```
