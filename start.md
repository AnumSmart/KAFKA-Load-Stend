# Старт, остановка и точки входа стенда

Рабочая директория всех команд — корень `Kafka_stend_go`.

Нужны Docker Desktop (движок должен быть запущен) и Make. Без Make те же шаги повторяются через `docker compose` ниже.

Веб-интерфейсы: Kafka UI (топики, группы, сообщения) и Grafana (метрики нагрузки).

---

## 1. Старт

### Полный стенд одной командой

```bash
make up
```

Эквивалент без Make:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example up -d --build
```

Что происходит по порядку:

1. Собираются образы продюсера и консьюмера.
2. Поднимаются три брокера Kafka (`kafka1`, `kafka2`, `kafka3`) в KRaft.
3. Команда ждёт `healthy` у всех брокеров.
4. `kafka-init` создаёт топик `load-test` (6 партиций, RF=3) и завершается.
5. Стартуют три реплики консьюмера в одной группе `load-test-group`.
6. Продюсер ждёт 12 секунд (группа успевает стать Stable) и начинает отправку.
7. Рядом поднимаются Kafka UI, JMX Exporter, Kafka Exporter, Prometheus, Grafana.

Первый запуск долгий: качаются образы Kafka/Go и собираются приложения.

### Проверка, что всё поднялось

```bash
make status
make topic-describe
```

Эквивалент:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example ps
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic load-test
```

Ожидаемый результат `topic-describe`: топик `load-test`, `PartitionCount: 6`, `ReplicationFactor: 3`.

Логи продюсера и консьюмеров:

```bash
make logs
```

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example logs -f producer consumer-1 consumer-2 consumer-3
```

### Повторить только нагрузку

Консьюмеры уже крутятся. Новый прогон продюсера:

```bash
make producer
```

Короткий отладочный прогон (не 100k сообщений по умолчанию):

```bash
MESSAGE_COUNT=10000 make producer
```

```powershell
$env:MESSAGE_COUNT="10000"
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example run --rm --service-ports producer
```

Параметры одного эксперимента (менять по одному):

```powershell
$env:PRODUCER_LINGER="100ms"
$env:PRODUCER_BATCH_MAX_BYTES="2097152"
$env:PRODUCER_REQUIRED_ACKS="all"
$env:PRODUCER_DISABLE_IDEMPOTENCY="true"
$env:MESSAGE_COUNT="100000"
make producer
```

Или те же ключи в `deployments/.env.example`.

### Локальный запуск приложений (без контейнеров Go)

Брокеры всё равно нужны в Docker (`make up` или хотя бы брокеры + init).

```bash
make consumer1-local
make consumer2-local
make consumer3-local
make producer-local
```

С хоста брокеры: `localhost:9092`, `localhost:9094`, `localhost:9096`.

---

## 2. Остановка

Остановить контейнеры, сеть оставить volumes (данные Kafka сохранятся):

```bash
make down
```

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example down
```

Полная очистка вместе с данными топика:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example down -v
```

После `down -v` следующий `make up` создаст топик заново.

Остановить только продюсер (консьюмеры продолжают читать):

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example stop producer
```

---

## 3. Веб-интерфейсы и HTTP-эндпоинты

### Kafka UI

|         |                                                                                   |
| ------- | --------------------------------------------------------------------------------- |
| URL     | http://localhost:8080                                                             |
| Кластер | `local` (три брокера по INTERNAL: `kafka1:19092`, `kafka2:19092`, `kafka3:19092`) |
| Логин   | не требуется                                                                      |

В UI видны топик `load-test`, партиции, consumer group `load-test-group` и сообщения.

### Grafana

|         |                                         |
| ------- | --------------------------------------- |
| URL     | http://localhost:3000                   |
| Логин   | `admin` / `admin`                       |
| Дашборд | папка `Kafka Stend` → `Kafka load-test` |
| Health  | http://localhost:3000/api/health        |

Анонимный просмотр включён (роль Viewer). Обновление графиков — каждые 5 секунд.

### Prometheus

|         |                               |
| ------- | ----------------------------- |
| UI      | http://localhost:9090         |
| Targets | http://localhost:9090/targets |
| Query   | http://localhost:9090/graph   |

Scrape каждые 5 секунд: три JMX Exporter, Kafka Exporter, продюсер, три консьюмера, сам Prometheus.

### Kafka Exporter

|         |                               |
| ------- | ----------------------------- |
| Метрики | http://localhost:9308/metrics |

Лаг consumer group и офсеты партиций.

### Метрики продюсера

|         |                               |
| ------- | ----------------------------- |
| Метрики | http://localhost:8081/metrics |

Порт открыт, пока жив контейнер `kafka-producer`. После завершения прогона контейнер останавливается (`restart: no`), эндпоинт пропадает до следующего `make producer`. Рядом с `producer_*` появляются `kgo_*` (плагин kprom).

### Метрики консьюмера

| Реплика     | Метрики                         |
| ----------- | ------------------------------- |
| consumer-1  | http://localhost:8082/metrics   |
| consumer-2  | http://localhost:8083/metrics   |
| consumer-3  | http://localhost:8084/metrics   |

Приложение отдаёт `consumer_messages_processed_total`, `consumer_batches_total`, `consumer_batch_size` (label `group` из `cfg.Consumer.GroupID`). Локальные `make consumerN-local` слушают те же порты на хосте.

### JMX Exporter (по брокерам)

| Брокер | Метрики                       |
| ------ | ----------------------------- |
| kafka1 | http://localhost:7071/metrics |
| kafka2 | http://localhost:7072/metrics |
| kafka3 | http://localhost:7073/metrics |

Удалённый JMX на образе `apache/kafka:4.0.0` может не отвечать. Тогда графики BytesIn/BytesOut пустые, а MessagesInPerSec берётся из Kafka Exporter.

### Брокеры Kafka (клиентские порты с хоста)

| Брокер | Адрес с хоста    | Адрес внутри Docker |
| ------ | ---------------- | ------------------- |
| kafka1 | `localhost:9092` | `kafka1:19092`      |
| kafka2 | `localhost:9094` | `kafka2:19092`      |
| kafka3 | `localhost:9096` | `kafka3:19092`      |

Топик: `load-test`. Группа: `load-test-group` (три реплики делят 6 партиций).

---

## 4. Метрики, которые сейчас сняты

### Продюсер (`job=producer`)

| Имя                                | Тип     | Смысл                                       |
| ---------------------------------- | ------- | ------------------------------------------- |
| `producer_messages_produced_total` | counter | Сколько записей ушло в `Produce`            |
| `producer_messages_acked_total`    | counter | Сколько брокер подтвердил (в ТЗ это «sent») |
| `producer_errors_total`            | counter | Ошибки отправки                             |
| `kgo_*`                            | mixed   | Внутренние метрики franz-go через kprom     |

На дашборде: `rate(producer_messages_acked_total[15s])`, `rate(producer_errors_total[15s])`, produced/acked/in-flight, latency p99.

### Консьюмер (`job=consumer`)

| Имя                                | Тип       | Смысл                                      |
| ---------------------------------- | --------- | ------------------------------------------ |
| `consumer_messages_processed_total`| counter   | Успешно обработанные сообщения             |
| `consumer_batches_total`           | counter   | Успешно обработанные батчи                 |
| `consumer_batch_size`              | histogram | Размер батча                               |

Label `group` берётся из конфига (`load-test-group` по умолчанию), реплики различаются по `instance`.

### Kafka Exporter (`job=kafka-exporter`)

Используются на дашборде:

| Имя                                    | Смысл                                                                                      |
| -------------------------------------- | ------------------------------------------------------------------------------------------ |
| `kafka_consumergroup_lag`              | Лаг группы на топик/партицию. На графике сумма по `consumergroup="load-test-group"`        |
| `kafka_topic_partition_current_offset` | Текущий офсет партиции. Запасной MessagesInPerSec: `sum(rate(...{topic="load-test"}[1m]))` |

Экспортер отдаёт и другие ряды (члены группы, лидеры партиций и т.д.) — они снимаются, но на дашборд не выведены.

### JMX Exporter (`job=jmx`, лейбл `broker`)

| Имя                                                              | Смысл                                          |
| ---------------------------------------------------------------- | ---------------------------------------------- |
| `kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate` | MessagesInPerSec, one-minute rate              |
| `kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate`    | BytesInPerSec                                  |
| `kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate`   | BytesOutPerSec                                 |
| `kafka_network_requestmetrics_requestqueuetimems_mean`           | среднее `RequestQueueTimeMs` (лейбл `request`) |
| `kafka_network_requestmetrics_localtimems_mean`                  | среднее `LocalTimeMs` (лейбл `request`)        |

### Что видно на дашборде Grafana `Kafka load-test`

1. **MessagesInPerSec** — JMX или, если его нет, скорость роста офсетов `load-test`.
2. **BytesInPerSec / BytesOutPerSec** — только JMX.
3. **kafka_consumergroup_lag** — лаг группы `load-test-group` (Kafka Exporter).
4. **Producer acked / errors** — скорость подтверждений и ошибок продюсера.
5. **Targets health**, produced/acked/in-flight, latency p99, consumer processing rate / batch size, end-to-end produced → broker → consumer.

Лаг группы — из Kafka Exporter; `consumer_*` — из `/metrics` каждой реплики консьюмера.
