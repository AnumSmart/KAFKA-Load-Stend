# Workflow: старт → метрики → остановка

Пошаговая инструкция для первого запуска стенда.

Рабочая директория всех команд — **корень репозитория** `Kafka_stend_go`
(там, где лежат `Makefile`, `go.work`, `deployments/`).

---

## 0. Что ты запускаешь (коротко)

| Компонент            | Зачем                                           |
| -------------------- | ----------------------------------------------- |
| 3 брокера Kafka      | Хранят топик `load-test` (6 партиций)           |
| 3 консьюмера         | Одна группа `load-test-group`, читают сообщения |
| 1 продюсер           | Шлёт ~100 000 сообщений и выходит               |
| Prometheus + Grafana | Собирают и показывают метрики                   |
| Kafka UI             | Смотреть топик/группу глазами                   |

Продюсер после отправки **не крутится вечно**: отработал → подержал
`/metrics` ~20 секунд → контейнер остановился. Консьюмеры продолжают жить.

---

## 1. Подготовка (один раз)

### Шаг 1.1. Убедись, что Docker запущен

1. Открой **Docker Desktop**.
2. Дождись статуса вроде _Engine running_ / зелёный индикатор.
3. В терминале проверь:

```powershell
docker version
docker compose version
```

Ожидание: обе команды печатают версии **без ошибок**.  
Нужен Compose **v2** (`docker compose`, не старый `docker-compose`).

### Шаг 1.2. Перейди в корень проекта

```powershell
cd D:\Different_stuff\GoLang\Projects\KAFKA_STEND\Kafka_stend_go
```

Проверь, что ты «на месте»:

```powershell
dir Makefile
dir deployments\docker-compose.yml
```

Оба файла должны существовать.

### Шаг 1.3. Make (желательно)

Если есть Make:

```powershell
make help
```

Если Make нет — ниже для каждого шага дан эквивалент через `docker compose`.

> На Windows Make часто ставят через Chocolatey (`choco install make`)
> или используют Git Bash / WSL. Без Make стенд всё равно поднимается.

---

## 2. Первый старт полного стенда

### Шаг 2.1. Поднять всё одной командой

С Make:

```powershell
make up
```

Без Make:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example up -d --build
```

**Что происходит (жди спокойно):**

1. Собираются Docker-образы продюсера и консьюмера (первый раз долго).
2. Стартуют брокеры `kafka1`, `kafka2`, `kafka3`.
3. Compose ждёт, пока брокеры станут `healthy`.
4. `kafka-init` создаёт топик `load-test` и завершается.
5. Стартуют `consumer-1`, `consumer-2`, `consumer-3`.
6. Продюсер **спит 12 секунд** (чтобы группа успела присоединиться),
   потом шлёт сообщения.
7. Поднимаются Kafka UI, Prometheus, Grafana, экспортёры.

Первый запуск может занять **несколько минут** (скачивание образов).

### Шаг 2.2. Проверить статус контейнеров

```powershell
make status
```

или:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example ps
```

**Как читать вывод:**

| Сервис                              | Нормально                                                      |
| ----------------------------------- | -------------------------------------------------------------- |
| `kafka1` / `kafka2` / `kafka3`      | `Up` / `healthy`                                               |
| `consumer-1` / `2` / `3`            | `Up`                                                           |
| `prometheus`, `grafana`, `kafka-ui` | `Up`                                                           |
| `producer`                          | Часто уже `Exited (0)` — это **нормально** (прогон закончился) |
| `kafka-init`                        | `Exited (0)` — тоже нормально                                  |

Если брокеры не `healthy` — подожди 30–60 секунд и снова `make status`.

### Шаг 2.3. Проверить топик

```powershell
make topic-describe
```

или:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic load-test
```

**Ожидай в выводе:**

- имя топика `load-test`
- `PartitionCount: 6`
- `ReplicationFactor: 3`

Если команды нет / ошибка «No such container» — стенд ещё не поднялся, вернись к шагу 2.1–2.2.

### Шаг 2.4. Посмотреть логи нагрузки (опционально)

```powershell
make logs
```

или:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example logs -f producer consumer-1 consumer-2 consumer-3
```

Что искать:

- у продюсера: `produce run finished`, `holding metrics endpoint`
- у консьюмеров: `batch committed`, растущий `total`

Выход из follow: `Ctrl+C` (контейнеры **не** останавливаются).

---

## 3. Повторный прогон нагрузки (без полного перезапуска)

Инфра и консьюмеры уже работают. Нужно только снова отправить сообщения.

### Шаг 3.1. Стандартный прогон (100 000 сообщений)

```powershell
make producer
```

или:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example run --rm --service-ports producer
```

Команда **блокирует терминал**, пока продюсер шлёт данные и держит
метрики (~20 секунд после Flush). Дождись возврата приглашения shell.

### Шаг 3.2. Короткий отладочный прогон (10 000 сообщений)

```powershell
$env:MESSAGE_COUNT="10000"
make producer
```

### Шаг 3.3. Эксперимент (менять по одному параметру)

Пример — увеличить linger:

```powershell
$env:PRODUCER_LINGER="100ms"
$env:MESSAGE_COUNT="100000"
make producer
```

Другие частые переменные: `PRODUCER_BATCH_MAX_BYTES`, `PRODUCER_REQUIRED_ACKS`,
`PRODUCER_COMPRESSION`, `PRODUCER_DISABLE_IDEMPOTENCY`, `PRODUCER_METRICS_HOLD`.
Подробнее — таблица в `README.md`.

---

## 4. Просмотр метрик (по шагам)

Делай **после** того, как стенд поднялся (раздел 2) и лучше — пока или
сразу после прогона продюсера (раздел 3).

### Шаг 4.1. Prometheus: все ли targets зелёные?

1. Открой в браузере: http://localhost:9090/targets
2. Найди jobs:
   - `jmx`
   - `kafka-exporter`
   - `producer`
   - `consumer`
   - `prometheus`
3. Статус должен быть **UP** (зелёный).

**Важно про producer:**

- Пока контейнер продюсера жив (отправка + hold ~20s) — target `producer` UP.
- После выхода контейнера target может стать **DOWN** — это ожидаемо.
- Консьюмеры должны оставаться UP постоянно.

Если `consumer` красный — проверь `make status` и порты 8082–8084.

### Шаг 4.2. Сырые метрики в браузере (проверка «жив ли эндпоинт»)

Открой по очереди:

| Кто                 | URL                           |
| ------------------- | ----------------------------- |
| Продюсер (пока жив) | http://localhost:8081/metrics |
| Консьюмер 1         | http://localhost:8082/metrics |
| Консьюмер 2         | http://localhost:8083/metrics |
| Консьюмер 3         | http://localhost:8084/metrics |
| Kafka Exporter      | http://localhost:9308/metrics |

На страницах консьюмера ищи строки вроде:

```text
consumer_messages_processed_total{group="load-test-group"} ...
consumer_batches_total{group="load-test-group"} ...
```

Эти счётчики растут **после успешного commit** оффсетов (не «на каждое
сырое сообщение до коммита»).

### Шаг 4.3. Простой запрос в Prometheus

1. Открой http://localhost:9090/graph
2. В поле Expression вставь, например:

```promql
sum(consumer_messages_processed_total)
```

3. Нажми **Execute**.
4. Должно появиться число (после прогона — порядка MESSAGE_COUNT, с учётом
   трёх реплик счётчики суммируются по instance).

Ещё полезные запросы:

```promql
sum(kafka_consumergroup_lag{consumergroup="load-test-group"})
```

```promql
rate(producer_messages_acked_total[15s])
```

(второй имеет смысл, пока продюсер ещё жив / только что закончил hold).

### Шаг 4.4. Grafana (основной способ смотреть нагрузку)

1. Открой http://localhost:3000
2. Логин / пароль: `admin` / `admin`  
   (при первом входе Grafana может попросить сменить пароль — можно Skip).
3. Слева: **Dashboards** (иконка квадратов) → папка **Kafka Stend**.
4. Открой дашборд **Kafka load-test**.
5. Справа сверху поставь refresh **5s** / **10s**, time range **Last 15 minutes**.

**Что смотреть на дашборде:**

| Панель (по смыслу)               | Норма после `make producer`                 |
| -------------------------------- | ------------------------------------------- |
| MessagesIn / throughput          | Всплеск на время прогона                    |
| Consumer lag (`load-test-group`) | Растёт во время отправки, потом падает к ~0 |
| Consumer processed / batch size  | Рост counters, гистограмма батчей           |
| Producer acked / errors          | Всплеск acked; errors ≈ 0                   |
| Targets health                   | Консьюмеры зелёные                          |

Анонимный просмотр (Viewer) тоже включён — можно зайти без логина, но
для спокойствия используй `admin`/`admin`.

### Шаг 4.5. Kafka UI (не метрики, а «что в Kafka»)

1. Открой http://localhost:8080
2. Кластер: `local`
3. Посмотри:
   - Topics → `load-test` (сообщения, партиции)
   - Consumers → группа `load-test-group` (members, lag, offsets)

Так проще понять, что три реплики делят 6 партиций.

---

## 5. Остановка

### Шаг 5.1. Обычная остановка (данные Kafka сохранить)

Контейнеры гасятся, **тома остаются** (топик и offsets на диске).

```powershell
make down
```

или:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example down
```

Проверка:

```powershell
make status
```

Список должен быть пустым / без running-сервисов стенда.

### Шаг 5.2. Полный сброс (удалить данные топика и offsets)

Нужно, когда хочешь «с чистого листа»:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example down -v
```

После этого следующий `make up` заново создаст топик через `kafka-init`.

### Шаг 5.3. Остановить только продюсер

Редко нужно (он и сам выходит), но если завис:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example stop producer
```

### Шаг 5.4. Остановить только инфру (консьюмеры оставить)

```powershell
make infra-down
```

Консьюмеры продолжат работать; Grafana/Prometheus уже не будут скрейпить
их из Docker-сети как раньше — используй осознанно.

---

## 6. Типичный день (чек-лист)

Скопируй и отмечай:

1. [ ] Docker Desktop запущен
2. [ ] `cd` в корень `Kafka_stend_go`
3. [ ] `make up`
4. [ ] `make status` — брокеры healthy, консьюмеры Up
5. [ ] `make topic-describe` — 6 партиций, RF=3
6. [ ] http://localhost:9090/targets — consumer UP
7. [ ] http://localhost:3000 → дашборд `Kafka load-test`
8. [ ] (по желанию) `make producer` — второй прогон, смотри графики
9. [ ] http://localhost:8080 — группа `load-test-group`
10. [ ] `make down` — ушёл домой / закончил работу

---

## 7. Если что-то пошло не так

### Порт занят (`bind: address already in use`)

Кто-то уже слушает 8080 / 3000 / 9090 / 8081–8084.  
Варианты: закрыть чужой процесс или сначала `make down`.

### `make up` падает / брокеры не healthy

1. Подожди минуту, `make status`.
2. Логи брокера:

```powershell
docker compose -f deployments/docker-compose.yml --env-file deployments/.env.example logs kafka1
```

3. Перезапуск с нуля: `down -v` (шаг 5.2) → снова `make up`.

### В Grafana пустые графики продюсера

Продюсер живёт коротко. Запусти `make producer` и **сразу** смотри дашборд
(или увеличь hold: `$env:PRODUCER_METRICS_HOLD="30s"`).

### Лаг не падает в ноль

- Консьюмеры не Up → `make status` / `make logs`.
- Смотри не ту группу → фильтр должен быть `load-test-group`.
- После смены группы offsets «с нуля» — нормально для новой group id.

### JMX красный, BytesIn пустые

На образе `apache/kafka:4.0.0` JMX бывает нестабилен.  
MessagesIn на дашборде должен уметь fallback через Kafka Exporter;
лаг группы от JMX **не зависит**.

### Команда `make` не найдена

Используй `docker compose ...` варианты из шагов выше — они равнозначны.

---

## 8. Куда смотреть дальше

| Файл                      | Содержание                                         |
| ------------------------- | -------------------------------------------------- |
| `start.md`                | Краткая шпаргалка по командам и эндпоинтам         |
| `README.md`               | Обзор стенда, env-переменные, лимиты CPU           |
| `docs/experiments.md`     | Таблица экспериментов (менять по одному параметру) |
| `observability_prompt.md` | Что уже сделано по метрикам                        |

Удачного прогона.
