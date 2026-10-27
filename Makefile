DOCKER_COMPOSE = docker compose
COMPOSE_FILE = deployments/docker-compose.yml
COMPOSE_CMD = $(DOCKER_COMPOSE) -f $(COMPOSE_FILE) --env-file deployments/.env.example

# Инфраструктура: брокеры + создание топика + мониторинг + UI.
# Продюсер и консьюмеры сюда НЕ входят.
INFRA_SERVICES = kafka1 kafka2 kafka3 kafka-init \
                 jmx-exporter-1 jmx-exporter-2 jmx-exporter-3 \
                 kafka-exporter prometheus grafana kafka-ui

CONSUMER_SERVICES = consumer-1 consumer-2 consumer-3

# Все консьюмеры (и Docker-реплики, и локальные) — в ОДНОЙ группе.
# 6 партиций делятся между всеми активными членами группы.
CONSUMER_GROUP = load-test-group

# Порты /metrics консьюмеров на хосте. Совпадают с портами, которые
# docker-compose пробрасывает наружу, чтобы переключение между
# docker-репликами и локальными не ломало Prometheus/Grafana.
# ВАЖНО: три локальные реплики должны занимать РАЗНЫЕ порты,
# иначе вторая и третья упадут с "bind: address already in use".
CONSUMER1_METRICS_ADDR = :8082
CONSUMER2_METRICS_ADDR = :8083
CONSUMER3_METRICS_ADDR = :8084

GOLANGCI_LINT = golangci-lint
LINT_TIMEOUT = 5m
LINT_CONFIG = .golangci.yml

.PHONY: up down status topic-describe producer consumers logs lint test help \
        producer-local consumer1-local consumer2-local consumer3-local \
        infra load infra-down run-experiment \
        exp-baseline exp-linger-100 exp-linger-200 exp-batch-2mb \
        exp-acks-all exp-idempotent exp-partitions-12 \
        exp-messages-700k exp-message-30kb

# Baseline прогона — те же значения, что в apps/kafka-producer/configs-yml/producer.yml.
# Каждая цель exp-* передаёт полный набор, чтобы переменные из оболочки
# не смешали два соседних эксперимента.
EXP_MESSAGE_COUNT = 100000
EXP_MESSAGE_COUNT_700K = 700000
# 30 КБ = 30 * 1024 байт несжатого value. В остальных прогонах message_bytes = 0.
EXP_MESSAGE_30KB = 30720
EXP_LINGER = 50ms
EXP_BATCH_MAX_BYTES = 1048576
EXP_BATCH_2MB = 2097152
EXP_COMPRESSION = snappy
EXP_REQUIRED_ACKS = leader
EXP_DISABLE_IDEMPOTENCY = true
EXP_PARTITIONS = 12

up:
	@echo "[INFO] Starting Kafka stand (brokers, monitoring, consumers, producer)..."
	$(COMPOSE_CMD) up -d --build
	@echo "[OK] Stand started"
	@$(MAKE) --no-print-directory status

# Только инфраструктура: брокеры, топик, экспортеры, Prometheus, Grafana, Kafka UI.
# Без продюсера и консьюмеров — удобно для отладки UI/дашбордов.
infra:
	@echo "[INFO] Starting infrastructure only (brokers, monitoring, UI)..."
	$(COMPOSE_CMD) up -d --build $(INFRA_SERVICES)
	@echo "[OK] Infrastructure started"
	@$(MAKE) --no-print-directory status

# Поднять консьюмеры (если ещё не подняты) и прогнать один продюсер.
# Требует, чтобы инфраструктура уже была запущена (make infra).
load:
	@echo "[INFO] Starting consumers and running producer..."
	$(COMPOSE_CMD) up -d --build $(CONSUMER_SERVICES)
	$(COMPOSE_CMD) run --rm --service-ports --use-aliases producer
	@echo "[OK] Load run finished"

down:
	@echo "[INFO] Stopping Kafka stand..."
	$(COMPOSE_CMD) down
	@echo "[OK] Stand stopped"

# Остановить только инфраструктуру (консьюмеры и продюсер не трогаются).
# Если консьюмеры были запущены через make load — они останутся работать.
infra-down:
	@echo "[INFO] Stopping infrastructure only..."
	$(COMPOSE_CMD) stop $(INFRA_SERVICES)
	@echo "[OK] Infrastructure stopped"

status:
	@echo "[INFO] Container status:"
	$(COMPOSE_CMD) ps

topic-describe:
	$(COMPOSE_CMD) exec kafka1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka1:19092 \
		--describe --topic load-test

consumers:
	$(COMPOSE_CMD) up -d --build $(CONSUMER_SERVICES)

producer:
	$(COMPOSE_CMD) run --rm --service-ports --use-aliases producer

# Внутренняя цель. Снаружи используйте make exp-*.
# Требует уже запущенную инфру (make infra). Консьюмеры поднимает сама.
# Аргументы приходят командной строкой под-make и подставляются в -e,
# поэтому перекрывают и YAML, и deployments/.env.example.
run-experiment:
	@echo "[INFO] linger=$(PRODUCER_LINGER) batch=$(PRODUCER_BATCH_MAX_BYTES) compression=$(PRODUCER_COMPRESSION) acks=$(PRODUCER_REQUIRED_ACKS) disable_idempotency=$(PRODUCER_DISABLE_IDEMPOTENCY) messages=$(MESSAGE_COUNT) message_bytes=$(PRODUCER_MESSAGE_BYTES)"
	$(COMPOSE_CMD) up -d --build $(CONSUMER_SERVICES)
	$(COMPOSE_CMD) run --rm --service-ports --use-aliases \
		-e MESSAGE_COUNT=$(MESSAGE_COUNT) \
		-e PRODUCER_LINGER=$(PRODUCER_LINGER) \
		-e PRODUCER_BATCH_MAX_BYTES=$(PRODUCER_BATCH_MAX_BYTES) \
		-e PRODUCER_COMPRESSION=$(PRODUCER_COMPRESSION) \
		-e PRODUCER_REQUIRED_ACKS=$(PRODUCER_REQUIRED_ACKS) \
		-e PRODUCER_DISABLE_IDEMPOTENCY=$(PRODUCER_DISABLE_IDEMPOTENCY) \
		-e PRODUCER_MESSAGE_BYTES=$(PRODUCER_MESSAGE_BYTES) \
		producer
	@echo "[OK] Experiment finished"

# $(call run_experiment,linger,batch,acks,disable_idempotency[,message_count[,message_bytes]])
define run_experiment
$(MAKE) --no-print-directory run-experiment \
	MESSAGE_COUNT=$(if $(5),$(5),$(EXP_MESSAGE_COUNT)) \
	PRODUCER_LINGER=$(1) \
	PRODUCER_BATCH_MAX_BYTES=$(2) \
	PRODUCER_COMPRESSION=$(EXP_COMPRESSION) \
	PRODUCER_REQUIRED_ACKS=$(3) \
	PRODUCER_DISABLE_IDEMPOTENCY=$(4) \
	PRODUCER_MESSAGE_BYTES=$(if $(6),$(6),0)
endef

exp-baseline:
	@echo "[INFO] Experiment 1: baseline"
	$(call run_experiment,$(EXP_LINGER),$(EXP_BATCH_MAX_BYTES),$(EXP_REQUIRED_ACKS),$(EXP_DISABLE_IDEMPOTENCY))

exp-linger-100:
	@echo "[INFO] Experiment 2: linger $(EXP_LINGER) -> 100ms"
	$(call run_experiment,100ms,$(EXP_BATCH_MAX_BYTES),$(EXP_REQUIRED_ACKS),$(EXP_DISABLE_IDEMPOTENCY))

exp-linger-200:
	@echo "[INFO] Experiment 3: linger $(EXP_LINGER) -> 200ms"
	$(call run_experiment,200ms,$(EXP_BATCH_MAX_BYTES),$(EXP_REQUIRED_ACKS),$(EXP_DISABLE_IDEMPOTENCY))

exp-batch-2mb:
	@echo "[INFO] Experiment 4: batch 1MB -> 2MB"
	$(call run_experiment,$(EXP_LINGER),$(EXP_BATCH_2MB),$(EXP_REQUIRED_ACKS),$(EXP_DISABLE_IDEMPOTENCY))

exp-acks-all:
	@echo "[INFO] Experiment 5: required_acks $(EXP_REQUIRED_ACKS) -> all"
	$(call run_experiment,$(EXP_LINGER),$(EXP_BATCH_MAX_BYTES),all,$(EXP_DISABLE_IDEMPOTENCY))

exp-idempotent:
	@echo "[INFO] Experiment 6: disable_idempotency $(EXP_DISABLE_IDEMPOTENCY) -> false"
	$(call run_experiment,$(EXP_LINGER),$(EXP_BATCH_MAX_BYTES),$(EXP_REQUIRED_ACKS),false)

# Число партиций меняется у уже созданного топика и обратно не уменьшается.
# Дальнейшие прогоны на этом томе идут уже с 12 партициями, пока не сбросить тома.
exp-partitions-12:
	@echo "[INFO] Experiment 7: partitions 6 -> $(EXP_PARTITIONS) (topic load-test, cannot be decreased later)"
	$(COMPOSE_CMD) exec kafka1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka1:19092 \
		--alter --topic load-test --partitions $(EXP_PARTITIONS)
	$(call run_experiment,$(EXP_LINGER),$(EXP_BATCH_MAX_BYTES),$(EXP_REQUIRED_ACKS),$(EXP_DISABLE_IDEMPOTENCY))
	@echo "[WARN] load-test now has $(EXP_PARTITIONS) partitions until volumes are removed"

exp-messages-700k:
	@echo "[INFO] Experiment 8: message_count $(EXP_MESSAGE_COUNT) -> $(EXP_MESSAGE_COUNT_700K)"
	$(call run_experiment,$(EXP_LINGER),$(EXP_BATCH_MAX_BYTES),$(EXP_REQUIRED_ACKS),$(EXP_DISABLE_IDEMPOTENCY),$(EXP_MESSAGE_COUNT_700K))

exp-message-30kb:
	@echo "[INFO] Experiment 9: message_bytes -> $(EXP_MESSAGE_30KB) (30 KiB uncompressed), count $(EXP_MESSAGE_COUNT)"
	$(call run_experiment,$(EXP_LINGER),$(EXP_BATCH_MAX_BYTES),$(EXP_REQUIRED_ACKS),$(EXP_DISABLE_IDEMPOTENCY),$(EXP_MESSAGE_COUNT),$(EXP_MESSAGE_30KB))

logs:
	$(COMPOSE_CMD) logs -f producer consumer-1 consumer-2 consumer-3

producer-local:
	cd apps/kafka-producer && \
		PRODUCER_METRICS_ADDR=:8081 \
		go run ./cmd

# --- Локальные консьюмеры в ТОЙ ЖЕ группе, что и docker-реплики. ---
# Каждый запуск добавляет ещё одного члена в группу $(CONSUMER_GROUP).
# Если параллельно работают docker-реплики — Kafka выполнит ребалансировку
# и поделит 6 партиций между всеми членами группы.
# Удобно запускать в трёх отдельных терминалах, чтобы получить 3 локальных
# реплики, либо несколько раз в одном терминале (каждый — отдельный процесс).
consumer1-local:
	cd apps/kafka-consumer && \
		CONSUMER_GROUP_ID=$(CONSUMER_GROUP) \
		CONSUMER_METRICS_ADDR=$(CONSUMER1_METRICS_ADDR) \
		go run ./cmd

consumer2-local:
	cd apps/kafka-consumer && \
		CONSUMER_GROUP_ID=$(CONSUMER_GROUP) \
		CONSUMER_METRICS_ADDR=$(CONSUMER2_METRICS_ADDR) \
		go run ./cmd

consumer3-local:
	cd apps/kafka-consumer && \
		CONSUMER_GROUP_ID=$(CONSUMER_GROUP) \
		CONSUMER_METRICS_ADDR=$(CONSUMER3_METRICS_ADDR) \
		go run ./cmd

lint:
	@echo "[INFO] Running linter..."
	$(GOLANGCI_LINT) run --config $(LINT_CONFIG) --timeout $(LINT_TIMEOUT) ./...
	@echo "[OK] Linter finished"

test:
	@echo "[INFO] Running tests..."
	go test -v -race -count=1 ./pkg/...
	@echo "[OK] Tests finished"

help:
	@echo "Available commands:"
	@echo "  make up              - Build and start the full stand (infra + consumers + producer)"
	@echo "  make infra           - Build and start infrastructure only (brokers, monitoring, UI)"
	@echo "  make load            - Start consumers and run one producer job (requires make infra)"
	@echo "  make down            - Stop the stand (containers removed, volumes kept)"
	@echo "  make infra-down      - Stop only infrastructure services"
	@echo "  make status          - Show container status"
	@echo "  make topic-describe  - Describe load-test topic"
	@echo "  make consumers       - Start three consumer containers"
	@echo "  make producer        - Run one produce job in Docker"
	@echo "  make exp-baseline     - Experiment 1: baseline (50ms, 1MB, snappy, acks=leader)"
	@echo "  make exp-linger-100   - Experiment 2: linger 100ms"
	@echo "  make exp-linger-200   - Experiment 3: linger 200ms"
	@echo "  make exp-batch-2mb    - Experiment 4: batch 2MB"
	@echo "  make exp-acks-all     - Experiment 5: required_acks=all"
	@echo "  make exp-idempotent   - Experiment 6: idempotent producer"
	@echo "  make exp-partitions-12 - Experiment 7: grow load-test to 12 partitions, then baseline produce"
	@echo "  make exp-messages-700k - Experiment 8: 700000 messages"
	@echo "  make exp-message-30kb - Experiment 9: 100000 messages, 30 KiB uncompressed each"
	@echo "  Experiments need make infra first. exp-partitions-12 is irreversible until volumes are removed."
	@echo "  make logs            - Follow producer and consumer logs"
	@echo "  make producer-local  - Run producer on the host"
	@echo "  make consumer1-local - Run consumer replica #1 (group $(CONSUMER_GROUP)) on the host"
	@echo "  make consumer2-local - Run consumer replica #2 (same group) on the host"
	@echo "  make consumer3-local - Run consumer replica #3 (same group) on the host"
	@echo "  Local consumer metrics ports: $(CONSUMER1_METRICS_ADDR), $(CONSUMER2_METRICS_ADDR), $(CONSUMER3_METRICS_ADDR)"
	@echo "  make lint            - Run linter"
	@echo "  make test            - Run pkg tests"