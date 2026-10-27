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
        infra load infra-down

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
	$(COMPOSE_CMD) run --rm --service-ports producer
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
	$(COMPOSE_CMD) run --rm --service-ports producer

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
	@echo "  make logs            - Follow producer and consumer logs"
	@echo "  make producer-local  - Run producer on the host"
	@echo "  make consumer1-local - Run consumer replica #1 (group $(CONSUMER_GROUP)) on the host"
	@echo "  make consumer2-local - Run consumer replica #2 (same group) on the host"
	@echo "  make consumer3-local - Run consumer replica #3 (same group) on the host"
	@echo "  Local consumer metrics ports: $(CONSUMER1_METRICS_ADDR), $(CONSUMER2_METRICS_ADDR), $(CONSUMER3_METRICS_ADDR)"
	@echo "  make lint            - Run linter"
	@echo "  make test            - Run pkg tests"