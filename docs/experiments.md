# Таблица экспериментов

Каждый прогон меняет один параметр. Остальные значения — baseline из `apps/kafka-producer/configs-yml/producer.yml`.

Baseline:

- linger 50 ms
- batch 1 MB
- compression snappy
- required_acks leader
- disable_idempotency true
- 6 партиций, RF=3
- 10 000 000 сообщений

Как запускать вариант:

```powershell
cd apps/kafka-producer
$env:PRODUCER_LINGER="100ms"; go run ./cmd
```

| № | Изменение | MessagesInPerSec | p99 latency | consumer lag | Замечания |
| --- | --- | --- | --- | --- | --- |
| 1 | Baseline (настройки по умолчанию) |  |  |  |  |
| 2 | ProducerLinger 50 → 100 ms |  |  |  |  |
| 3 | ProducerLinger 100 → 200 ms |  |  |  |  |
| 4 | ProducerBatchMaxBytes 1 → 2 MB |  |  |  |  |
| 5 | RequiredAcks all → leader |  |  |  |  |
| 6 | DisableIdempotency true |  |  |  |  |
| 7 | Партиции 6 → 12 |  |  |  |  |

Throughput и latency в обратной зависимости: крупнее батчи — выше скорость, выше задержка в буфере.
