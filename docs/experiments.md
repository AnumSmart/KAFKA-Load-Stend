# Таблица экспериментов

Каждый прогон меняет один параметр. Остальные значения — baseline из `apps/kafka-producer/configs-yml/producer.yml`.

Baseline:

- linger 50 ms
- batch 1 MB
- compression snappy
- required_acks leader
- disable_idempotency true
- 6 партиций, RF=3
- 100 000 сообщений

Сначала один раз поднять инфру, затем гонять прогоны отдельными целями.
Консьюмеры цель поднимает сама. Меняется один параметр, остальные — baseline.

```powershell
make infra
make exp-baseline
make exp-linger-100
```

`make exp-partitions-12` увеличивает число партиций топика `load-test` до 12.
Уменьшить его нельзя: следующие прогоны на этих томах уже идут с 12 партициями,
пока тома не удалены.

| №   | Команда                  | Изменение                                             | MessagesInPerSec | p99 latency | consumer lag | Замечания                                   |
| --- | ------------------------ | ----------------------------------------------------- | ---------------- | ----------- | ------------ | ------------------------------------------- |
| 1   | `make exp-baseline`      | Baseline                                              |                  |             |              |                                             |
| 2   | `make exp-linger-100`    | linger 50 → 100 ms                                    |                  |             |              |                                             |
| 3   | `make exp-linger-200`    | linger 50 → 200 ms                                    |                  |             |              |                                             |
| 4   | `make exp-batch-2mb`     | batch 1 → 2 MB                                        |                  |             |              |                                             |
| 5   | `make exp-acks-all`      | required_acks leader → all                            |                  |             |              | baseline уже `leader`                       |
| 6   | `make exp-idempotent`    | disable_idempotency true → false                      |                  |             |              | baseline уже с выключенной идемпотентностью |
| 7   | `make exp-partitions-12` | партиции 6 → 12                                       |                  |             |              | необратимо до удаления томов                |
| 8   | `make exp-messages-700k` | сообщений 100 000 → 700 000                           |                  |             |              | размер value как в baseline                 |
| 9   | `make exp-message-30kb`  | value 30 КБ (30720 байт) до сжатия, 100 000 сообщений |                  |             |              | тело псевдослучайное                        |

Throughput и latency в обратной зависимости: крупнее батчи — выше скорость, выше задержка в буфере.
