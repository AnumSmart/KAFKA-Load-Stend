package franzgoproducer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/plugin/kprom"
)

var (
	producedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "producer_messages_produced_total",
		Help: "Число записей, переданных в Produce",
	})
	ackedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "producer_messages_acked_total",
		Help: "Число подтверждённых отправок",
	})
	errorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "producer_errors_total",
		Help: "Число ошибок отправки",
	})
)

// NewKpromMetrics создаёт плагин kprom с базовыми счётчиками и гистограммами
// задержек. Namespace "kgo" даёт имена вида kgo_produce_bytes_total и т.п.
// Использует prometheus.DefaultRegisterer — тот же реестр, что и у
// producer_*-метрик и у стандартного /metrics-handler'а.
func NewKpromMetrics() *kprom.Metrics {
	return kprom.NewMetrics("kgo",
		kprom.Registerer(prometheus.DefaultRegisterer),
		kprom.Histograms(
			kprom.WriteWait,
			kprom.WriteTime,
			kprom.RequestDurationE2E,
			kprom.RequestThrottled,
		),
	)
}
