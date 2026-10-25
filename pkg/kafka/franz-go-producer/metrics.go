package franzgoproducer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
