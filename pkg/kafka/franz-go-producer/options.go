package franzgoproducer

// ProducerOptions — настройки прогона, не связанные с kgo.Client.
type ProducerOptions struct {
	Topic           string
	MessageCount    int64
	MessageTemplate string
	ExpectedTotal   int64
}

func DefaultOptions() *ProducerOptions {
	return &ProducerOptions{
		Topic:           "load-test",
		MessageCount:    10_000_000,
		MessageTemplate: "привет - я сообщение #%d",
		ExpectedTotal:   10_000_000,
	}
}

func (o *ProducerOptions) Validate() error {
	if o.Topic == "" {
		o.Topic = "load-test"
	}
	if o.MessageCount <= 0 {
		o.MessageCount = 1
	}
	if o.MessageTemplate == "" {
		o.MessageTemplate = "привет - я сообщение #%d"
	}
	if o.ExpectedTotal <= 0 {
		o.ExpectedTotal = o.MessageCount
	}
	return nil
}
