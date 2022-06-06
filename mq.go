package ticktick

type MQ interface {
	Publish(topic string, body []byte)
	Subscribe(topic string, h func(body []byte) error)
}
