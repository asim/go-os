package broker

import (
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-platform/log"
)

type BrokerOutput struct {
	broker broker.Broker
	topic  string
	opts   []broker.PublishOption
}

func New(b broker.Broker, topic string, opts ...broker.PublishOption) *BrokerOutput {
	return &BrokerOutput{broker: b, topic: topic, opts: opts}
}

func (out *BrokerOutput) Send(ev *log.Event) error {
	msg, err := ev.MarshalJSON()
	if err != nil {
		return err
	}

	return broker.Publish(out.topic, &broker.Message{
		Body: msg,
	}, out.opts...)
}

func (out *BrokerOutput) Flush() error {
	return nil
}

func (out *BrokerOutput) Close() error {
	return out.broker.Disconnect()
}

func (out *BrokerOutput) String() string {
	return "broker"
}
