package kafkaGo

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Writer struct {
	Writer *kafka.Writer
}

func NewKafkaWriter() *Writer {
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "topic_2",
	}

	return &Writer{
		Writer: writer,
	}
}

func (k *Writer) WriteMessage(ctx context.Context, message <-chan kafka.Message, messageChan chan kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-message:
			err := k.Writer.WriteMessages(ctx, kafka.Message{
				Value: m.Value,
			})
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
			case messageChan <- m:
			}
		}
	}
}
