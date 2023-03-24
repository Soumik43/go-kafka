package kafkaGo

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type Reader struct {
	Reader *kafka.Reader
}

func NewKafkaReader() *Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "topic_1",
		GroupID: "groupz",
	})

	return &Reader{Reader: reader}
}

func (k *Reader) FetchMessage(ctx context.Context, messages chan<- kafka.Message) error {
	for {
		message, err := k.Reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case messages <- message:
			fmt.Println("This message has been fetched successfully", string(message.Value))
		}
	}
}

func (k *Reader) CommitMessage(ctx context.Context, messagesCommitChan <-chan kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
		case msg := <-messagesCommitChan:
			err := k.Reader.CommitMessages(ctx, msg)
			if err != nil {
				return err
			}
			log.Printf("committed an msg: %v \n", string(msg.Value))
		}
	}
}
