package main

import (
	"context"
	"kafka-test/kafkaGo"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	reader := kafkaGo.NewKafkaReader()
	writer := kafkaGo.NewKafkaWriter()
	ctx := context.Background()

	messages := make(chan kafka.Message, 1000)
	messagesCommitChan := make(chan kafka.Message, 1000)

	g, ctx := errgroup.WithContext(ctx)

	go func() error {
		return reader.FetchMessage(ctx, messages)
	}()

	g.Go(func() error {
		return writer.WriteMessage(ctx, messages, messagesCommitChan)
	})

	g.Go(func() error {
		return reader.CommitMessage(ctx, messagesCommitChan)
	})
}
