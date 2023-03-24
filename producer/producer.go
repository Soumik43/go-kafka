package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

func producer() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "kafka-test", 0)
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	conn.WriteMessages(kafka.Message{
		Value: []byte("This is [golang<new>] message"),
	}, kafka.Message{
		Value: []byte("This is [golang1<new>] message"),
	})
	conn.Close()
}
