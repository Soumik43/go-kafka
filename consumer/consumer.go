package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func consumer() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "kafka-test", 0)
	conn.SetReadDeadline(time.Now().Add(time.Second * 10))
	batch := conn.ReadBatch(1e3, 1e9)
	bytes := make([]byte, 1e3)
	for {
		_, err := batch.Read(bytes)
		if err != nil {
			break
		}
		fmt.Println(string(bytes))
	}
	conn.Close()
}
