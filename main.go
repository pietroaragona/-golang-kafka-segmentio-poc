package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

const (
	topic     = "foo"
	partition = 0
)

func main() {
	fmt.Println("Hello from golang")

	produce()
	consume()

}

func produce() {
	fmt.Println("START Produce")

	conn, err := kafka.DialLeader(context.Background(), "tcp", "kafka-svc:9092", topic, partition)
	if err != nil {
		fmt.Println("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		fmt.Println("failed to write messages:", err)
	}
	if err := conn.Close(); err != nil {
		fmt.Println("failed to close writer:", err)
	}
	fmt.Println("Conn Close")

}

func consume() {
	fmt.Println("START Consume")

	conn, err := kafka.DialLeader(context.Background(), "tcp", "kafka-svc:9092", topic, partition)
	if err != nil {
		fmt.Println("failed to dial leader:", err)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max
	//b := make([]byte, 10e3)            // 10KB max per message
	for {
		b := make([]byte, 10e3)
		_, err := batch.Read(b)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(string(b))
	}

	if err := batch.Close(); err != nil {
		fmt.Println("failed to close batch:", err)
	}

}
