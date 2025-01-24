package server_test

import (
	"log"
	"net"
	"testing"
	"time"

	"github.com/reqfleet/pubsub/client"
	"github.com/reqfleet/pubsub/messages"
	"github.com/reqfleet/pubsub/server"
)

func TestServer(t *testing.T) {
	server := server.NewPubSubServer(server.TCP)
	go server.Listen()

	client := &client.PubSubClient{
		Addr: "localhost:2416",
	}

	numberOfClients := 500
	numberOfMessages := 30

	cases := []struct {
		name             string
		expectedMessages int
		sendFunc         func(topic string, message []byte)
	}{
		{
			name:             "broadcast",
			expectedMessages: numberOfClients * numberOfMessages,
			sendFunc:         server.Broadcast,
		},
		{
			name:             "roundrobin",
			expectedMessages: numberOfMessages,
			sendFunc:         server.Roundrobin,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			time.Sleep(4 * time.Second)
			messageChans := make([]chan messages.Message, numberOfClients)
			conns := make([]net.Conn, numberOfClients)
			for i := 0; i < numberOfClients; i++ {
				c, conn, err := client.Subscribe("engine", &messages.EngineMessage{})
				if err != nil {
					t.FailNow()
				}
				messageChans[i] = c
				conns[i] = conn

			}
			timer := time.NewTimer(2 * time.Second)
			<-timer.C
		waitloop:
			for {
				select {
				case <-timer.C:
					t.Error("Clients are not ready")
					break waitloop
				default:
					if server.NumberOfClients("engine") == numberOfClients {
						break waitloop
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
			messageReceived := make(chan int)
			for _, c := range messageChans {
				go func(messageChan chan messages.Message) {
					for msg := range messageChan {
						if msg.String() == "" {
							continue
						}
						messageReceived <- 1
					}
				}(c)
			}
			for i := 0; i < numberOfMessages; i++ {
				m := &messages.EngineMessage{Verb: "Start"}
				message, _ := m.ToJSON()
				tc.sendFunc("engine", message)
			}
			k := 0
			for i := range messageReceived {
				k += i
				if k >= tc.expectedMessages {
					break
				}
			}
			if k != tc.expectedMessages {
				t.Errorf("Not equal from messages received %d vs message sent %d", k, tc.expectedMessages)
			}
			for _, conn := range conns {
				conn.Close()
			}
		})
	}
	server.Shutdown()
}

func BenchmarkServer(b *testing.B) {
	startServer := func() *server.PubSubServer {
		server := server.NewPubSubServer(server.TCP)
		go func() {
			if err := server.Listen(); err != nil {
				log.Fatal(err)
			}
		}()
		return server
	}
	ps := startServer()

	client := &client.PubSubClient{
		Addr: "localhost:2416",
	}
	numberOfClients := 500
	messageChans := make([]chan messages.Message, numberOfClients)
	conns := make([]net.Conn, numberOfClients)
	for i := 0; i < numberOfClients; i++ {
		c, conn, err := client.Subscribe("engine", &messages.EngineMessage{})
		if err != nil {
			b.FailNow()
		}
		messageChans[i] = c
		conns[i] = conn
	}
	for _, c := range messageChans {
		go func(messageChan chan messages.Message) {
			for msg := range messageChan {
				if msg.String() == "" {
					continue
				}
			}
		}(c)
	}
	m := &messages.EngineMessage{Verb: "start"}
	message, _ := m.ToJSON()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.Broadcast("engine", message)
	}
	for _, conn := range conns {
		conn.Close()
	}
}
