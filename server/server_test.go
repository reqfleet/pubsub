package server_test

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/reqfleet/pubsub/client"
	"github.com/reqfleet/pubsub/messages"
	"github.com/reqfleet/pubsub/server"
)

type EngineMessage struct {
	Verb string `json:"verb"`
}

func (em EngineMessage) ToJSON() ([]byte, error) {
	return json.Marshal(em)
}

func (em EngineMessage) String() string {
	return em.Verb
}

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
		sendFunc         func(topic string, message messages.Message) error
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
			messageChans := make([]chan messages.Message, numberOfClients)
			conns := make([]net.Conn, numberOfClients)
			for i := 0; i < numberOfClients; i++ {
				c, conn, err := client.Subscribe("engine", &EngineMessage{})
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
				m := &EngineMessage{Verb: "Start"}
				tc.sendFunc("engine", m)
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
	server := server.NewPubSubServer(server.TCP)
	go server.Listen()
	client := &client.PubSubClient{
		Addr: "localhost:2416",
	}
	numberOfClients := 500
	messageChans := make([]chan messages.Message, numberOfClients)
	conns := make([]net.Conn, numberOfClients)
	for i := 0; i < numberOfClients; i++ {
		c, conn, err := client.Subscribe("engine", &EngineMessage{})
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
	m := &EngineMessage{Verb: "start"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Broadcast("engine", m)
	}
	for _, conn := range conns {
		conn.Close()
	}
}
