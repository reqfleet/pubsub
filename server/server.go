package server

import (
	"encoding/json"
	"log"
	"net"
	"strings"
	"time"

	"github.com/reqfleet/pubsub/broker"
	"github.com/reqfleet/pubsub/messages"
)

type ServerMode string

const (
	TCP ServerMode = "TCP"
)

type PubSubServer struct {
	broker *broker.Broker
	Mode   ServerMode
}

func NewPubSubServer(mode ServerMode) *PubSubServer {
	return &PubSubServer{
		broker: broker.NewBroker(broker.BrokerConfig{WorkerNum: 10, GCInterval: 10 * time.Second}),
		Mode:   mode,
	}
}

func (s *PubSubServer) Broadcast(topic string, message messages.Message) error {
	return s.broker.Broadcast(topic, message)
}

func (s *PubSubServer) Roundrobin(topic string, message messages.Message) error {
	return s.broker.Roundrobin(topic, message)
}

func (s *PubSubServer) handleClient(conn net.Conn, decoder *json.Decoder) {
	topic := &messages.Topic{}
	if err := decoder.Decode(topic); err != nil {
		return
	}
	tt := topic.Name
	s.broker.AddClient(topic.Name, conn)
	for {
		if err := decoder.Decode(topic); err != nil {
			s.broker.RemoveClient(tt, conn)
			break
		}
	}
}

func (s *PubSubServer) NumberOfClients(topic string) int {
	return s.broker.NumberOfClients(topic)
}

func (s *PubSubServer) HandleConnection(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	if s.Mode == TCP {
		tcpConn := conn.(*net.TCPConn)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(2 * time.Second)
		s.handleClient(tcpConn, decoder)
		return
	}
	s.handleClient(conn, decoder)
}

func (s *PubSubServer) Listen() error {
	listener, err := net.Listen(strings.ToLower(string(s.Mode)), ":2416")
	if err != nil {
		log.Println("Error starting server:", err)
		return err
	}
	log.Println("PubSub server started on port 2416")
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go s.HandleConnection(conn)
	}
}

func (s *PubSubServer) Shutdown() {
	// TODO stop the socket listening as well
	s.broker.Close()
}
