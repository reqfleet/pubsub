package client

import (
	"encoding/json"
	"net"
	"time"

	"github.com/reqfleet/pubsub/messages"
)

type PubSubClient struct {
	Addr string
}

func (psc *PubSubClient) connect() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", psc.Addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetKeepAlivePeriod(1 * time.Second)
	return tcpConn, nil
}

func (psc *PubSubClient) clean(messageChan chan messages.Message, conn net.Conn) {
	close(messageChan)
	conn.Close()
}

func (psc *PubSubClient) Subscribe(topic string, value messages.Message) (chan messages.Message, net.Conn, error) {
	conn, err := psc.connect()
	if err != nil {
		return nil, nil, err
	}
	t := &messages.Topic{
		Name: topic,
	}
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(t); err != nil {
		return nil, nil, err
	}
	messageChan := make(chan messages.Message)
	go func(conn net.Conn) {
		defer psc.clean(messageChan, conn)
		decoder := json.NewDecoder(conn)
		for {
			if err := decoder.Decode(value); err != nil {
				break
			}
			messageChan <- value
		}
	}(conn)
	return messageChan, conn, nil
}
