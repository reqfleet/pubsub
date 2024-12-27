package broker

import (
	"container/list"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type queueitem struct {
	conn    net.Conn
	topic   string
	message []byte
	err     error
}

type BrokerConfig struct {
	WorkerNum        int
	BroadcastTimeout time.Duration
	GCInterval       time.Duration
}

type Broker struct {
	closed           bool
	clients          map[string]*list.List
	connToEle        map[net.Conn]*list.Element
	broadcastTimeout time.Duration
	workerQueue      chan queueitem
	workerNum        int
	gcInterval       time.Duration
	mu               sync.RWMutex
	wg               sync.WaitGroup
}

func NewBroker(config BrokerConfig) *Broker {
	gcInterval := config.GCInterval
	if gcInterval == 0 {
		gcInterval = 1 * time.Minute
	}
	b := &Broker{
		workerNum:        config.WorkerNum,
		clients:          make(map[string]*list.List),
		workerQueue:      make(chan queueitem),
		broadcastTimeout: config.BroadcastTimeout,
		connToEle:        make(map[net.Conn]*list.Element),
		gcInterval:       gcInterval,
	}
	b.workerStart()
	go b.gc()
	return b
}

func (b *Broker) gc() {
	for {
		b.mu.Lock()
		for topic, clients := range b.clients {
			log.Printf("GC checking topic %s, clients len %d", topic, clients.Len())
			if clients == nil || clients.Len() == 0 {
				continue
			}
			for e := clients.Front(); e != nil; e = e.Next() {
				conn := e.Value.(net.Conn)
				if _, err := conn.Write([]byte("\n")); err != nil {
					next := e.Next()
					go func() {
						b.RemoveClient(topic, conn)
						log.Printf("GC closed broken conn %s, topic: %s", conn.RemoteAddr(), topic)
					}()
					e = next
					if e == nil {
						break
					}
				}
			}
		}
		b.mu.Unlock()
		time.Sleep(b.gcInterval)
	}
}

func (b *Broker) worker(number int) {
	defer b.wg.Done()
	for item := range b.workerQueue {
		// Maybe we should set a deadline for the write operation
		if b.broadcastTimeout.Abs() > 0 {
			log.Println("Setting deadline for", item.conn.RemoteAddr())
			item.conn.SetWriteDeadline(time.Now().Add(b.broadcastTimeout))
		}
		if _, err := item.conn.Write(item.message); err != nil {
			item.err = err
			go func(item queueitem) { b.RemoveClient(item.topic, item.conn) }(item)
			continue
		}
	}
	log.Println("Worker finished, id: ", number)
}

func (b *Broker) workerStart() {
	for i := 0; i < b.workerNum; i++ {
		b.wg.Add(1)
		go b.worker(i)
	}
	log.Println("Started broker with", b.workerNum, "workers")
}

func (b *Broker) AddClient(topic string, conn net.Conn) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errors.New("Broker is closed")
	}
	if _, ok := b.clients[topic]; !ok {
		b.clients[topic] = list.New()
	}
	l := b.clients[topic]
	ele := l.PushFront(conn)
	b.connToEle[conn] = ele
	log.Printf("Client connected to %s, topic %s, client len %d", conn.RemoteAddr(), topic, l.Len())
	return nil
}

// cleanConn removes the connection from the broker's internal state
// This method does not have lock and it should be used by the caller with a lock
func (b *Broker) cleanConn(conn net.Conn) {
	delete(b.connToEle, conn)
	conn.Close()
}

func (b *Broker) RemoveClient(topic string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	l := b.clients[topic]
	if l == nil {
		return
	}
	ele, ok := b.connToEle[conn]
	if !ok {
		return
	}
	l.Remove(ele)
	b.cleanConn(conn)
	log.Println("Client disconnected from", conn.RemoteAddr())
}

func (b *Broker) Broadcast(topic string, message []byte) {
	b.mu.RLock()
	clients := b.clients[topic]
	if clients == nil {
		b.mu.RUnlock()
		return
	}
	for e := clients.Front(); e != nil; e = e.Next() {
		conn := e.Value.(net.Conn)
		b.workerQueue <- queueitem{conn: conn, message: message, topic: topic}
	}
	b.mu.RUnlock()
}

func (b *Broker) Roundrobin(topic string, message []byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	clients := b.clients[topic]
	if clients == nil {
		return
	}
	ele := clients.Front()
	if ele == nil {
		return
	}
	conn := ele.Value.(net.Conn)
	clients.MoveToBack(ele)
	b.workerQueue <- queueitem{conn: conn, message: message, topic: topic}
}

func (b *Broker) NumberOfClients(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	clients := b.clients[topic]
	if clients == nil {
		return 0
	}
	return clients.Len()
}

func (b *Broker) Close() {
	close(b.workerQueue)
	b.mu.RLock()
	b.closed = true
	for _, clients := range b.clients {
		for e := clients.Front(); e != nil; e = e.Next() {
			c := e.Value.(net.Conn)
			c.Close()
		}
		clients.Init()
	}
	b.clients = nil
	b.mu.RUnlock()
	b.wg.Wait()
	log.Println("Broker closed")
	log.Println("len(b.clients):", len(b.clients))
}
