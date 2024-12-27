package messages

import (
	"encoding/json"
)

/*
reqfleet is having a coordinatoir and engine design.
coordinator and engines are communicated via pubsub through TCP.
More specifically, the coordinator sends commands to the engines via broadcast. The commands are

- start
- stop

In the future, the coordinator could send other types of commands. So we need to have a future-proof design.

So it's better to define specific message types based on the communication needs.

- engines -> EngineMessage. This is the message broadcasted by the coordinator to the engines.
   * engines need to send the EngineMessage for engine topic

*/

type Topic struct {
	Name string `json:"name"`
}

type Message interface {
	// Used by the broker to write messages to the connection
	ToJSON() ([]byte, error)

	// For debugging purposes
	String() string
}

type EngineMessage struct {
	Verb string `json:"verb"`
}

func (em EngineMessage) ToJSON() ([]byte, error) {
	return json.Marshal(em)
}

func (em EngineMessage) String() string {
	return em.Verb
}

type TestMessage struct {
	Non string `json:"non"`
}

func (tm TestMessage) ToJSON() ([]byte, error) {
	return json.Marshal(tm)
}

func (tm TestMessage) String() string {
	return tm.Non
}
