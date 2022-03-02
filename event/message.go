package event

type Message struct {
	Key   string
	Value []byte
}

func (m *Message) GetKey() string {
	return m.Key
}

func (m *Message) GetValue() []byte {
	return m.Value
}

func NewMessage(key string, value []byte) *Message {
	return &Message{
		Key:   key,
		Value: value,
	}
}
