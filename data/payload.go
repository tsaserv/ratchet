package data

import (
	"sync"
	"fmt"
	"log"
	"github.com/dailyburn/ratchet/logger"
)

// Payload is the data type that is passed along all data channels.
// Under the covers, Payload is simply a []byte containing binary data with built-in serializer.
// It's up to you what serializer to use
type Serializer interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(d []byte, o interface{}) (error)
	Type()(SerializerType)
}

type Payload struct {
	serializer Serializer
	data []byte
}

type SerializerType int

type serializerFactory func() (Serializer)

var (
	serializerFactoryMu sync.RWMutex
	serializerFactoryList   = make(map[SerializerType]serializerFactory)
)

func NextType() (SerializerType) {
	serializerFactoryMu.Lock()
	defer serializerFactoryMu.Unlock()

	return SerializerType(len(serializerFactoryList))
}

func RegisterType(t SerializerType, f serializerFactory) {
	serializerFactoryMu.Lock()
	defer serializerFactoryMu.Unlock()

	if f == nil {
		log.Panicln("Can't register serializer without factory.")
	}

	_, registered := serializerFactoryList[t]
	if registered {
		log.Panicln("Factory was already registered. Ignoring.")
	}

	serializerFactoryList[t] = f
}

func NewPayload(v interface{}, t SerializerType) (*Payload, error) {
	serializerFactoryMu.RLock()
	defer serializerFactoryMu.RUnlock()

	f,_ := serializerFactoryList[t]
	s := f()

	d, err := s.Marshal(v)
	if err != nil {
		logger.Debug(fmt.Sprintf("data: failure to marshal payload %+v - error is \"%v\"", v, err.Error()))
		logger.Debug(fmt.Sprintf("	Failed val: %+v", v))
		return nil, err
	}

	return &Payload{serializer: s, data: d}, err
}

func Clone(p *Payload) (*Payload) {
	serializerFactoryMu.RLock()
	defer serializerFactoryMu.RUnlock()

	dc := make([]byte, len(p.data))
	copy(dc, p.data)

	f,_ := serializerFactoryList[p.serializer.Type()]
	s := f()

	return &Payload{serializer: s, data: dc}
}

func Unmarshal(p *Payload, v interface{}) (error) {
	err := p.serializer.Unmarshal(p.data, v)
	if err != nil {
		logger.Debug(fmt.Sprintf("data: failure to unmarshal payload into %+v - error is \"%v\"", v, err.Error()))
		logger.Debug(fmt.Sprintf("	Failed Data: %+v", p))
	}

	return err
}

func UnmarshalSilent(p *Payload, v interface{}) (error) {
	return p.serializer.Unmarshal(p.data, v)
}

/*
func Objects(p Payload)([]map[string]interface{}, error) {
	var objects []map[string]interface{}

	// return if we have null instead of object(s).
	//if bytes.Equal(p, []byte("null")) {
	//	logger.Debug("Objects: received null. Expected object or objects. Skipping.")
	//	return objects, nil
	//}

	var v interface{}
	err := Unmarshal(p, &v)
	if err != nil {
		return nil, err
	}

	// check if we have a single object or a slice of objects
	switch vv := v.(type) {
	case []interface{}:
		for _, o := range vv {
			objects = append(objects, o.(map[string]interface{}))
		}
	case map[string]interface{}:
		objects = []map[string]interface{}{vv}
	case []map[string]interface{}:
		objects = vv
	default:
		err = fmt.Errorf("Objects: unsupported data type: %T", vv)
		return nil, err
	}

	return objects, nil

}
*/
