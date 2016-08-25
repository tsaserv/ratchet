package data

import (
	"encoding/json"
)

var JSON SerializerType

type jsonSerializer struct {
}

func (j jsonSerializer) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j jsonSerializer) Unmarshal(d []byte, v interface{}) (error) {
	return json.Unmarshal(d, v)
}

func (j jsonSerializer) Type() (SerializerType) {
	return JSON
}

func init() {
	JSON = NextType()

	RegisterType(JSON, func()(Serializer) {
		return &jsonSerializer{}
	})
}