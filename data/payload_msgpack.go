package data


import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

var MSGPACK SerializerType

type msgPackSerializer struct {
}

func (g msgPackSerializer) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (g msgPackSerializer) Unmarshal(d []byte, v interface{}) (error) {
	return msgpack.Unmarshal(d, v)
}

func (g msgPackSerializer) Type() (SerializerType) {
	return MSGPACK
}

func init() {
	MSGPACK = NextType()

	RegisterType(MSGPACK, func()(Serializer) {
		return &msgPackSerializer{}
	})
}
