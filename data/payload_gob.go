package data

import (
	"bytes"
	"encoding/gob"
)

var GOB SerializerType

type gobSerializer struct {
}

func (g gobSerializer) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (g gobSerializer) Unmarshal(d []byte, v interface{}) (error) {
	return gob.NewDecoder(bytes.NewBuffer(d)).Decode(v)
}

func (g gobSerializer) Type() (SerializerType) {
	return GOB
}

func init() {
	GOB = NextType()

	RegisterType(GOB, func()(Serializer) {
		return &gobSerializer{}
	})
}

