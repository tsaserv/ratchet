package data

import (
	"bytes"
	"encoding/gob"
)

type gobSerializer struct {
}

func (g gobSerializer) MarshalPayload(v interface{}) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (g gobSerializer) UnmarshalPayload(d []byte, v interface{}) (error) {
	return gob.NewDecoder(bytes.NewBuffer(d)).Decode(v)
}

func NewGobSerializer() (*gobSerializer){
	return &gobSerializer{}
}


