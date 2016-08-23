package data

import (
	"encoding/json"
)

type jsonSerializer struct {
}

func (j jsonSerializer) MarshalPayload(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j jsonSerializer) UnmarshalPayload(d []byte, v interface{}) (error) {
	return json.Unmarshal(d, v)
}

func NewJsonSerializer() (*jsonSerializer){
	return &jsonSerializer{}
}