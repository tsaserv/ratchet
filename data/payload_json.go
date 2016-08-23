package data

import (
	"encoding/json"
)

type JsonSerializer struct {
}

func (j JsonSerializer) MarshalPayload(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j JsonSerializer) UnmarshalPayload(d []byte, v interface{}) (error) {
	return json.Unmarshal(d, v)
}

func NewJsonSerializer() (*JsonSerializer){
	return &JsonSerializer{}
}