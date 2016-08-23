package data

import (
	"bytes"
	"encoding/gob"
)

type GobSerializer struct {
}

func (g GobSerializer) MarshalPayload(v interface{}) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (g GobSerializer) UnmarshalPayload(d []byte, v interface{}) (error) {
	return gob.NewDecoder(bytes.NewBuffer(d)).Decode(v)
}

func NewGobSerializer() (*GobSerializer){
	return &GobSerializer{}
}



