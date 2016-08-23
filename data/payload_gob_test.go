package data

import (
	"testing"
	"github.com/stretchr/testify/assert"

	"time"
	"encoding/gob"
)

var gob_payload = NewGobSerializer()

func TestGobSerializer_Simple(t *testing.T) {
	assert := assert.New(t)

	d, err := gob_payload.MarshalPayload("12345")
	assert.Nil(err)
	assert.NotNil(d)

	var v string
	err = gob_payload.UnmarshalPayload(d, &v)
	assert.Nil(err)
	assert.Equal(v, "12345")
}

func TestGobSerializer_Struct(t *testing.T) {
	assert := assert.New(t)

	//test simple struct
	a := newA()
	d, err := gob_payload.MarshalPayload(a)
	assert.Nil(err)
	assert.NotNil(d)

	v := A{}
	err = gob_payload.UnmarshalPayload(d, &v)
	assert.Nil(err)
	assertStructs(t, &a, &v)
}

func TestGobSerializer_Interface(t *testing.T) {
	assert := assert.New(t)

	//marshal unknown interface/type
	a := newA()
	b := map[string]interface{}{}
	b["string"] = a.F_string
	b["time"] = a.F_time
	d, err := gob_payload.MarshalPayload(b)
	assert.NotNil(err)
	assert.Nil(d)

	//marshal known interface/type
	gob.Register(time.Time{})
	d, err = gob_payload.MarshalPayload(b)
	assert.Nil(err)
	assert.NotNil(d)

	//unmarshal known interface/type
	v := map[string]interface{}{}
	err = gob_payload.UnmarshalPayload(d, &v)
	assert.Nil(err)
	assert.Equal(v["string"], a.F_string)
	assert.Equal(v["time"], a.F_time)
}
