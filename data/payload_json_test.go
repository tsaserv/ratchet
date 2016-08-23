package data

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

var json_payload = NewJsonSerializer()

func TestJsonSerializer_Simple(t *testing.T) {
	assert := assert.New(t)

	d, err := json_payload.MarshalPayload("12345")
	assert.Nil(err)
	assert.NotNil(d)

	var v string
	err = json_payload.UnmarshalPayload(d, &v)
	assert.Nil(err)
	assert.Equal(v, "12345")
}

func TestJsonSerializer_Struct(t *testing.T) {
	assert := assert.New(t)

	//test simple struct
	a := newA()
	d, err := json_payload.MarshalPayload(a)
	assert.Nil(err)
	assert.NotNil(d)

	v := A{}
	err = json_payload.UnmarshalPayload(d, &v)
	assert.Nil(err)
	assertStructs(t, &a, &v)
}
/*

func TestJsonSerializer_Interface(t *testing.T) {
	assert := assert.New(t)

	//marshal unknown interface/type
	a := newA()
	b := map[string]interface{}{}
	b["string"] = a.F_string
	b["time"] = a.F_time
	d, err := json_payload.MarshalPayload(b)
	assert.NotNil(err)
	assert.Nil(d)

	//marshal known interface/type
	gob.Register(time.Time{})
	d, err = json_payload.MarshalPayload(b)
	assert.Nil(err)
	assert.NotNil(d)

	//unmarshal known interface/type
	v := map[string]interface{}{}
	err = json_payload.UnmarshalPayload(d, &v)
	assert.Nil(err)
	assert.Equal(v["string"], a.F_string)
	assert.Equal(v["time"], a.F_time)
}
*/
