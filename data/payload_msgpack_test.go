/**
 * Created by Andrey Gayvoronsky on 25/08/16.
 * (C) Luxms-BI
 */

package data

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestMsgpack_Common(t *testing.T) {
	testSimple(t, MSGPACK)
	testStruct(t, MSGPACK)
}

func TestMsgpack_Interface(t *testing.T) {
	assert := assert.New(t)

	a := newA()
	b := map[string]interface{}{}
	b["string"] = a.F_string
	b["time"] = a.F_time
	p, err := NewPayload(b, MSGPACK)
	assert.Nil(err)
	assert.NotNil(p)

	v := map[string]interface{}{}
	err = Unmarshal(p, &v)
	assert.Nil(err)
	assert.Equal(v["string"], a.F_string)

	//TODO refactor test for custom types at MSGPACK
	//mt, err := a.F_time.MarshalText()
	//assert.Nil(err)
	//assert.Equal(v["time"], string(mt))
}


func BenchmarkMsgpack_New(b *testing.B) {
	benchNewPayload(b, MSGPACK)
}

func BenchmarkMsgpack_Clone(b *testing.B) {
	benchClone(b, MSGPACK)
}

func BenchmarkMsgpack_Unmarshal(b *testing.B) {
	benchUnmarshal(b, MSGPACK)
}
