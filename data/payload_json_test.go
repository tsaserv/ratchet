package data

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestJson_Common(t *testing.T) {
	testSimple(t, JSON)
	testStruct(t, JSON)
	testClone(t, JSON)
}

func TestJson_Interface(t *testing.T) {
	assert := assert.New(t)

	a := newA()
	b := map[string]interface{}{}
	b["string"] = a.F_string
	b["time"] = a.F_time
	p, err := JSON.NewPayload(b)
	assert.Nil(err)
	assert.NotNil(p)

	v := map[string]interface{}{}
	err = Unmarshal(p, &v)
	assert.Nil(err)
	assert.Equal(v["string"], a.F_string)

	//TODO refactor test for custom types at JSON
	mt, err := a.F_time.MarshalText()
	assert.Nil(err)
	assert.Equal(v["time"], string(mt))
}


func BenchmarkJson_New(b *testing.B) {
	benchNewPayload(b, JSON)
}

func BenchmarkJson_Clone(b *testing.B) {
	benchClone(b, JSON)
}

func BenchmarkJson_Unmarshal(b *testing.B) {
	benchUnmarshal(b, JSON)
}