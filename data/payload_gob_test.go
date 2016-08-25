package data

import (
	"testing"
	"github.com/stretchr/testify/assert"

	"time"
	"encoding/gob"
)

func TestGob_Common(t *testing.T) {
	testSimple(t, GOB)
	testStruct(t, GOB)
}

func TestGob_Interface(t *testing.T) {
	assert := assert.New(t)

	//marshal unknown interface/type
	a := newA()
	b := map[string]interface{}{}
	b["string"] = a.F_string
	b["time"] = a.F_time
	p, err := NewPayload(b, GOB)
	assert.NotNil(err)
	assert.Nil(p)

	//marshal known interface/type
	gob.Register(time.Time{})
	p, err = NewPayload(b, GOB)
	assert.Nil(err)
	assert.NotNil(p)

	//unmarshal known interface/type
	v := map[string]interface{}{}
	err = Unmarshal(p, &v)
	assert.Nil(err)
	assert.Equal(v["string"], a.F_string)
	assert.Equal(v["time"], a.F_time)
}

func BenchmarkGob_New(b *testing.B) {
	benchNewPayload(b, GOB)
}

func BenchmarkGob_Clone(b *testing.B) {
	benchClone(b, GOB)
}

func BenchmarkGob_Unmarshal(b *testing.B) {
	benchUnmarshal(b, GOB)
}