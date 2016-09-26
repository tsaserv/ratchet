package data

import (
	"testing"
	"github.com/stretchr/testify/assert"

	"time"
)

type A struct {
	F_string	string
	F_time		time.Time
	F_int		int
	F_bool		bool
	F_float		float64
}

func newA()(A) {
	return A{
		F_string: "some text",
		F_time: time.Now(),
		F_int: 12345,
		F_bool: true,
		F_float: 12345.54321,
	}
}

func assertStructs(t *testing.T, a, b *A) {
	assert := assert.New(t)

	assert.Equal(a.F_string, b.F_string)
	assert.Equal(a.F_time, b.F_time)
	assert.Equal(a.F_int, b.F_int)
	assert.Equal(a.F_bool, b.F_bool)
	assert.Equal(a.F_float, b.F_float)
}

func testSimple(t *testing.T, st SerializerType) {
	assert := assert.New(t)

	p, err := NewPayload("12345", st)
	assert.Nil(err)
	assert.NotNil(p)

	var v string
	err = Unmarshal(p, &v)
	assert.Nil(err)
	assert.Equal(v, "12345")
}

func testStruct(t *testing.T, st SerializerType) {
	assert := assert.New(t)

	//test simple struct
	a := newA()
	p, err := NewPayload(a, st)
	assert.Nil(err)
	assert.NotNil(p)

	v := A{}
	err = Unmarshal(p, &v)
	assert.Nil(err)
	assertStructs(t, &a, &v)
}

func testClone(t *testing.T, st SerializerType) {
	assert := assert.New(t)

	//test simple struct
	a := newA()
	p, err := NewPayload(a, st)
	assert.Nil(err)
	assert.NotNil(p)

	v := A{}
	pc := Clone(p)
	err = Unmarshal(pc, &v)
	assert.Nil(err)
	assertStructs(t, &a, &v)
}

func benchNewPayload(b *testing.B, st SerializerType) {
	a := newA()

	for i := 0; i < b.N; i++ {
		NewPayload(a, st)
	}
}

func benchClone(b *testing.B, st SerializerType) {
	a := newA()
	p,_ := NewPayload(a, st)

	for i := 0; i < b.N; i++ {
		Clone(p)
	}
}

func benchUnmarshal(b *testing.B, st SerializerType) {
	a := newA()
	p,_ := NewPayload(a, st)

	for i := 0; i < b.N; i++ {
		UnmarshalSilent(p, &A{})
	}
}
