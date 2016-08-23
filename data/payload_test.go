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
	assert.Equal(a.F_bool, b.F_bool)
	assert.Equal(a.F_float, b.F_float)
	assert.Equal(a.F_float, b.F_float)
}
