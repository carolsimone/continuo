package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/stretchr/testify/assert"
)

func TestHeadroom(t *testing.T) {
	cases := []struct {
		name                   string
		limit, inFlight, batch int
		want                   int
	}{
		{"free slots below batch", 50, 47, 50, 3},
		{"batch caps a large headroom", 50, 0, 20, 20},
		{"at the cap", 50, 50, 50, 0},
		{"cap lowered below what is in flight", 10, 40, 50, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, model.Headroom(c.limit, c.inFlight, c.batch))
		})
	}
}
