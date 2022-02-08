package dbi

import (
	_ "github.com/lib/pq"
	"testing"
)

func TestDbiOpen(t *testing.T) {
	c := &Config{}
	if err := c.Open(); err == nil {
		t.Fatalf("Open empty didn't fail")
	}
}
