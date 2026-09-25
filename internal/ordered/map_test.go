package ordered

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMap(t *testing.T) {
	m := &Map[int]{}
	m.Set("z", 1)
	m.Set("a", 2)
	m.Set("m", 3)
	m.Set("z", 4) // replacing keeps position
	m.Delete("m")
	m.Delete("missing")

	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"z":4,"a":2}` {
		t.Errorf("json = %s", b)
	}
	y, err := yaml.Marshal(struct {
		M *Map[int] `yaml:"m,omitempty"`
		E *Map[int] `yaml:"e,omitempty"`
	}{M: m, E: &Map[int]{}})
	if err != nil {
		t.Fatal(err)
	}
	if string(y) != "m:\n    z: 4\n    a: 2\n" {
		t.Errorf("yaml = %q", y)
	}
	m.SortKeys()
	if k := m.Keys(); k[0] != "a" || k[1] != "z" {
		t.Errorf("sorted keys = %v", k)
	}
}
