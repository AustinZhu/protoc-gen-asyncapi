package asyncapi_test

import (
	"strings"
	"testing"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
)

func TestDecodeJSON(t *testing.T) {
	v, err := asyncapi.DecodeJSON([]byte(`{"z": 1, "a": [18446744073709551615, -9223372036854775808, 1.5, true, null, "s"], "m": {"k": 1e3}}`))
	if err != nil {
		t.Fatal(err)
	}
	m := v.(*asyncapi.Map[any])
	if got := strings.Join(m.Keys(), ","); got != "z,a,m" {
		t.Errorf("keys = %s, want insertion order", got)
	}
	list, _ := m.Get("a")
	want := []any{uint64(18446744073709551615), int64(-9223372036854775808), 1.5, true, nil, "s"}
	for i, x := range list.([]any) {
		if x != want[i] {
			t.Errorf("a[%d] = %#v, want %#v", i, x, want[i])
		}
	}
	for _, bad := range []string{`{"a": 1, "a": 2}`, `{} {}`, `{"a":`, ``} {
		if _, err := asyncapi.DecodeJSON([]byte(bad)); err == nil {
			t.Errorf("DecodeJSON(%q) succeeded", bad)
		}
	}
}

func TestRawSchema(t *testing.T) {
	raw, err := asyncapi.DecodeJSON([]byte(`{"type": "integer", "minimum": 0, "description": "From the override."}`))
	if err != nil {
		t.Fatal(err)
	}
	s := &asyncapi.Schema{Description: "From the comment.", ReadOnly: true, Raw: raw.(*asyncapi.Map[any])}
	out, err := asyncapi.MarshalYAML(s, "")
	if err != nil {
		t.Fatal(err)
	}
	// Typed keywords the override does not set come first; the override
	// wins on conflicts.
	want := "readOnly: true\ntype: integer\nminimum: 0\ndescription: From the override.\n"
	if string(out) != want {
		t.Errorf("got:\n%s\nwant:\n%s", out, want)
	}
	j, err := asyncapi.MarshalJSON(&asyncapi.Schema{Raw: raw.(*asyncapi.Map[any])})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(j), `"minimum": 0`) {
		t.Errorf("JSON = %s", j)
	}
}
