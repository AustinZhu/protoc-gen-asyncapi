package asyncapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// DecodeJSON parses a JSON value, keeping the key order of objects: objects
// become *Map[any], arrays []any, integers int64 (uint64 when larger) and
// other numbers float64.
func DecodeJSON(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("unexpected data after the JSON value")
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			m := NewMap[any]()
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := k.(string)
				if _, dup := m.Get(key); dup {
					return nil, fmt.Errorf("duplicate key %q", key)
				}
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				m.Set(key, v)
			}
			_, err := dec.Token() // '}'
			return m, err
		case '[':
			list := []any{}
			for dec.More() {
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				list = append(list, v)
			}
			_, err := dec.Token() // ']'
			return list, err
		}
		return nil, fmt.Errorf("unexpected %v", t)
	case json.Number:
		s := t.String()
		if !strings.ContainsAny(s, ".eE") {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i, nil
			}
			if u, err := strconv.ParseUint(s, 10, 64); err == nil {
				return u, nil
			}
		}
		return t.Float64()
	}
	return tok, nil // string, bool or nil
}

// ToPlain converts a value from DecodeJSON to plain Go values
// (map[string]any), e.g. for JSON Schema validation.
func ToPlain(v any) any {
	switch t := v.(type) {
	case *Map[any]:
		out := map[string]any{}
		for _, k := range t.Keys() {
			x, _ := t.Get(k)
			out[k] = ToPlain(x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = ToPlain(x)
		}
		return out
	case int64:
		return json.Number(strconv.FormatInt(t, 10))
	case uint64:
		return json.Number(strconv.FormatUint(t, 10))
	case float64:
		return json.Number(strconv.FormatFloat(t, 'g', -1, 64))
	}
	return v
}

// schemaAlias has the fields of Schema without its marshaller.
type schemaAlias Schema

// MarshalYAML renders a schema. When Raw is set (a schema given verbatim,
// e.g. by an annotation), its keywords are rendered after those of the
// typed fields it does not set.
func (s *Schema) MarshalYAML() (any, error) {
	if s.Raw == nil {
		return (*schemaAlias)(s), nil
	}
	var typed, raw yaml.Node
	if err := typed.Encode((*schemaAlias)(s)); err != nil {
		return nil, err
	}
	if err := raw.Encode(s.Raw); err != nil {
		return nil, err
	}
	out := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for i := 0; i+1 < len(typed.Content); i += 2 {
		if _, ok := s.Raw.Get(typed.Content[i].Value); !ok {
			out.Content = append(out.Content, typed.Content[i], typed.Content[i+1])
		}
	}
	out.Content = append(out.Content, raw.Content...)
	return out, nil
}
