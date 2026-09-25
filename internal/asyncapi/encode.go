package asyncapi

import (
	"bytes"
	"encoding/json"
	"sort"

	"go.yaml.in/yaml/v3"
)

// Map is an insertion-ordered string-keyed map that marshals to YAML and JSON
// with its keys in insertion order, so generated documents are stable and read
// in a meaningful order rather than Go's random map order. The zero value is
// ready to use.
type Map[V any] struct {
	keys   []string
	values map[string]V
}

// Set inserts or replaces the value for key. Replacing keeps the original position.
func (m *Map[V]) Set(key string, value V) {
	if m.values == nil {
		m.values = make(map[string]V)
	}
	if _, ok := m.values[key]; !ok {
		m.keys = append(m.keys, key)
	}
	m.values[key] = value
}

// Get returns the value for key and whether it was present.
func (m *Map[V]) Get(key string) (V, bool) {
	v, ok := m.values[key]
	return v, ok
}

// Delete removes key if present.
func (m *Map[V]) Delete(key string) {
	if _, ok := m.values[key]; !ok {
		return
	}
	delete(m.values, key)
	for i, k := range m.keys {
		if k == key {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
}

// Len returns the number of entries.
func (m *Map[V]) Len() int {
	if m == nil {
		return 0
	}
	return len(m.keys)
}

// Keys returns the keys in insertion order.
func (m *Map[V]) Keys() []string {
	if m == nil {
		return nil
	}
	return append([]string(nil), m.keys...)
}

// SortKeys reorders the entries by key.
func (m *Map[V]) SortKeys() { sort.Strings(m.keys) }

// IsZero reports whether the map is empty, so `omitempty` drops it in YAML.
func (m *Map[V]) IsZero() bool { return m.Len() == 0 }

// MarshalYAML implements yaml.Marshaler.
func (m *Map[V]) MarshalYAML() (any, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, k := range m.keys {
		var val yaml.Node
		if err := val.Encode(m.values[k]); err != nil {
			return nil, err
		}
		node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: k}, &val)
	}
	return node, nil
}

// MarshalJSON implements json.Marshaler.
func (m *Map[V]) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range m.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := json.Marshal(m.values[k])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// MarshalYAML renders doc as YAML with a two-space indent, preceded by header
// (comment lines, each ending in a newline).
func MarshalYAML(doc *Document, header string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(header)
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalJSON renders doc as indented JSON without HTML escaping.
func MarshalJSON(doc *Document) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
