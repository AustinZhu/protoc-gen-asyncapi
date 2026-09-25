package asyncapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"go.yaml.in/yaml/v3"
)

// Map is a string keyed map that remembers insertion order.
type Map[V any] struct {
	keys   []string
	values map[string]V
}

// NewMap returns an empty Map.
func NewMap[V any]() *Map[V] { return &Map[V]{values: map[string]V{}} }

// Set adds or replaces a value. New keys are appended.
func (m *Map[V]) Set(key string, value V) {
	if m.values == nil {
		m.values = map[string]V{}
	}
	if _, ok := m.values[key]; !ok {
		m.keys = append(m.keys, key)
	}
	m.values[key] = value
}

// Get returns the value stored under key.
func (m *Map[V]) Get(key string) (V, bool) {
	if m == nil {
		var zero V
		return zero, false
	}
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

// Keys returns the keys in insertion order.
func (m *Map[V]) Keys() []string {
	if m == nil {
		return nil
	}
	return m.keys
}

// Len returns the number of entries.
func (m *Map[V]) Len() int {
	if m == nil {
		return 0
	}
	return len(m.keys)
}

// IsZero reports whether the map is empty; used by omitempty.
func (m *Map[V]) IsZero() bool { return m.Len() == 0 }

// MarshalYAML renders the map as a YAML mapping in insertion order.
func (m *Map[V]) MarshalYAML() (any, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, k := range m.keys {
		var v yaml.Node
		if err := v.Encode(m.values[k]); err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: k}, &v)
	}
	return node, nil
}

// MarshalYAML renders a document with two space indentation.
func MarshalYAML(v any, header string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(header)
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalJSON renders a document as indented JSON, keeping the field order
// of the YAML rendering.
func MarshalJSON(v any) ([]byte, error) {
	var node yaml.Node
	if err := node.Encode(v); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := writeJSON(&buf, &node, ""); err != nil {
		return nil, err
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

func writeJSON(buf *bytes.Buffer, n *yaml.Node, indent string) error {
	switch n.Kind {
	case yaml.DocumentNode:
		return writeJSON(buf, n.Content[0], indent)
	case yaml.AliasNode:
		return writeJSON(buf, n.Alias, indent)
	case yaml.MappingNode:
		if len(n.Content) == 0 {
			buf.WriteString("{}")
			return nil
		}
		buf.WriteString("{\n")
		inner := indent + "  "
		for i := 0; i < len(n.Content); i += 2 {
			buf.WriteString(inner)
			writeString(buf, n.Content[i].Value)
			buf.WriteString(": ")
			if err := writeJSON(buf, n.Content[i+1], inner); err != nil {
				return err
			}
			if i+2 < len(n.Content) {
				buf.WriteByte(',')
			}
			buf.WriteByte('\n')
		}
		buf.WriteString(indent)
		buf.WriteByte('}')
	case yaml.SequenceNode:
		if len(n.Content) == 0 {
			buf.WriteString("[]")
			return nil
		}
		buf.WriteString("[\n")
		inner := indent + "  "
		for i, c := range n.Content {
			buf.WriteString(inner)
			if err := writeJSON(buf, c, inner); err != nil {
				return err
			}
			if i+1 < len(n.Content) {
				buf.WriteByte(',')
			}
			buf.WriteByte('\n')
		}
		buf.WriteString(indent)
		buf.WriteByte(']')
	case yaml.ScalarNode:
		return writeScalar(buf, n)
	default:
		return fmt.Errorf("unsupported YAML node kind %v", n.Kind)
	}
	return nil
}

func writeScalar(buf *bytes.Buffer, n *yaml.Node) error {
	switch n.ShortTag() {
	case "!!null":
		buf.WriteString("null")
	case "!!bool":
		b, err := strconv.ParseBool(n.Value)
		if err != nil {
			return err
		}
		buf.WriteString(strconv.FormatBool(b))
	case "!!int":
		if _, err := strconv.ParseInt(n.Value, 0, 64); err != nil {
			if _, err := strconv.ParseUint(n.Value, 0, 64); err != nil {
				return fmt.Errorf("invalid integer %q", n.Value)
			}
		}
		buf.WriteString(n.Value)
	case "!!float":
		f, err := strconv.ParseFloat(n.Value, 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			writeString(buf, n.Value)
			return nil
		}
		buf.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
	default:
		writeString(buf, n.Value)
	}
	return nil
}

func writeString(buf *bytes.Buffer, s string) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	buf.Write(bytes.TrimSuffix(b.Bytes(), []byte("\n")))
}
