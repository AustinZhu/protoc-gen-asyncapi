// Package jsonschema converts Protobuf message and enum descriptors into JSON
// Schema documents describing their canonical protobuf JSON encoding (the
// encoding Temporal's "json/protobuf" payload converter uses).
//
// The emitted keywords stay within JSON Schema draft-07, the dialect AsyncAPI
// 3.0's default schema format (the AsyncAPI Schema Object) is a superset of, so
// the schemas can be embedded in an AsyncAPI document without a schemaFormat.
package jsonschema

import "github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/ordered"

// Schema is the subset of JSON Schema the converter emits. Field order is the
// order keywords appear in the marshaled output.
type Schema struct {
	Ref              string                `json:"$ref,omitempty" yaml:"$ref,omitempty"`
	Title            string                `json:"title,omitempty" yaml:"title,omitempty"`
	Description      string                `json:"description,omitempty" yaml:"description,omitempty"`
	Type             any                   `json:"type,omitempty" yaml:"type,omitempty"` // string or []string
	Format           string                `json:"format,omitempty" yaml:"format,omitempty"`
	Pattern          string                `json:"pattern,omitempty" yaml:"pattern,omitempty"`
	ContentEncoding  string                `json:"contentEncoding,omitempty" yaml:"contentEncoding,omitempty"`
	Enum             []any                 `json:"enum,omitempty" yaml:"enum,omitempty"`
	Const            any                   `json:"const,omitempty" yaml:"const,omitempty"`
	Minimum          any                   `json:"minimum,omitempty" yaml:"minimum,omitempty"`
	ExclusiveMinimum any                   `json:"exclusiveMinimum,omitempty" yaml:"exclusiveMinimum,omitempty"`
	Maximum          any                   `json:"maximum,omitempty" yaml:"maximum,omitempty"`
	ExclusiveMaximum any                   `json:"exclusiveMaximum,omitempty" yaml:"exclusiveMaximum,omitempty"`
	MinLength        *uint64               `json:"minLength,omitempty" yaml:"minLength,omitempty"`
	MaxLength        *uint64               `json:"maxLength,omitempty" yaml:"maxLength,omitempty"`
	Items            *Schema               `json:"items,omitempty" yaml:"items,omitempty"`
	MinItems         *uint64               `json:"minItems,omitempty" yaml:"minItems,omitempty"`
	MaxItems         *uint64               `json:"maxItems,omitempty" yaml:"maxItems,omitempty"`
	UniqueItems      bool                  `json:"uniqueItems,omitempty" yaml:"uniqueItems,omitempty"`
	Properties       *ordered.Map[*Schema] `json:"properties,omitempty" yaml:"properties,omitempty"`
	Required         []string              `json:"required,omitempty" yaml:"required,omitempty"`
	PropertyNames    *Schema               `json:"propertyNames,omitempty" yaml:"propertyNames,omitempty"`
	AdditionalProps  any                   `json:"additionalProperties,omitempty" yaml:"additionalProperties,omitempty"` // bool or *Schema
	MinProperties    *uint64               `json:"minProperties,omitempty" yaml:"minProperties,omitempty"`
	MaxProperties    *uint64               `json:"maxProperties,omitempty" yaml:"maxProperties,omitempty"`
	OneOf            []*Schema             `json:"oneOf,omitempty" yaml:"oneOf,omitempty"`
	AnyOf            []*Schema             `json:"anyOf,omitempty" yaml:"anyOf,omitempty"`
	AllOf            []*Schema             `json:"allOf,omitempty" yaml:"allOf,omitempty"`
	Not              *Schema               `json:"not,omitempty" yaml:"not,omitempty"`
	Deprecated       bool                  `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`
}

// clone returns a shallow copy, enough to decorate a shared well-known-type
// schema with per-field keywords.
func (s *Schema) clone() *Schema {
	c := *s
	return &c
}
