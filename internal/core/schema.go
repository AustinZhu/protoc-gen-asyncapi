package core

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
)

// schemaGen converts Protobuf messages and enums to AsyncAPI (JSON Schema)
// schemas following the Protobuf JSON mapping.
type schemaGen struct {
	b       *Builder
	schemas *asyncapi.Map[*asyncapi.Schema]
	rules   *validateRules
}

func newSchemaGen(b *Builder) *schemaGen {
	rules := &validateRules{disabled: true}
	if b.Params.Protovalidate {
		rules = newValidateRules(b.Plugin)
	}
	return &schemaGen{b: b, schemas: asyncapi.NewMap[*asyncapi.Schema](), rules: rules}
}

// addAll adds the schemas of every message and enum declared in files.
func (g *schemaGen) addAll(files []*protogen.File) error {
	var addEnums func(msgs []*protogen.Message)
	addEnums = func(msgs []*protogen.Message) {
		for _, m := range msgs {
			for _, e := range m.Enums {
				g.enumRef(e)
			}
			addEnums(m.Messages)
		}
	}
	for _, f := range files {
		var err error
		WalkMessages(f.Messages, func(m *protogen.Message) {
			if err == nil {
				_, err = g.messageRef(m)
			}
		})
		if err != nil {
			return err
		}
		for _, e := range f.Enums {
			g.enumRef(e)
		}
		addEnums(f.Messages)
	}
	return nil
}

func schemaRef(name protoreflect.FullName) *asyncapi.Schema {
	return asyncapi.SchemaRef(string(name))
}

// fieldName returns the JSON property name of a field.
func (g *schemaGen) fieldName(f *protogen.Field) string {
	if g.b.Params.JSONNames {
		return f.Desc.JSONName()
	}
	return string(f.Desc.Name())
}

// messageRef returns a schema for a message: well-known types are inlined,
// other messages are added to the components and referenced.
func (g *schemaGen) messageRef(m *protogen.Message) (*asyncapi.Schema, error) {
	if s := wellKnownSchema(m.Desc.FullName()); s != nil {
		return s, nil
	}
	name := m.Desc.FullName()
	if _, ok := g.schemas.Get(string(name)); !ok {
		// Reserve the slot first so recursive messages terminate.
		g.schemas.Set(string(name), &asyncapi.Schema{})
		s, err := g.messageSchema(m)
		if err != nil {
			return nil, err
		}
		g.schemas.Set(string(name), s)
	}
	return schemaRef(name), nil
}

func (g *schemaGen) messageSchema(m *protogen.Message) (*asyncapi.Schema, error) {
	s := &asyncapi.Schema{
		Type:        "object",
		Description: describe(m.Comments),
		Deprecated:  isDeprecated(m.Desc),
		Properties:  asyncapi.NewMap[*asyncapi.Schema](),
	}
	for _, f := range m.Fields {
		fo := fieldOptions(f)
		if fo.GetHidden() {
			continue
		}
		fs, err := g.fieldSchema(f)
		if err != nil {
			return nil, err
		}
		name := g.fieldName(f)
		s.Properties.Set(name, fs)
		behaviors := fieldBehaviors(f.Desc)
		required := fo.GetRequired() || f.Desc.Cardinality() == protoreflect.Required || behaviors[behaviorRequired]
		if behaviors[behaviorOutputOnly] {
			fs.ReadOnly = true
		}
		if behaviors[behaviorInputOnly] {
			fs.WriteOnly = true
		}
		if req, err := g.rules.apply(f, fs); err != nil {
			return nil, err
		} else if req {
			required = true
		}
		if required {
			s.Required = append(s.Required, name)
		}
	}
	if s.Properties.Len() == 0 {
		s.Properties = nil
	}

	// Real oneofs: at most one member may be set (exactly one when the
	// oneof is required by buf.validate).
	for _, o := range m.Oneofs {
		if o.Desc.IsSynthetic() || len(o.Fields) < 2 {
			continue
		}
		var branches, members []*asyncapi.Schema
		for _, f := range o.Fields {
			if fieldOptions(f).GetHidden() {
				continue
			}
			member := &asyncapi.Schema{Required: []string{g.fieldName(f)}}
			members = append(members, member)
			branches = append(branches, member)
		}
		if len(members) < 2 {
			continue
		}
		if !g.rules.oneofRequired(o.Desc) {
			branches = append(branches, &asyncapi.Schema{Not: &asyncapi.Schema{AnyOf: members}})
		}
		s.AllOf = append(s.AllOf, &asyncapi.Schema{
			Description: fmt.Sprintf("Oneof `%s`: at most one of the fields may be set.", o.Desc.Name()),
			OneOf:       branches,
		})
		if g.rules.oneofRequired(o.Desc) {
			s.AllOf[len(s.AllOf)-1].Description = fmt.Sprintf("Oneof `%s`: exactly one of the fields must be set.", o.Desc.Name())
		}
	}
	if len(s.AllOf) == 1 {
		// Keep the common case compact.
		s.OneOf, s.AllOf = s.AllOf[0].OneOf, nil
	}
	return s, nil
}

func (g *schemaGen) fieldSchema(f *protogen.Field) (*asyncapi.Schema, error) {
	var s *asyncapi.Schema
	switch {
	case f.Desc.IsMap():
		key, val := f.Message.Fields[0], f.Message.Fields[1]
		vs, err := g.singular(val)
		if err != nil {
			return nil, err
		}
		s = &asyncapi.Schema{Type: "object", AdditionalProperties: vs}
		switch key.Desc.Kind() {
		case protoreflect.BoolKind:
			s.PropertyNames = &asyncapi.Schema{Enum: []any{"true", "false"}}
		case protoreflect.StringKind:
		default:
			s.PropertyNames = &asyncapi.Schema{Pattern: `^-?[0-9]+$`}
		}
	case f.Desc.IsList():
		items, err := g.singular(f)
		if err != nil {
			return nil, err
		}
		s = &asyncapi.Schema{Type: "array", Items: items}
	default:
		var err error
		if s, err = g.singular(f); err != nil {
			return nil, err
		}
	}

	fo := fieldOptions(f)
	desc := describe(f.Comments)
	protoType := ""
	if g.b.Params.ProtoTypes {
		protoType = protoTypeName(f.Desc)
	}
	if s.Ref != "" && (desc != "" || isDeprecated(f.Desc) || len(fo.GetExamples()) > 0 || protoType != "" || hasBehaviorFlags(f.Desc)) {
		// Siblings of $ref are ignored by JSON Schema Draft 07; wrap it.
		s = &asyncapi.Schema{AllOf: []*asyncapi.Schema{s}}
	}
	if desc != "" {
		s.Description = desc
	}
	s.Deprecated = isDeprecated(f.Desc)
	if protoType != "" {
		if s.Extensions == nil {
			s.Extensions = asyncapi.Extensions{}
		}
		s.Extensions["x-protobuf-type"] = protoType
	}
	if fo.GetFormat() != "" {
		target := s
		if is, ok := s.Items.(*asyncapi.Schema); ok {
			target = is
		}
		target.Format = fo.GetFormat()
	}
	for i, e := range fo.GetExamples() {
		var v any
		if err := json.Unmarshal([]byte(e), &v); err != nil {
			return nil, Errorf(f.Desc, "example %d is not valid JSON (strings must be quoted, e.g. \"\\\"abc\\\"\"): %v", i+1, err)
		}
		s.Examples = append(s.Examples, v)
	}
	return s, nil
}

// singular returns the schema of a single value of the field's type.
func (g *schemaGen) singular(f *protogen.Field) (*asyncapi.Schema, error) {
	switch f.Desc.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return g.messageRef(f.Message)
	case protoreflect.EnumKind:
		return g.enumRef(f.Enum), nil
	}
	return scalarSchema(f.Desc.Kind()), nil
}

func scalarSchema(k protoreflect.Kind) *asyncapi.Schema {
	switch k {
	case protoreflect.BoolKind:
		return &asyncapi.Schema{Type: "boolean"}
	case protoreflect.StringKind:
		return &asyncapi.Schema{Type: "string"}
	case protoreflect.BytesKind:
		return &asyncapi.Schema{Type: "string", Format: "byte", ContentEncoding: "base64"}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return &asyncapi.Schema{Type: "integer", Format: "int32"}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return &asyncapi.Schema{Type: "integer", Format: "uint32", Minimum: 0}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		// The Protobuf JSON mapping encodes 64-bit integers as strings.
		return &asyncapi.Schema{Type: "string", Format: "int64", Pattern: `^-?[0-9]+$`}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return &asyncapi.Schema{Type: "string", Format: "uint64", Pattern: `^[0-9]+$`}
	case protoreflect.FloatKind:
		return &asyncapi.Schema{Type: "number", Format: "float"}
	case protoreflect.DoubleKind:
		return &asyncapi.Schema{Type: "number", Format: "double"}
	}
	return &asyncapi.Schema{}
}

func (g *schemaGen) enumRef(e *protogen.Enum) *asyncapi.Schema {
	if e.Desc.FullName() == "google.protobuf.NullValue" {
		return &asyncapi.Schema{Type: "null"}
	}
	name := string(e.Desc.FullName())
	if _, ok := g.schemas.Get(name); !ok {
		s := &asyncapi.Schema{Deprecated: isDeprecated(e.Desc)}
		mode := g.b.Params.EnumValues
		var lines []string
		for _, v := range e.Values {
			if mode != EnumNumbers {
				s.Enum = append(s.Enum, string(v.Desc.Name()))
			}
			if d := firstLine(describe(v.Comments)); d != "" {
				lines = append(lines, fmt.Sprintf("- `%s` (%d): %s", v.Desc.Name(), v.Desc.Number(), d))
			}
		}
		if mode != EnumNames {
			// The Protobuf JSON mapping accepts enum numbers on input.
			for _, v := range e.Values {
				s.Enum = append(s.Enum, int64(v.Desc.Number()))
			}
		}
		switch mode {
		case EnumNames:
			s.Type = "string"
		case EnumNumbers:
			s.Type, s.Format = "integer", "int32"
		default:
			s.Type = []string{"string", "integer"}
		}
		s.Description = describe(e.Comments)
		if len(lines) > 0 {
			s.Description = strings.TrimSpace(s.Description + "\n\n" + strings.Join(lines, "\n"))
		}
		g.schemas.Set(name, s)
	}
	return asyncapi.SchemaRef(string(e.Desc.FullName()))
}

// protoTypeName names the Protobuf type of a field for x-protobuf-type.
func protoTypeName(fd protoreflect.FieldDescriptor) string {
	single := func(fd protoreflect.FieldDescriptor) string {
		switch fd.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			return string(fd.Message().FullName())
		case protoreflect.EnumKind:
			return string(fd.Enum().FullName())
		}
		return fd.Kind().String()
	}
	switch {
	case fd.IsMap():
		return "map<" + single(fd.MapKey()) + ", " + single(fd.MapValue()) + ">"
	case fd.IsList():
		return "repeated " + single(fd)
	}
	return single(fd)
}

func hasBehaviorFlags(fd protoreflect.FieldDescriptor) bool {
	b := fieldBehaviors(fd)
	return b[behaviorOutputOnly] || b[behaviorInputOnly]
}

// wellKnownSchema returns the JSON representation of well-known types.
func wellKnownSchema(name protoreflect.FullName) *asyncapi.Schema {
	nullable := func(t string, format string) *asyncapi.Schema {
		return &asyncapi.Schema{Type: []string{t, "null"}, Format: format}
	}
	switch name {
	case "google.protobuf.Timestamp":
		return &asyncapi.Schema{Type: "string", Format: "date-time"}
	case "google.protobuf.Duration":
		return &asyncapi.Schema{Type: "string", Pattern: `^-?[0-9]+(\.[0-9]{1,9})?s$`, Examples: []any{"1.5s"}}
	case "google.protobuf.FieldMask":
		return &asyncapi.Schema{Type: "string", Description: "Comma separated list of field paths in lowerCamelCase."}
	case "google.protobuf.Struct":
		return &asyncapi.Schema{Type: "object", AdditionalProperties: true}
	case "google.protobuf.Value":
		return &asyncapi.Schema{}
	case "google.protobuf.ListValue":
		return &asyncapi.Schema{Type: "array", Items: &asyncapi.Schema{}}
	case "google.protobuf.Empty":
		return &asyncapi.Schema{Type: "object"}
	case "google.protobuf.Any":
		return &asyncapi.Schema{
			Type: "object",
			Properties: func() *asyncapi.Map[*asyncapi.Schema] {
				p := asyncapi.NewMap[*asyncapi.Schema]()
				p.Set("@type", &asyncapi.Schema{Type: "string", Format: "uri-reference"})
				return p
			}(),
			Required:             []string{"@type"},
			AdditionalProperties: true,
		}
	case "google.protobuf.StringValue":
		return nullable("string", "")
	case "google.protobuf.BytesValue":
		s := nullable("string", "byte")
		s.ContentEncoding = "base64"
		return s
	case "google.protobuf.BoolValue":
		return nullable("boolean", "")
	case "google.protobuf.Int32Value":
		return nullable("integer", "int32")
	case "google.protobuf.UInt32Value":
		return nullable("integer", "uint32")
	case "google.protobuf.Int64Value":
		return nullable("string", "int64")
	case "google.protobuf.UInt64Value":
		return nullable("string", "uint64")
	case "google.protobuf.FloatValue":
		return nullable("number", "float")
	case "google.protobuf.DoubleValue":
		return nullable("number", "double")
	}
	return nil
}

// autoExample composes a payload example from field examples, or returns
// nil when no field has one.
func (g *schemaGen) autoExample(m *protogen.Message) (*asyncapi.Map[any], error) {
	var out *asyncapi.Map[any]
	for _, f := range m.Fields {
		fo := fieldOptions(f)
		ex := fo.GetExamples()
		if len(ex) == 0 || fo.GetHidden() {
			continue
		}
		var v any
		if err := json.Unmarshal([]byte(ex[0]), &v); err != nil {
			return nil, Errorf(f.Desc, "example is not valid JSON: %v", err)
		}
		if out == nil {
			out = asyncapi.NewMap[any]()
		}
		out.Set(g.fieldName(f), v)
	}
	return out, nil
}
