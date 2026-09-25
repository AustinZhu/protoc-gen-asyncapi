package jsonschema

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/ordered"
)

// DefaultRefPrefix is where referenced schemas live in an AsyncAPI document.
const DefaultRefPrefix = "#/components/schemas/"

// Options configure a Converter.
type Options struct {
	// RefPrefix is prepended to a schema name to build a $ref. Defaults to DefaultRefPrefix.
	RefPrefix string
	// UseProtoNames names properties after the .proto field names instead of
	// their lowerCamelCase JSON names (protojson's UseProtoNames).
	UseProtoNames bool
	// Protovalidate, when non-nil, enables translating buf.validate field
	// constraints into JSON Schema keywords. It must be able to resolve the
	// buf.validate.* extensions, e.g. a dynamicpb.Types built from the files
	// in the CodeGeneratorRequest. If it cannot, enrichment is silently skipped.
	Protovalidate protoregistry.ExtensionTypeResolver
}

// Converter turns descriptors into schemas, collecting every named message
// and enum schema it references so they can be emitted once under
// components.schemas and referenced by $ref.
type Converter struct {
	opts     Options
	validate *validator
	defs     map[string]*Schema
}

// NewConverter returns a Converter with the given options.
func NewConverter(opts Options) *Converter {
	if opts.RefPrefix == "" {
		opts.RefPrefix = DefaultRefPrefix
	}
	return &Converter{opts: opts, validate: newValidator(opts.Protovalidate), defs: map[string]*Schema{}}
}

// Name returns the components.schemas key for a message or enum: its fully
// qualified proto name (e.g. "acme.orders.v1.Order"). Dots are legal in
// AsyncAPI component keys and need no escaping in a JSON pointer.
func Name(d protoreflect.Descriptor) string { return string(d.FullName()) }

// Ref returns a schema that references the named schema for d, registering
// d's schema (and everything it references) in the Converter's definitions.
// Well-known types are returned inline rather than by reference.
func (c *Converter) Ref(d protoreflect.Descriptor) *Schema {
	switch d := d.(type) {
	case protoreflect.MessageDescriptor:
		if wkt := wellKnown(d); wkt != nil {
			return wkt
		}
		c.Message(d)
	case protoreflect.EnumDescriptor:
		if d.FullName() == "google.protobuf.NullValue" {
			return &Schema{Type: "null"}
		}
		c.Enum(d)
	}
	return &Schema{Ref: c.opts.RefPrefix + Name(d)}
}

// Definitions returns every named schema registered so far, keyed by Name.
func (c *Converter) Definitions() map[string]*Schema { return c.defs }

// SortedNames returns the keys of Definitions in lexical order.
func (c *Converter) SortedNames() []string {
	names := make([]string, 0, len(c.defs))
	for n := range c.defs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// MessageSchema converts md and returns its own (unreferenced) schema together
// with every named schema it transitively references, keyed by Name.
func MessageSchema(md protoreflect.MessageDescriptor, opts Options) (*Schema, map[string]*Schema) {
	c := NewConverter(opts)
	s := c.Message(md)
	defs := map[string]*Schema{}
	for k, v := range c.defs {
		if k != Name(md) {
			defs[k] = v
		}
	}
	return s, defs
}

// Message returns the schema for md itself and registers it in Definitions.
func (c *Converter) Message(md protoreflect.MessageDescriptor) *Schema {
	if wkt := wellKnown(md); wkt != nil {
		return wkt
	}
	name := Name(md)
	if s, ok := c.defs[name]; ok {
		return s
	}
	s := &Schema{Type: "object", Title: string(md.Name()), Description: comments(md)}
	// Register before descending so recursive message types terminate.
	c.defs[name] = s

	props := &ordered.Map[*Schema]{}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		fs, required := c.field(fd)
		props.Set(c.propertyName(fd), fs)
		if required {
			s.Required = append(s.Required, c.propertyName(fd))
		}
	}
	if props.Len() > 0 {
		s.Properties = props
	}

	var groups []*Schema
	oneofs := md.Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		if g := c.oneof(oneofs.Get(i)); g != nil {
			groups = append(groups, g)
		}
	}
	switch len(groups) {
	case 0:
	case 1:
		s.OneOf = groups[0].OneOf
	default:
		s.AllOf = groups
	}
	return s
}

// Enum returns the schema for ed and registers it in Definitions. Enums are
// encoded by value name in protobuf JSON.
func (c *Converter) Enum(ed protoreflect.EnumDescriptor) *Schema {
	name := Name(ed)
	if s, ok := c.defs[name]; ok {
		return s
	}
	s := &Schema{Type: "string", Title: string(ed.Name())}
	var docs []string
	values := ed.Values()
	for i := 0; i < values.Len(); i++ {
		v := values.Get(i)
		s.Enum = append(s.Enum, string(v.Name()))
		if doc := comments(v); doc != "" {
			docs = append(docs, fmt.Sprintf("- `%s`: %s", v.Name(), strings.ReplaceAll(doc, "\n", " ")))
		}
	}
	s.Description = comments(ed)
	if len(docs) > 0 {
		if s.Description != "" {
			s.Description += "\n\n"
		}
		s.Description += strings.Join(docs, "\n")
	}
	c.defs[name] = s
	return s
}

func (c *Converter) propertyName(fd protoreflect.FieldDescriptor) string {
	if c.opts.UseProtoNames {
		return string(fd.Name())
	}
	return fd.JSONName()
}

// field returns the schema of a field and whether it is required.
func (c *Converter) field(fd protoreflect.FieldDescriptor) (*Schema, bool) {
	var s *Schema
	switch {
	case fd.IsMap():
		s = &Schema{Type: "object", AdditionalProps: c.singular(fd.MapValue())}
		if pn := mapKeyNames(fd.MapKey()); pn != nil {
			s.PropertyNames = pn
		}
	case fd.IsList():
		s = &Schema{Type: "array", Items: c.singular(fd)}
	default:
		s = c.singular(fd)
		// Scalar, enum and well-known schemas may be shared; decorate a copy.
		s = s.clone()
	}
	if doc := comments(fd); doc != "" {
		s.Description = doc
	}
	if opts, ok := fd.Options().(interface{ GetDeprecated() bool }); ok && opts.GetDeprecated() {
		s.Deprecated = true
	}
	required := fd.Cardinality() == protoreflect.Required
	if c.validate.apply(fd, s) {
		required = true
	}
	return s, required
}

// singular returns the schema for one value of fd's type, ignoring cardinality.
func (c *Converter) singular(fd protoreflect.FieldDescriptor) *Schema {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return c.Ref(fd.Message())
	case protoreflect.EnumKind:
		return c.Ref(fd.Enum())
	}
	return scalar(fd.Kind())
}

// oneof returns a schema whose OneOf lists one alternative per member of od.
// A oneof may be left unset in proto, so unless protovalidate marks it
// required an extra alternative matches "none of the members present".
func (c *Converter) oneof(od protoreflect.OneofDescriptor) *Schema {
	if od.IsSynthetic() {
		return nil // proto3 `optional`: presence tracking only, no schema impact.
	}
	fields := od.Fields()
	var alts []*Schema
	for i := 0; i < fields.Len(); i++ {
		alts = append(alts, &Schema{Required: []string{c.propertyName(fields.Get(i))}})
	}
	g := &Schema{OneOf: alts}
	if !c.validate.oneofRequired(od) {
		none := make([]*Schema, len(alts))
		for i, a := range alts {
			none[i] = &Schema{Required: a.Required}
		}
		g.OneOf = append(g.OneOf, &Schema{Not: &Schema{AnyOf: none}})
	}
	return g
}

func mapKeyNames(fd protoreflect.FieldDescriptor) *Schema {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return nil
	case protoreflect.BoolKind:
		return &Schema{Enum: []any{"true", "false"}}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return &Schema{Pattern: `^[0-9]+$`}
	default:
		return &Schema{Pattern: `^-?[0-9]+$`}
	}
}

func uint64p(v uint64) *uint64 { return &v }

// scalar maps a scalar kind to its protobuf JSON schema. 64-bit integers are
// emitted as strings by protobuf JSON (numbers are accepted on input), so both
// forms are allowed.
func scalar(k protoreflect.Kind) *Schema {
	switch k {
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return &Schema{Type: "number"}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return &Schema{Type: "integer", Format: "int32"}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return &Schema{Type: "integer", Format: "uint32", Minimum: int64(0)}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return &Schema{Type: []string{"integer", "string"}, Format: "int64", Pattern: `^-?[0-9]+$`}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return &Schema{Type: []string{"integer", "string"}, Format: "uint64", Pattern: `^[0-9]+$`, Minimum: int64(0)}
	case protoreflect.BoolKind:
		return &Schema{Type: "boolean"}
	case protoreflect.StringKind:
		return &Schema{Type: "string"}
	case protoreflect.BytesKind:
		return &Schema{Type: "string", ContentEncoding: "base64"}
	}
	return &Schema{}
}

// wellKnown returns the special-cased schema for a google.protobuf well-known
// type, whose protobuf JSON encoding differs from its structure, or nil.
func wellKnown(md protoreflect.MessageDescriptor) *Schema {
	switch md.FullName() {
	case "google.protobuf.Timestamp":
		return &Schema{Type: "string", Format: "date-time"}
	case "google.protobuf.Duration":
		return &Schema{Type: "string", Pattern: `^-?[0-9]+(\.[0-9]+)?s$`}
	case "google.protobuf.Empty":
		return &Schema{Type: "object", AdditionalProps: false}
	case "google.protobuf.Struct":
		return &Schema{Type: "object"}
	case "google.protobuf.Value":
		return &Schema{}
	case "google.protobuf.ListValue":
		return &Schema{Type: "array"}
	case "google.protobuf.FieldMask":
		return &Schema{Type: "string", Description: "Comma-separated field paths in lowerCamelCase."}
	case "google.protobuf.Any":
		return &Schema{
			Type:            "object",
			Properties:      anyProperties(),
			Required:        []string{"@type"},
			AdditionalProps: true,
		}
	case "google.protobuf.DoubleValue", "google.protobuf.FloatValue":
		return nullable(scalar(protoreflect.DoubleKind))
	case "google.protobuf.Int32Value":
		return nullable(scalar(protoreflect.Int32Kind))
	case "google.protobuf.UInt32Value":
		return nullable(scalar(protoreflect.Uint32Kind))
	case "google.protobuf.Int64Value":
		return nullable(scalar(protoreflect.Int64Kind))
	case "google.protobuf.UInt64Value":
		return nullable(scalar(protoreflect.Uint64Kind))
	case "google.protobuf.BoolValue":
		return nullable(scalar(protoreflect.BoolKind))
	case "google.protobuf.StringValue":
		return nullable(scalar(protoreflect.StringKind))
	case "google.protobuf.BytesValue":
		return nullable(scalar(protoreflect.BytesKind))
	}
	return nil
}

func anyProperties() *ordered.Map[*Schema] {
	m := &ordered.Map[*Schema]{}
	m.Set("@type", &Schema{Type: "string", Description: "Type URL of the packed message, e.g. type.googleapis.com/acme.v1.Order."})
	return m
}

func nullable(s *Schema) *Schema {
	switch t := s.Type.(type) {
	case string:
		s.Type = []string{t, "null"}
	case []string:
		s.Type = append(t, "null")
	}
	return s
}

// comments returns the documentation attached to d in its file's
// SourceCodeInfo: the leading comment, falling back to the trailing one.
func comments(d protoreflect.Descriptor) string {
	loc := d.ParentFile().SourceLocations().ByDescriptor(d)
	doc := loc.LeadingComments
	if strings.TrimSpace(doc) == "" {
		doc = loc.TrailingComments
	}
	return CleanComment(doc)
}

// CleanComment normalizes a raw proto comment: trims the conventional single
// leading space of each line and surrounding blank lines.
func CleanComment(raw string) string {
	lines := strings.Split(strings.TrimRight(raw, "\n "), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(strings.TrimPrefix(l, " "), " \t")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// Comments exposes the documentation lookup to other packages.
func Comments(d protoreflect.Descriptor) string { return comments(d) }
