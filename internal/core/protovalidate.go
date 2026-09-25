package core

import (
	"fmt"
	"regexp"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
)

// validateRules translates protovalidate (buf.validate) constraints into
// JSON Schema keywords. The plugin does not link the protovalidate Go types:
// the rules are decoded dynamically from the descriptors in the request, so
// they are honoured whenever the input imports buf/validate/validate.proto.
type validateRules struct {
	fieldExt protoreflect.ExtensionType
	oneofExt protoreflect.ExtensionType
	types    *protoregistry.Types
}

func newValidateRules(plugin *protogen.Plugin) *validateRules {
	v := &validateRules{types: new(protoregistry.Types)}
	for _, f := range plugin.Files {
		if f.Desc.Package() != "buf.validate" {
			continue
		}
		exts := f.Desc.Extensions()
		for i := 0; i < exts.Len(); i++ {
			xd := exts.Get(i)
			var dst *protoreflect.ExtensionType
			switch {
			case xd.FullName() == "buf.validate.field":
				dst = &v.fieldExt
			case xd.FullName() == "buf.validate.oneof":
				dst = &v.oneofExt
			default:
				continue
			}
			xt := dynamicpb.NewExtensionType(xd)
			if err := v.types.RegisterExtension(xt); err == nil {
				*dst = xt
			}
		}
	}
	return v
}

// lookup returns the message value of the extension named name in opts.
func (v *validateRules) lookup(opts proto.Message, xt protoreflect.ExtensionType, name protoreflect.FullName) protoreflect.Message {
	if opts == nil {
		return nil
	}
	var found protoreflect.Message
	opts.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		if fd.FullName() == name && fd.Message() != nil {
			found = val.Message()
			return false
		}
		return true
	})
	if found != nil || xt == nil || len(opts.ProtoReflect().GetUnknown()) == 0 {
		return found
	}
	raw, err := proto.Marshal(opts)
	if err != nil {
		return nil
	}
	fresh := opts.ProtoReflect().New().Interface()
	if err := (proto.UnmarshalOptions{Resolver: v.types}).Unmarshal(raw, fresh); err != nil {
		return nil
	}
	if !fresh.ProtoReflect().Has(xt.TypeDescriptor()) {
		return nil
	}
	return fresh.ProtoReflect().Get(xt.TypeDescriptor()).Message()
}

func (v *validateRules) oneofRequired(o protoreflect.OneofDescriptor) bool {
	r := v.lookup(o.Options(), v.oneofExt, "buf.validate.oneof")
	if r == nil {
		return false
	}
	b, _ := field(r, "required")
	return b.IsValid() && b.Bool()
}

// apply adds the constraints of f to s and reports whether f is required.
func (v *validateRules) apply(f *protogen.Field, s *asyncapi.Schema) (bool, error) {
	r := v.lookup(f.Desc.Options(), v.fieldExt, "buf.validate.field")
	if r == nil {
		return false, nil
	}
	required := false
	if b, ok := field(r, "required"); ok && b.Bool() {
		required = true
	}
	switch {
	case f.Desc.IsMap():
		if m, ok := message(r, "map"); ok {
			s.MinProperties = uintField(m, "min_pairs")
			s.MaxProperties = uintField(m, "max_pairs")
			if keys, ok := message(m, "keys"); ok && s.PropertyNames == nil {
				s.PropertyNames = &asyncapi.Schema{}
				v.applyTyped(keys, s.PropertyNames, protoreflect.StringKind, f.Message.Fields[0])
			}
			if vals, ok := message(m, "values"); ok {
				if as, ok := s.AdditionalProperties.(*asyncapi.Schema); ok {
					v.applyTyped(vals, as, f.Message.Fields[1].Desc.Kind(), f.Message.Fields[1])
				}
			}
		}
	case f.Desc.IsList():
		if m, ok := message(r, "repeated"); ok {
			s.MinItems = uintField(m, "min_items")
			s.MaxItems = uintField(m, "max_items")
			if u, ok := field(m, "unique"); ok && u.Bool() {
				s.UniqueItems = true
			}
			if items, ok := message(m, "items"); ok {
				if is, ok := s.Items.(*asyncapi.Schema); ok {
					v.applyTyped(items, is, f.Desc.Kind(), f)
				}
			}
		}
	default:
		v.applyTyped(r, s, f.Desc.Kind(), f)
	}
	return required, nil
}

// applyTyped applies the type specific rules (string, int32, enum, ...) of
// a FieldRules message.
func (v *validateRules) applyTyped(r protoreflect.Message, s *asyncapi.Schema, kind protoreflect.Kind, f *protogen.Field) {
	oneof := r.Descriptor().Oneofs().ByName("type")
	if oneof == nil {
		return
	}
	fd := r.WhichOneof(oneof)
	if fd == nil || fd.Message() == nil {
		return
	}
	rules := r.Get(fd).Message()
	if s.Ref != "" {
		// Constraints next to $ref would be ignored; wrap the reference.
		ref := *s
		*s = asyncapi.Schema{AllOf: []*asyncapi.Schema{&ref}}
	}
	switch fd.Name() {
	case "string":
		applyString(rules, s)
	case "enum":
		applyEnum(rules, s, f)
	case "float", "double", "int32", "uint32", "sint32", "fixed32", "sfixed32":
		applyNumber(rules, s, false)
	case "int64", "uint64", "sint64", "fixed64", "sfixed64":
		applyNumber(rules, s, true)
	case "bool":
		if c, ok := field(rules, "const"); ok {
			s.Const = c.Bool()
		}
	}
}

func applyString(r protoreflect.Message, s *asyncapi.Schema) {
	if c, ok := field(r, "const"); ok {
		s.Const = c.String()
	}
	if n := uintField(r, "len"); n != nil {
		s.MinLength, s.MaxLength = n, n
	}
	if n := uintField(r, "min_len"); n != nil {
		s.MinLength = n
	}
	if n := uintField(r, "max_len"); n != nil {
		s.MaxLength = n
	}
	if p, ok := field(r, "pattern"); ok {
		s.Pattern = p.String()
	} else if p, ok := field(r, "prefix"); ok {
		s.Pattern = "^" + regexp.QuoteMeta(p.String())
	} else if p, ok := field(r, "suffix"); ok {
		s.Pattern = regexp.QuoteMeta(p.String()) + "$"
	} else if p, ok := field(r, "contains"); ok {
		s.Pattern = regexp.QuoteMeta(p.String())
	}
	if in := listField(r, "in"); len(in) > 0 {
		s.Enum = nil
		for _, e := range in {
			s.Enum = append(s.Enum, e.String())
		}
	}
	if in := listField(r, "not_in"); len(in) > 0 {
		not := &asyncapi.Schema{}
		for _, e := range in {
			not.Enum = append(not.Enum, e.String())
		}
		s.Not = not
	}
	formats := map[protoreflect.Name]string{
		"email": "email", "hostname": "hostname", "ipv4": "ipv4", "ipv6": "ipv6",
		"uri": "uri", "uri_ref": "uri-reference", "uuid": "uuid",
	}
	if wk := r.Descriptor().Oneofs().ByName("well_known"); wk != nil {
		if fd := r.WhichOneof(wk); fd != nil && r.Get(fd).Bool() {
			if format, ok := formats[fd.Name()]; ok {
				s.Format = format
			} else if fd.Name() == "tuuid" {
				s.Pattern = "^[0-9a-fA-F]{32}$"
			}
		}
	}
	for _, e := range listField(r, "example") {
		s.Examples = append(s.Examples, e.String())
	}
}

func applyEnum(r protoreflect.Message, s *asyncapi.Schema, f *protogen.Field) {
	if f == nil || f.Enum == nil {
		return
	}
	name := func(n protoreflect.EnumNumber) any {
		if v := f.Enum.Desc.Values().ByNumber(n); v != nil {
			return string(v.Name())
		}
		return int64(n)
	}
	if c, ok := field(r, "const"); ok {
		s.Const = name(protoreflect.EnumNumber(c.Int()))
	}
	if in := listField(r, "in"); len(in) > 0 {
		for _, e := range in {
			s.Enum = append(s.Enum, name(protoreflect.EnumNumber(e.Int())))
		}
	}
	if in := listField(r, "not_in"); len(in) > 0 {
		not := &asyncapi.Schema{}
		for _, e := range in {
			not.Enum = append(not.Enum, name(protoreflect.EnumNumber(e.Int())))
		}
		s.Not = not
	}
}

func applyNumber(r protoreflect.Message, s *asyncapi.Schema, asString bool) {
	conv := func(v protoreflect.Value) any {
		x := v.Interface()
		if asString {
			return fmt.Sprint(x)
		}
		return x
	}
	if c, ok := field(r, "const"); ok {
		s.Const = conv(c)
	}
	if in := listField(r, "in"); len(in) > 0 {
		for _, e := range in {
			s.Enum = append(s.Enum, conv(e))
		}
	}
	if in := listField(r, "not_in"); len(in) > 0 {
		not := &asyncapi.Schema{}
		for _, e := range in {
			not.Enum = append(not.Enum, conv(e))
		}
		s.Not = not
	}
	if !asString {
		// Range rules cannot be expressed on the string encoding of 64-bit
		// integers. protovalidate treats an upper bound below the lower
		// bound as an exclusive ("outside") range, which JSON Schema bounds
		// cannot express either.
		lo, hasLo := firstField(r, "gt", "gte")
		hi, hasHi := firstField(r, "lt", "lte")
		if hasLo && hasHi && toFloat(hi.Interface()) < toFloat(lo.Interface()) {
			hasLo, hasHi = false, false
		}
		if hasLo {
			if x, ok := field(r, "gt"); ok {
				s.ExclusiveMinimum = x.Interface()
				if s.Minimum == 0 {
					s.Minimum = nil // uint32 lower bound superseded
				}
			} else if x, ok := field(r, "gte"); ok {
				s.Minimum = x.Interface()
			}
		}
		if hasHi {
			if x, ok := field(r, "lt"); ok {
				s.ExclusiveMaximum = x.Interface()
			} else if x, ok := field(r, "lte"); ok {
				s.Maximum = x.Interface()
			}
		}
	}
	for _, e := range listField(r, "example") {
		s.Examples = append(s.Examples, conv(e))
	}
}

// field returns a populated field of a dynamic message by name.
func field(m protoreflect.Message, name protoreflect.Name) (protoreflect.Value, bool) {
	fd := m.Descriptor().Fields().ByName(name)
	if fd == nil || fd.IsList() || !m.Has(fd) {
		return protoreflect.Value{}, false
	}
	return m.Get(fd), true
}

func message(m protoreflect.Message, name protoreflect.Name) (protoreflect.Message, bool) {
	fd := m.Descriptor().Fields().ByName(name)
	if fd == nil || fd.Message() == nil || !m.Has(fd) {
		return nil, false
	}
	return m.Get(fd).Message(), true
}

func listField(m protoreflect.Message, name protoreflect.Name) []protoreflect.Value {
	fd := m.Descriptor().Fields().ByName(name)
	if fd == nil || !fd.IsList() || !m.Has(fd) {
		return nil
	}
	l := m.Get(fd).List()
	out := make([]protoreflect.Value, l.Len())
	for i := range out {
		out[i] = l.Get(i)
	}
	return out
}

func uintField(m protoreflect.Message, name protoreflect.Name) *uint64 {
	v, ok := field(m, name)
	if !ok {
		return nil
	}
	n := v.Uint()
	return &n
}

// Values of google.api.FieldBehavior.
const (
	behaviorRequired   = 2
	behaviorOutputOnly = 3
	behaviorInputOnly  = 4
)

// fieldBehaviors returns the google.api.field_behavior values of a field,
// whether or not google/api/field_behavior.proto is linked in.
func fieldBehaviors(fd protoreflect.FieldDescriptor) map[uint64]bool {
	const fieldBehavior = 1052
	out := map[uint64]bool{}
	opts := fd.Options()
	if opts == nil {
		return out
	}
	opts.ProtoReflect().Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.FullName() == "google.api.field_behavior" && f.IsList() {
			for i := 0; i < v.List().Len(); i++ {
				out[uint64(v.List().Get(i).Enum())] = true
			}
		}
		return true
	})
	b := opts.ProtoReflect().GetUnknown()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return out
		}
		b = b[n:]
		switch {
		case num == fieldBehavior && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return out
			}
			out[v] = true
			b = b[n:]
		case num == fieldBehavior && typ == protowire.BytesType:
			packed, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return out
			}
			for len(packed) > 0 {
				v, m := protowire.ConsumeVarint(packed)
				if m < 0 {
					break
				}
				out[v] = true
				packed = packed[m:]
			}
			b = b[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return out
			}
			b = b[n:]
		}
	}
	return out
}

func firstField(m protoreflect.Message, names ...protoreflect.Name) (protoreflect.Value, bool) {
	for _, n := range names {
		if v, ok := field(m, n); ok {
			return v, true
		}
	}
	return protoreflect.Value{}, false
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case float32:
		return float64(x)
	case float64:
		return x
	}
	return 0
}
