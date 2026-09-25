package generator

import (
	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/asyncapi"
	"math"
	"strconv"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// validator translates protovalidate (buf.validate) constraints into JSON
// Schema keywords. It reads the extensions dynamically through the resolver it
// is given, so the plugin needs no compiled-in protovalidate dependency and
// works unchanged on files that never import buf/validate/validate.proto.
// A nil validator (enrichment off) is valid and applies nothing.
type validator struct {
	resolver protoregistry.ExtensionTypeResolver
	field    protoreflect.ExtensionType
	oneof    protoreflect.ExtensionType
}

func newValidator(r protoregistry.ExtensionTypeResolver) *validator {
	if r == nil {
		return nil
	}
	field, err := r.FindExtensionByName("buf.validate.field")
	if err != nil {
		return nil
	}
	oneof, _ := r.FindExtensionByName("buf.validate.oneof")
	return &validator{resolver: r, field: field, oneof: oneof}
}

// extension re-parses opts with the resolver so the dynamically-typed
// extension becomes readable, and returns it, or nil if unset.
func (v *validator) extension(opts proto.Message, xt protoreflect.ExtensionType) protoreflect.Message {
	if xt == nil || opts == nil {
		return nil
	}
	b, err := proto.Marshal(opts)
	if err != nil || len(b) == 0 {
		return nil
	}
	fresh := opts.ProtoReflect().New()
	if err := (proto.UnmarshalOptions{Resolver: v.resolver}).Unmarshal(b, fresh.Interface()); err != nil {
		return nil
	}
	if !fresh.Has(xt.TypeDescriptor()) {
		return nil
	}
	return fresh.Get(xt.TypeDescriptor()).Message()
}

func (v *validator) oneofRequired(od protoreflect.OneofDescriptor) bool {
	if v == nil {
		return false
	}
	rules := v.extension(od.Options(), v.oneof)
	return rules != nil && getBool(rules, "required")
}

// apply decorates s (the schema of fd, including list/map wrapping) with fd's
// constraints and reports whether the field is required.
func (v *validator) apply(fd protoreflect.FieldDescriptor, s *asyncapi.Schema) bool {
	if v == nil {
		return false
	}
	rules := v.extension(fd.Options(), v.field)
	if rules == nil {
		return false
	}
	applyRules(rules, s)
	return getBool(rules, "required")
}

// applyRules applies one buf.validate.FieldRules message to s.
func applyRules(rules protoreflect.Message, s *asyncapi.Schema) {
	od := rules.Descriptor().Oneofs().ByName("type")
	if od == nil {
		return
	}
	set := rules.WhichOneof(od)
	if set == nil {
		return
	}
	typed := rules.Get(set).Message()
	switch set.Name() {
	case "string":
		applyString(typed, s)
	case "repeated":
		setUint(typed, "min_items", &s.MinItems)
		setUint(typed, "max_items", &s.MaxItems)
		if getBool(typed, "unique") {
			s.UniqueItems = true
		}
		if items := getMessage(typed, "items"); items != nil && s.Items != nil {
			s.Items = s.Items.Clone()
			applyRules(items, s.Items)
		}
	case "map":
		setUint(typed, "min_pairs", &s.MinProperties)
		setUint(typed, "max_pairs", &s.MaxProperties)
		if values := getMessage(typed, "values"); values != nil {
			if vs, ok := s.AdditionalProps.(*asyncapi.Schema); ok {
				vs = vs.Clone()
				applyRules(values, vs)
				s.AdditionalProps = vs
			}
		}
	case "float", "double", "int32", "int64", "uint32", "uint64",
		"sint32", "sint64", "fixed32", "fixed64", "sfixed32", "sfixed64":
		applyNumeric(typed, s)
	}
}

func applyString(r protoreflect.Message, s *asyncapi.Schema) {
	setUint(r, "min_len", &s.MinLength)
	setUint(r, "max_len", &s.MaxLength)
	if f := field(r, "len"); f != nil && r.Has(f) {
		n := r.Get(f).Uint()
		s.MinLength, s.MaxLength = &n, &n
	}
	if f := field(r, "pattern"); f != nil && r.Has(f) {
		s.Pattern = r.Get(f).String()
	}
	if f := field(r, "const"); f != nil && r.Has(f) {
		s.Const = r.Get(f).String()
	}
	if f := field(r, "in"); f != nil {
		list := r.Get(f).List()
		for i := 0; i < list.Len(); i++ {
			s.Enum = append(s.Enum, list.Get(i).String())
		}
	}
	formats := []struct {
		rule   protoreflect.Name
		format string
	}{
		{"email", "email"}, {"hostname", "hostname"}, {"ipv4", "ipv4"}, {"ipv6", "ipv6"},
		{"uri", "uri"}, {"uri_ref", "uri-reference"}, {"uuid", "uuid"},
	}
	for _, f := range formats {
		if getBool(r, f.rule) {
			s.Format = f.format
		}
	}
}

// applyNumeric maps gt/gte/lt/lte/const/in. protovalidate treats an upper
// bound below the lower bound as an exclusive (outside) range, which plain
// JSON Schema bounds cannot express; such ranges are left out.
func applyNumeric(r protoreflect.Message, s *asyncapi.Schema) {
	lower, lowerName := bound(r, "gt", "gte")
	upper, upperName := bound(r, "lt", "lte")
	if lower != nil && upper != nil && toFloat(upper) < toFloat(lower) {
		lower, upper = nil, nil
	}
	if lower != nil {
		if lowerName == "gt" {
			s.ExclusiveMinimum, s.Minimum = lower, nil
		} else {
			s.Minimum = lower
		}
	}
	if upper != nil {
		if upperName == "lt" {
			s.ExclusiveMaximum = upper
		} else {
			s.Maximum = upper
		}
	}
	if f := field(r, "const"); f != nil && r.Has(f) {
		s.Const = number(r.Get(f))
	}
	if f := field(r, "in"); f != nil {
		list := r.Get(f).List()
		for i := 0; i < list.Len(); i++ {
			s.Enum = append(s.Enum, number(list.Get(i)))
		}
	}
}

func bound(r protoreflect.Message, exclusive, inclusive protoreflect.Name) (any, protoreflect.Name) {
	for _, name := range []protoreflect.Name{exclusive, inclusive} {
		if f := field(r, name); f != nil && r.Has(f) {
			return number(r.Get(f)), name
		}
	}
	return nil, ""
}

// number normalizes a numeric protoreflect value to int64, uint64 or float64.
func number(v protoreflect.Value) any {
	switch x := v.Interface().(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	case uint32:
		return uint64(x)
	case uint64:
		return x
	case float32:
		// Round-trip through the shortest float32 representation so 0.1f
		// prints as 0.1 rather than 0.10000000149011612.
		f, _ := strconv.ParseFloat(strconv.FormatFloat(float64(x), 'g', -1, 32), 64)
		return f
	case float64:
		return x
	}
	return nil
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case uint64:
		return float64(x)
	case float64:
		return x
	}
	return math.NaN()
}

func field(m protoreflect.Message, name protoreflect.Name) protoreflect.FieldDescriptor {
	return m.Descriptor().Fields().ByName(name)
}

func getBool(m protoreflect.Message, name protoreflect.Name) bool {
	f := field(m, name)
	return f != nil && f.Kind() == protoreflect.BoolKind && m.Get(f).Bool()
}

func getMessage(m protoreflect.Message, name protoreflect.Name) protoreflect.Message {
	f := field(m, name)
	if f == nil || f.Message() == nil || !m.Has(f) {
		return nil
	}
	return m.Get(f).Message()
}

func setUint(m protoreflect.Message, name protoreflect.Name, dst **uint64) {
	if f := field(m, name); f != nil && m.Has(f) {
		*dst = uint64p(m.Get(f).Uint())
	}
}
