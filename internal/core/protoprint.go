package core

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// printProto renders the Protobuf source of a message for the
// `application/vnd.google.protobuf` schema format. The output contains the
// message first, followed by every declaration of the same package it
// depends on; types of other packages are imported. It returns the source
// and the schema format version ("2" or "3").
func printProto(m *protogen.Message) (string, string, error) {
	file := m.Desc.ParentFile()
	pkg := file.Package()

	var decls []protoreflect.Descriptor
	declared := map[protoreflect.FullName]bool{}
	imports := map[string]bool{}
	var queue []protoreflect.MessageDescriptor

	addType := func(d protoreflect.Descriptor) {
		top := d
		for {
			if _, ok := top.Parent().(protoreflect.FileDescriptor); ok {
				break
			}
			top = top.Parent()
		}
		if top.ParentFile().Package() != pkg || strings.HasPrefix(string(top.FullName()), "google.protobuf.") {
			if top.ParentFile().Path() != file.Path() {
				imports[top.ParentFile().Path()] = true
			}
			return
		}
		if declared[top.FullName()] {
			return
		}
		declared[top.FullName()] = true
		decls = append(decls, top)
		if md, ok := top.(protoreflect.MessageDescriptor); ok {
			queue = append(queue, md)
		}
	}
	addType(m.Desc)
	// Every declaration printed is printed in full, so walk all nested
	// messages to collect their dependencies.
	for len(queue) > 0 {
		md := queue[0]
		queue = queue[1:]
		fields := md.Fields()
		for i := 0; i < fields.Len(); i++ {
			f := fields.Get(i)
			if f.Message() != nil {
				if f.Message().IsMapEntry() {
					queue = append(queue, f.Message())
				} else {
					addType(f.Message())
				}
			}
			if f.Enum() != nil {
				addType(f.Enum())
			}
		}
		nested := md.Messages()
		for i := 0; i < nested.Len(); i++ {
			queue = append(queue, nested.Get(i))
		}
	}

	p := &protoPrinter{pkg: pkg, file: file}
	version := "3"
	switch file.Syntax() {
	case protoreflect.Proto2:
		version = "2"
		p.line(0, `syntax = "proto2";`)
	case protoreflect.Proto3:
		p.line(0, `syntax = "proto3";`)
	default:
		edition := strings.TrimPrefix(protodesc.ToFileDescriptorProto(file).GetEdition().String(), "EDITION_")
		p.line(0, fmt.Sprintf("edition = %q;", edition))
	}
	if pkg != "" {
		p.line(0, "")
		p.line(0, fmt.Sprintf("package %s;", pkg))
	}
	if features := editionFeatures(file.Options()); len(features) > 0 {
		p.line(0, "")
		for _, f := range features {
			p.line(0, "option "+f+";")
		}
	}
	if len(imports) > 0 {
		p.line(0, "")
		paths := make([]string, 0, len(imports))
		for path := range imports {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			p.line(0, fmt.Sprintf("import %q;", path))
		}
	}
	for _, d := range decls {
		p.line(0, "")
		switch d := d.(type) {
		case protoreflect.MessageDescriptor:
			p.message(d, 0)
		case protoreflect.EnumDescriptor:
			p.enum(d, 0)
		}
	}
	return p.b.String(), version, nil
}

type protoPrinter struct {
	b    strings.Builder
	pkg  protoreflect.FullName
	file protoreflect.FileDescriptor
}

func (p *protoPrinter) line(indent int, s string) {
	if s != "" {
		p.b.WriteString(strings.Repeat("  ", indent))
		p.b.WriteString(s)
	}
	p.b.WriteByte('\n')
}

func (p *protoPrinter) comments(d protoreflect.Descriptor, indent int) {
	loc := d.ParentFile().SourceLocations().ByDescriptor(d)
	text := strings.TrimRight(loc.LeadingComments, "\n ")
	if text == "" {
		return
	}
	for _, l := range strings.Split(text, "\n") {
		p.line(indent, strings.TrimRight("//"+l, " "))
	}
}

// typeName returns the name to use for a type reference.
func (p *protoPrinter) typeName(d protoreflect.Descriptor) string {
	name := string(d.FullName())
	if p.pkg != "" && strings.HasPrefix(name, string(p.pkg)+".") {
		return strings.TrimPrefix(name, string(p.pkg)+".")
	}
	return name
}

func (p *protoPrinter) fieldType(f protoreflect.FieldDescriptor) string {
	switch f.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return p.typeName(f.Message())
	case protoreflect.EnumKind:
		return p.typeName(f.Enum())
	}
	return f.Kind().String()
}

func (p *protoPrinter) message(md protoreflect.MessageDescriptor, indent int) {
	p.comments(md, indent)
	p.line(indent, fmt.Sprintf("message %s {", md.Name()))
	in := indent + 1
	if isDeprecated(md) {
		p.line(in, "option deprecated = true;")
	}
	for _, f := range editionFeatures(md.Options()) {
		p.line(in, "option "+f+";")
	}

	printed := map[protoreflect.OneofDescriptor]bool{}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if o := f.ContainingOneof(); o != nil && !o.IsSynthetic() {
			if printed[o] {
				continue
			}
			printed[o] = true
			p.comments(o, in)
			p.line(in, fmt.Sprintf("oneof %s {", o.Name()))
			for j := 0; j < o.Fields().Len(); j++ {
				p.field(o.Fields().Get(j), in+1, true)
			}
			p.line(in, "}")
			continue
		}
		p.field(f, in, false)
	}
	p.reserved(md.ReservedRanges(), md.ReservedNames(), in)
	ranges := md.ExtensionRanges()
	for i := 0; i < ranges.Len(); i++ {
		r := ranges.Get(i)
		p.line(in, fmt.Sprintf("extensions %s;", rangeString(int64(r[0]), int64(r[1])-1, 536870911)))
	}

	nested := md.Messages()
	for i := 0; i < nested.Len(); i++ {
		if n := nested.Get(i); !n.IsMapEntry() {
			p.line(0, "")
			p.message(n, in)
		}
	}
	enums := md.Enums()
	for i := 0; i < enums.Len(); i++ {
		p.line(0, "")
		p.enum(enums.Get(i), in)
	}
	p.line(indent, "}")
}

func (p *protoPrinter) field(f protoreflect.FieldDescriptor, indent int, inOneof bool) {
	p.comments(f, indent)
	var label string
	switch {
	case inOneof:
	case f.IsMap():
	case f.IsList():
		label = "repeated "
	case p.file.Syntax() == protoreflect.Proto2 && f.Cardinality() == protoreflect.Required:
		label = "required "
	case p.file.Syntax() == protoreflect.Proto2:
		label = "optional "
	case p.file.Syntax() == protoreflect.Proto3 && f.HasOptionalKeyword():
		label = "optional "
	}
	typ := p.fieldType(f)
	if f.IsMap() {
		typ = fmt.Sprintf("map<%s, %s>", p.fieldType(f.MapKey()), p.fieldType(f.MapValue()))
	}
	var opts []string
	if f.HasDefault() && p.file.Syntax() == protoreflect.Proto2 {
		opts = append(opts, "default = "+defaultValue(f))
	}
	if isDeprecated(f) {
		opts = append(opts, "deprecated = true")
	}
	opts = append(opts, editionFeatures(f.Options())...)
	suffix := ""
	if len(opts) > 0 {
		suffix = " [" + strings.Join(opts, ", ") + "]"
	}
	p.line(indent, fmt.Sprintf("%s%s %s = %d%s;", label, typ, f.Name(), f.Number(), suffix))
}

func defaultValue(f protoreflect.FieldDescriptor) string {
	v := f.Default()
	switch f.Kind() {
	case protoreflect.StringKind:
		return strconv.Quote(v.String())
	case protoreflect.BytesKind:
		return strconv.Quote(string(v.Bytes()))
	case protoreflect.EnumKind:
		return string(f.DefaultEnumValue().Name())
	}
	return fmt.Sprint(v.Interface())
}

func (p *protoPrinter) enum(ed protoreflect.EnumDescriptor, indent int) {
	p.comments(ed, indent)
	p.line(indent, fmt.Sprintf("enum %s {", ed.Name()))
	in := indent + 1
	values := ed.Values()
	numbers := map[protoreflect.EnumNumber]bool{}
	for _, f := range editionFeatures(ed.Options()) {
		p.line(in, "option "+f+";")
	}
	for i := 0; i < values.Len(); i++ {
		n := values.Get(i).Number()
		if numbers[n] {
			p.line(in, "option allow_alias = true;")
			break
		}
		numbers[n] = true
	}
	for i := 0; i < values.Len(); i++ {
		v := values.Get(i)
		p.comments(v, in)
		suffix := ""
		if isDeprecated(v) {
			suffix = " [deprecated = true]"
		}
		p.line(in, fmt.Sprintf("%s = %d%s;", v.Name(), v.Number(), suffix))
	}
	p.reservedEnum(ed, in)
	p.line(indent, "}")
}

func (p *protoPrinter) reserved(ranges protoreflect.FieldRanges, names protoreflect.Names, indent int) {
	if ranges.Len() > 0 {
		var parts []string
		for i := 0; i < ranges.Len(); i++ {
			r := ranges.Get(i)
			parts = append(parts, rangeString(int64(r[0]), int64(r[1])-1, 536870911))
		}
		p.line(indent, "reserved "+strings.Join(parts, ", ")+";")
	}
	p.reservedNames(names, indent)
}

func (p *protoPrinter) reservedEnum(ed protoreflect.EnumDescriptor, indent int) {
	ranges := ed.ReservedRanges()
	if ranges.Len() > 0 {
		var parts []string
		for i := 0; i < ranges.Len(); i++ {
			r := ranges.Get(i)
			parts = append(parts, rangeString(int64(r[0]), int64(r[1]), 2147483647))
		}
		p.line(indent, "reserved "+strings.Join(parts, ", ")+";")
	}
	p.reservedNames(ed.ReservedNames(), indent)
}

func (p *protoPrinter) reservedNames(names protoreflect.Names, indent int) {
	if names.Len() == 0 {
		return
	}
	var parts []string
	for i := 0; i < names.Len(); i++ {
		if p.file.Syntax() == protoreflect.Editions {
			parts = append(parts, string(names.Get(i)))
		} else {
			parts = append(parts, strconv.Quote(string(names.Get(i))))
		}
	}
	p.line(indent, "reserved "+strings.Join(parts, ", ")+";")
}

func rangeString(start, end, max int64) string {
	switch {
	case start == end:
		return strconv.FormatInt(start, 10)
	case end >= max:
		return fmt.Sprintf("%d to max", start)
	default:
		return fmt.Sprintf("%d to %d", start, end)
	}
}

// editionFeatures returns the features explicitly set in options, e.g.
// "features.field_presence = LEGACY_REQUIRED".
func editionFeatures(opts proto.Message) []string {
	if opts == nil {
		return nil
	}
	m := opts.ProtoReflect()
	fd := m.Descriptor().Fields().ByName("features")
	if fd == nil || !m.Has(fd) {
		return nil
	}
	var out []string
	features := m.Get(fd).Message()
	fields := features.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if !features.Has(f) || f.Enum() == nil {
			continue
		}
		v := f.Enum().Values().ByNumber(features.Get(f).Enum())
		if v != nil {
			out = append(out, fmt.Sprintf("features.%s = %s", f.Name(), v.Name()))
		}
	}
	return out
}
