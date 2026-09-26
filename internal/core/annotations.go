package core

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"

	// Every protocol's annotations are registered so that ForeignAnnotations
	// recognizes them whichever plugin binary runs.
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/nats/asyncapi/v1"
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/redis/asyncapi/v1"
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/temporal/asyncapi/v1"
)

// Extension returns the value of extension xt set on opts, or the zero
// value (a nil message) when it is unset.
func Extension[T proto.Message](opts proto.Message, xt protoreflect.ExtensionType) T {
	var zero T
	if opts == nil || !proto.HasExtension(opts, xt) {
		return zero
	}
	v, _ := proto.GetExtension(opts, xt).(T)
	return v
}

// DocumentOptions returns the (asyncapi.v3.document) option of a file.
func DocumentOptions(f *protogen.File) *asyncapiv3.Document {
	return Extension[*asyncapiv3.Document](f.Desc.Options(), asyncapiv3.E_Document)
}

// ServiceOptions returns the (asyncapi.v3.service) option of a service.
func ServiceOptions(s *protogen.Service) *asyncapiv3.Service {
	return Extension[*asyncapiv3.Service](s.Desc.Options(), asyncapiv3.E_Service)
}

// OperationOptions returns the (asyncapi.v3.operation) option of a method.
func OperationOptions(m *protogen.Method) *asyncapiv3.Operation {
	return Extension[*asyncapiv3.Operation](m.Desc.Options(), asyncapiv3.E_Operation)
}

// MessageOptions returns the (asyncapi.v3.message) option of a message.
func MessageOptions(m *protogen.Message) *asyncapiv3.Message {
	if m == nil {
		return nil
	}
	return Extension[*asyncapiv3.Message](m.Desc.Options(), asyncapiv3.E_Message)
}

func fieldOptions(f *protogen.Field) *asyncapiv3.Field {
	return Extension[*asyncapiv3.Field](f.Desc.Options(), asyncapiv3.E_Field)
}

// Errorf formats an error located at a descriptor (file:line:column).
func Errorf(d protoreflect.Descriptor, format string, args ...any) error {
	return fmt.Errorf("%s: %s", Position(d), fmt.Sprintf(format, args...))
}

// Position returns file:line:column of a descriptor, or its file path when
// source info is missing.
func Position(d protoreflect.Descriptor) string {
	f := d.ParentFile()
	if f == nil {
		return string(d.FullName())
	}
	loc := f.SourceLocations().ByDescriptor(d)
	if loc.Path == nil {
		return f.Path()
	}
	return fmt.Sprintf("%s:%d:%d", f.Path(), loc.StartLine+1, loc.StartColumn+1)
}

// ConvertTag converts an annotation tag.
func ConvertTag(t *asyncapiv3.Tag) *asyncapi.Tag {
	return &asyncapi.Tag{Name: t.GetName(), Description: t.GetDescription(), ExternalDocs: ConvertDocs(t.GetExternalDocs())}
}

// ConvertTags converts annotation tags.
func ConvertTags(tags []*asyncapiv3.Tag) []*asyncapi.Tag {
	var out []*asyncapi.Tag
	for _, t := range tags {
		out = append(out, ConvertTag(t))
	}
	return out
}

// ConvertDocs converts annotation external docs; nil when unset.
func ConvertDocs(d *asyncapiv3.ExternalDocs) *asyncapi.ExternalDocs {
	if d == nil || d.GetUrl() == "" {
		return nil
	}
	return &asyncapi.ExternalDocs{URL: d.GetUrl(), Description: d.GetDescription()}
}

// AddTag appends a tag unless one with the same name is present.
func AddTag(tags []*asyncapi.Tag, t *asyncapi.Tag) []*asyncapi.Tag {
	for _, e := range tags {
		if e.Name == t.Name {
			return tags
		}
	}
	return append(tags, t)
}

// decodeExtensions parses annotation extensions (JSON values keyed by
// "x-..." names) into ext, which is created when nil.
func decodeExtensions(where string, in map[string]string, ext *asyncapi.Extensions) error {
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !strings.HasPrefix(k, "x-") {
			return fmt.Errorf("%s: extension %q must start with \"x-\"", where, k)
		}
		var v any
		if err := decodeInto(in[k], &v); err != nil {
			return fmt.Errorf("%s: extension %q is not valid JSON (strings must be quoted): %v", where, k, err)
		}
		if *ext == nil {
			*ext = asyncapi.Extensions{}
		}
		(*ext)[k] = v
	}
	return nil
}

// decodeBindings parses annotation bindings into b (created when nil),
// checking the protocol against the bindings kind of the AsyncAPI version.
func decodeBindings(where, version, kind string, in []*asyncapiv3.Binding, b **asyncapi.Bindings) error {
	for _, bd := range in {
		p := bd.GetProtocol()
		if p == "" {
			return fmt.Errorf("%s: binding protocol is required", where)
		}
		if !asyncapi.BindingAllowed(version, kind, p) {
			return fmt.Errorf("%s: %q is not a %s binding of AsyncAPI %s (use an official protocol or an \"x-\" key)", where, p, kind, version)
		}
		var v any
		if err := json.Unmarshal([]byte(bd.GetJson()), &v); err != nil {
			return fmt.Errorf("%s: %s binding is not valid JSON: %v", where, p, err)
		}
		if *b == nil {
			*b = &asyncapi.Bindings{}
		}
		(*b).Set(p, v)
	}
	return nil
}

func isDeprecated(d protoreflect.Descriptor) bool {
	o, ok := d.Options().(interface{ GetDeprecated() bool })
	return ok && o.GetDeprecated()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// FirstNonEmpty returns the first non-empty value.
func FirstNonEmpty(values ...string) string { return firstNonEmpty(values...) }

func firstLine(s string) string {
	if i := strings.Index(s, "\n\n"); i >= 0 {
		s = s[:i]
	}
	return strings.Join(strings.Fields(s), " ")
}

// FirstLine returns the first paragraph of a description on one line.
func FirstLine(s string) string { return firstLine(s) }

// Describe returns the comment attached to a declaration.
func Describe(loc protogen.CommentSet) string { return describe(loc) }

// Summarize splits a description into summary and description.
func Summarize(desc string) (string, string) { return summarize(desc) }

// SnakeCase converts CamelCase identifiers to snake_case.
func SnakeCase(s string) string { return snakeCase(s) }

// NormalizeName folds a name for loose comparisons.
func NormalizeName(s string) string { return normalizeName(s) }

// JSONPointerEscape escapes a JSON pointer token.
func JSONPointerEscape(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
}

// ForeignAnnotations reports whether a service or one of its rpcs carries
// the annotations of a protocol other than own (e.g. "nats.asyncapi.v1"),
// so protocols inferring services leave it to that protocol.
func ForeignAnnotations(s *protogen.Service, own protoreflect.FullName) bool {
	foreign := func(opts proto.Message) bool {
		found := false
		if opts == nil || !opts.ProtoReflect().IsValid() {
			return false
		}
		opts.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			pkg := fd.ParentFile().Package()
			if fd.IsExtension() && pkg != own && strings.HasSuffix(string(pkg), ".asyncapi.v1") {
				found = true
				return false
			}
			return true
		})
		return found
	}
	if foreign(s.Desc.Options()) {
		return true
	}
	for _, m := range s.Methods {
		if foreign(m.Desc.Options()) {
			return true
		}
	}
	return false
}
