package core

import (
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
)

// Enum value renderings (enum_values option).
const (
	EnumNames   = "names"
	EnumNumbers = "numbers"
	EnumBoth    = "both"
)

// Params are the options every protocol shares, passed as
// `--<plugin>_opt=key=value` with protoc or as `opt:` entries in
// buf.gen.yaml.
type Params struct {
	// Format of the output: "yaml" (default), "json", or "jsonschema" (a
	// standalone JSON Schema of the payloads instead of an AsyncAPI document).
	Format string
	// Merge all input files into a single document.
	Merge bool
	// Base name (without extension) of the merged document.
	MergeFileName string
	// "server" (default): the document describes the application
	// implementing the services (NATS service, Temporal Worker); "client":
	// the applications calling them.
	Perspective string
	// Payload schema format: "jsonschema" (default) or "protobuf".
	Payload string
	// AsyncAPI version: "3.1.0" (default) or "3.0.0".
	AsyncAPIVersion string
	// Override info.title, info.version, info.description and the
	// document id.
	Title, Version, Description, ID string
	// Overrides the default content type of messages.
	ContentType string
	// Use JSON (lowerCamelCase) field names in schemas (default true).
	JSONNames bool
	// Enum rendering: names (default), numbers or both.
	EnumValues string
	// Annotate field schemas with their Protobuf type (x-protobuf-type).
	ProtoTypes bool
	// Emit only the schemas reachable from an operation instead of every
	// message and enum of the documented files.
	TrimUnusedSchemas bool
	// Translate buf.validate constraints (default true).
	Protovalidate bool
	// Omit the tag named after each service (and its info.tags entry);
	// declared and protocol tags are kept.
	WithoutDefaultTags bool
	// Fully-qualified service name globs to document; empty means all.
	// "*" matches one name segment and "**" any number.
	Services []string
}

// Option is one plugin option.
type Option struct {
	Name  string
	Usage string
	// Set applies a value; a bare `name` passes "".
	Set func(value string) error
}

// DefaultParams returns the defaults.
func DefaultParams() *Params {
	return &Params{
		Format:          "yaml",
		MergeFileName:   "asyncapi",
		Perspective:     "server",
		Payload:         "jsonschema",
		AsyncAPIVersion: asyncapi.DefaultVersion,
		JSONNames:       true,
		EnumValues:      EnumNames,
		Protovalidate:   true,
	}
}

func oneOf(key, value string, allowed ...string) error {
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return fmt.Errorf("invalid value %q for option %q (want %s)", value, key, strings.Join(allowed, " or "))
}

// BoolOption returns the Set function of a boolean option.
func BoolOption(key string, dst *bool) func(string) error {
	return func(value string) error {
		if value == "" {
			*dst = true
			return nil
		}
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid value %q for option %q (want true or false)", value, key)
		}
		*dst = b
		return nil
	}
}

// Options returns the shared options bound to p.
func (p *Params) Options() []Option {
	str := func(dst *string) func(string) error { return func(v string) error { *dst = v; return nil } }
	choice := func(key string, dst *string, allowed ...string) func(string) error {
		return func(v string) error { *dst = v; return oneOf(key, v, allowed...) }
	}
	return []Option{
		{"format", "yaml|json|jsonschema", choice("format", &p.Format, "yaml", "json", "jsonschema")},
		{"merge", "true|false: one document for all files instead of one per file", BoolOption("merge", &p.Merge)},
		{"merge_file_name", "base name of the merged document (default asyncapi)", func(v string) error {
			if v == "" || strings.ContainsAny(v, `/\`) || strings.HasPrefix(v, ".") {
				return fmt.Errorf("invalid value %q for option %q (want a file name without directory or extension)", v, "merge_file_name")
			}
			p.MergeFileName = v
			return nil
		}},
		{"perspective", "server|client (worker is an alias of server)", func(v string) error {
			if v == "worker" {
				v = "server"
			}
			p.Perspective = v
			return oneOf("perspective", v, "server", "client")
		}},
		{"payload", "jsonschema|protobuf", choice("payload", &p.Payload, "jsonschema", "protobuf")},
		{"asyncapi_version", strings.Join(asyncapi.Versions, "|"), choice("asyncapi_version", &p.AsyncAPIVersion, asyncapi.Versions...)},
		{"title", "info.title (default: the service or proto package)", str(&p.Title)},
		{"version", "info.version (default 0.0.0)", str(&p.Version)},
		{"description", "info.description (default: the file's leading comments)", str(&p.Description)},
		{"id", "document id", str(&p.ID)},
		{"content_type", "default content type of messages", str(&p.ContentType)},
		{"json_names", "true|false: lowerCamelCase JSON names (default true) or .proto names", BoolOption("json_names", &p.JSONNames)},
		{"enum_values", "names|numbers|both", choice("enum_values", &p.EnumValues, EnumNames, EnumNumbers, EnumBoth)},
		{"enums_as_ints", "true|false: shorthand for enum_values=numbers", func(v string) error {
			var ints bool
			if err := BoolOption("enums_as_ints", &ints)(v); err != nil {
				return err
			}
			p.EnumValues = EnumNames
			if ints {
				p.EnumValues = EnumNumbers
			}
			return nil
		}},
		{"trim_unused_schemas", "true|false: only schemas reachable from an operation", BoolOption("trim_unused_schemas", &p.TrimUnusedSchemas)},
		{"protovalidate", "true|false: translate buf.validate constraints (default true)", BoolOption("protovalidate", &p.Protovalidate)},
		{"without_default_tags", "true|false: omit the tags named after services; declared and protocol tags are kept", BoolOption("without_default_tags", &p.WithoutDefaultTags)},
		{"proto_types", "true|false: annotate fields with x-protobuf-type", BoolOption("proto_types", &p.ProtoTypes)},
		{"services", "fully-qualified service glob, e.g. acme.orders.** (repeatable)", func(v string) error {
			for _, g := range strings.Split(v, "|") {
				if g = strings.TrimSpace(g); g != "" {
					p.Services = append(p.Services, g)
				}
			}
			return nil
		}},
		// Accepted for compatibility with tooling that always passes it;
		// output paths are always relative to the source file.
		{"paths", "ignored", func(string) error { return nil }},
	}
}

// parseParams applies a CodeGeneratorRequest parameter to the options.
func parseParams(param string, opts []Option) error {
	byName := map[string]Option{}
	for _, o := range opts {
		byName[o.Name] = o
	}
	for _, pair := range strings.Split(param, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		key = strings.TrimSpace(key)
		o, ok := byName[key]
		if !ok {
			return fmt.Errorf("unknown option %q; valid options are:\n%s", key, Usage(opts))
		}
		if err := o.Set(strings.TrimSpace(value)); err != nil {
			return err
		}
	}
	return nil
}

// Usage lists options with their descriptions, for --help and errors.
func Usage(opts []Option) string {
	sorted := append([]Option(nil), opts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var b strings.Builder
	for _, o := range sorted {
		if o.Name == "paths" {
			continue
		}
		fmt.Fprintf(&b, "  %-18s %s\n", o.Name, o.Usage)
	}
	return b.String()
}

func (p *Params) extension() string {
	switch p.Format {
	case "json":
		return ".json"
	case "jsonschema":
		return ".schema.json"
	}
	return ".yaml"
}

// MatchService reports whether a fully-qualified service name matches a
// glob of dotted segments: "*" matches one segment, "**" any number.
func MatchService(glob, name string) bool {
	var g, n []string
	if glob != "" {
		g = strings.Split(glob, ".")
	}
	if name != "" {
		n = strings.Split(name, ".")
	}
	return matchSegments(g, n)
}

func matchSegments(g, n []string) bool {
	if len(g) == 0 {
		return len(n) == 0
	}
	if g[0] == "**" {
		for i := 0; i <= len(n); i++ {
			if matchSegments(g[1:], n[i:]) {
				return true
			}
		}
		return false
	}
	if len(n) == 0 {
		return false
	}
	if ok, _ := path.Match(g[0], n[0]); !ok {
		return false
	}
	return matchSegments(g[1:], n[1:])
}
