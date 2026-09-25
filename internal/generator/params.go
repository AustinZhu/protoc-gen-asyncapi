package generator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/asyncapi"
)

// Params are the plugin options, passed as `--temporal-asyncapi_opt=key=value`
// with protoc or as `opt:` entries in buf.gen.yaml.
type Params struct {
	// Format of the output: "yaml" (default) or "json".
	Format string
	// Merge all input files into a single document.
	Merge bool
	// Base name (without extension) of the merged document.
	MergeFileName string
	// Perspective of the document: "client" (default) describes the
	// applications starting Workflows and sending them messages, "worker" the
	// Worker hosting them.
	Perspective string
	// AsyncAPI version to emit: "3.1.0" (default) or "3.0.0".
	AsyncAPIVersion string
	// info.title; defaults to the proto package.
	Title string
	// info.version; defaults to 0.0.0.
	Version string
	// info.description; defaults to a generated overview.
	Description string
	// Document id; defaults to urn:temporal:<package>.
	ID string
	// Temporal frontend address; emits a `temporal` server when set.
	ServerURL string
	// Temporal namespace, recorded on the server.
	Namespace string
	// Use JSON (lowerCamelCase) field names in schemas. Defaults to true, in
	// line with the Protobuf JSON mapping.
	JSONNames bool
	// Emit only schemas reachable from an operation.
	TrimUnusedSchemas bool
	// Translate buf.validate constraints when available. Defaults to true.
	Protovalidate bool
}

// Parameter names and their descriptions, used for --help and error messages.
var paramDocs = map[string]string{
	"format":              "yaml|json",
	"merge":               "true|false: one document for all files instead of one per file",
	"merge_file_name":     "base name of the merged document (default asyncapi)",
	"perspective":         "client|worker",
	"asyncapi_version":    strings.Join(asyncapi.Versions, "|"),
	"title":               "info.title (default: the proto package)",
	"version":             "info.version (default 0.0.0)",
	"description":         "info.description (default: a generated overview)",
	"id":                  "document id (default urn:temporal:<package>)",
	"server_url":          "Temporal frontend address, e.g. temporal.example.com:7233",
	"namespace":           "Temporal namespace, recorded on the server",
	"json_names":          "true|false: lowerCamelCase JSON names (default true) or .proto names",
	"trim_unused_schemas": "true|false: only schemas reachable from an operation",
	"protovalidate":       "true|false: translate buf.validate constraints (default true)",
}

// DefaultParams returns the default options.
func DefaultParams() Params {
	return Params{
		Format:          "yaml",
		MergeFileName:   "asyncapi",
		Perspective:     string(Client),
		AsyncAPIVersion: asyncapi.DefaultVersion,
		Version:         "0.0.0",
		JSONNames:       true,
		Protovalidate:   true,
	}
}

// Set applies a single key=value option.
func (p *Params) Set(key, value string) error {
	oneOf := func(allowed ...string) error {
		for _, a := range allowed {
			if value == a {
				return nil
			}
		}
		return fmt.Errorf("invalid value %q for option %q (want %s)", value, key, strings.Join(allowed, " or "))
	}
	boolean := func(dst *bool) error {
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
	switch key {
	case "format":
		p.Format = value
		return oneOf("yaml", "json")
	case "merge":
		return boolean(&p.Merge)
	case "merge_file_name":
		if value == "" || strings.ContainsAny(value, `/\`) || strings.HasPrefix(value, ".") {
			return fmt.Errorf("invalid value %q for option %q (want a plain file name without extension)", value, key)
		}
		p.MergeFileName = value
	case "perspective":
		p.Perspective = value
		return oneOf(string(Client), string(Worker))
	case "asyncapi_version":
		p.AsyncAPIVersion = value
		return oneOf(asyncapi.Versions...)
	case "title":
		p.Title = value
	case "version":
		p.Version = value
	case "description":
		p.Description = value
	case "id":
		p.ID = value
	case "server_url":
		p.ServerURL = value
	case "namespace":
		p.Namespace = value
	case "json_names":
		return boolean(&p.JSONNames)
	case "trim_unused_schemas":
		return boolean(&p.TrimUnusedSchemas)
	case "protovalidate":
		return boolean(&p.Protovalidate)
	case "paths":
		// Accepted for compatibility with tooling that always passes it;
		// output paths are always relative to the source file.
	default:
		return fmt.Errorf("unknown option %q; valid options are:\n%s", key, Usage())
	}
	return nil
}

// ParseParams parses a CodeGeneratorRequest parameter: comma-separated
// key=value pairs, where a bare key means key=true.
func ParseParams(s string) (Params, error) {
	p := DefaultParams()
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		if err := p.Set(strings.TrimSpace(key), strings.TrimSpace(value)); err != nil {
			return p, err
		}
	}
	return p, nil
}

func (p Params) extension() string {
	if p.Format == "json" {
		return ".json"
	}
	return ".yaml"
}

// Usage lists every option with its description.
func Usage() string {
	keys := make([]string, 0, len(paramDocs))
	for k := range paramDocs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "  %-20s %s\n", k, paramDocs[k])
	}
	return b.String()
}
