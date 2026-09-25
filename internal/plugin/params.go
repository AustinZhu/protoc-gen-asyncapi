package plugin

import (
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/asyncapi"
)

// Params are the plugin's options, passed as comma-separated key=value pairs
// (buf's `opt:` list, or protoc's --temporal-asyncapi_opt).
type Params struct {
	Format            string // "yaml" or "json"
	Path              string
	Services          []string // package globs
	Title             string
	Version           string
	Description       string
	ID                string
	ServerURL         string
	Namespace         string
	Perspective       asyncapi.Perspective
	TrimUnusedSchemas bool
	WithProtovalidate bool
	UseProtoNames     bool
}

type param struct {
	usage string
	set   func(p *Params, value string) error
}

var params = map[string]param{
	"format": {"yaml or json; defaults to json when path ends in .json, else yaml", func(p *Params, v string) error {
		v = strings.ToLower(v)
		if v != "yaml" && v != "json" {
			return fmt.Errorf("must be yaml or json, got %q", v)
		}
		p.Format = v
		return nil
	}},
	"path": {"output file name inside the out directory (default asyncapi.yaml)", func(p *Params, v string) error {
		if v == "" || path.IsAbs(v) || strings.HasPrefix(path.Clean(v), "..") {
			return fmt.Errorf("must be a relative path inside the output directory, got %q", v)
		}
		p.Path = path.Clean(v)
		return nil
	}},
	"services": {"proto package glob to scan, e.g. acme.orders.** (repeatable)", func(p *Params, v string) error {
		for _, g := range strings.Split(v, "|") {
			if g = strings.TrimSpace(g); g != "" {
				p.Services = append(p.Services, g)
			}
		}
		return nil
	}},
	"title":       {"info.title (default: the first package name)", func(p *Params, v string) error { p.Title = v; return nil }},
	"version":     {"info.version (default 0.0.0)", func(p *Params, v string) error { p.Version = v; return nil }},
	"description": {"info.description (default: a generated overview)", func(p *Params, v string) error { p.Description = v; return nil }},
	"id":          {"document id (default urn:temporal:<first package>)", func(p *Params, v string) error { p.ID = v; return nil }},
	"server-url":  {"Temporal frontend address, e.g. temporal.example.com:7233", func(p *Params, v string) error { p.ServerURL = v; return nil }},
	"namespace":   {"Temporal namespace, recorded on the server", func(p *Params, v string) error { p.Namespace = v; return nil }},
	"perspective": {"client (operations send) or worker (operations receive)", func(p *Params, v string) error {
		switch asyncapi.Perspective(v) {
		case asyncapi.Client, asyncapi.Worker:
			p.Perspective = asyncapi.Perspective(v)
			return nil
		}
		return fmt.Errorf("must be client or worker, got %q", v)
	}},
	"trim-unused-schemas": {"only emit schemas reachable from an operation (default false)", boolParam(func(p *Params) *bool { return &p.TrimUnusedSchemas })},
	"with-protovalidate":  {"translate buf.validate constraints when available (default true)", boolParam(func(p *Params) *bool { return &p.WithProtovalidate })},
	"use-proto-names":     {"use .proto field names instead of lowerCamelCase JSON names (default false)", boolParam(func(p *Params) *bool { return &p.UseProtoNames })},
}

func boolParam(field func(*Params) *bool) func(*Params, string) error {
	return func(p *Params, v string) error {
		if v == "" {
			*field(p) = true // a bare `key` means key=true
			return nil
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("must be true or false, got %q", v)
		}
		*field(p) = b
		return nil
	}
}

// ParseParams parses a CodeGeneratorRequest parameter string. Pairs are
// separated by commas (or semicolons); unknown keys are errors so typos are
// caught instead of silently ignored.
func ParseParams(s string) (*Params, error) {
	p := &Params{Version: "0.0.0", Perspective: asyncapi.Client, WithProtovalidate: true}
	formatSet := false
	for _, pair := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ';' }) {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		key = strings.TrimSpace(key)
		def, ok := params[key]
		if !ok {
			return nil, fmt.Errorf("unknown parameter %q; valid parameters are:\n%s", key, Usage())
		}
		if err := def.set(p, strings.TrimSpace(value)); err != nil {
			return nil, fmt.Errorf("parameter %s: %w", key, err)
		}
		formatSet = formatSet || key == "format"
	}
	if p.Path == "" {
		p.Path = "asyncapi.yaml"
		if p.Format == "json" {
			p.Path = "asyncapi.json"
		}
	}
	if !formatSet {
		p.Format = "yaml"
		if strings.EqualFold(path.Ext(p.Path), ".json") {
			p.Format = "json"
		}
	}
	return p, nil
}

// Usage lists every parameter with its description.
func Usage() string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "  %-20s %s\n", k, params[k].usage)
	}
	return b.String()
}

// matchPackage reports whether pkg matches glob, where globs are dotted
// segments: `*` matches exactly one segment and `**` any number (including
// zero). `acme.**` matches acme, acme.v1 and acme.orders.v1.
func matchPackage(glob, pkg string) bool {
	var g, p []string
	if glob != "" {
		g = strings.Split(glob, ".")
	}
	if pkg != "" {
		p = strings.Split(pkg, ".")
	}
	return matchSegments(g, p)
}

func matchSegments(g, p []string) bool {
	if len(g) == 0 {
		return len(p) == 0
	}
	if g[0] == "**" {
		for i := 0; i <= len(p); i++ {
			if matchSegments(g[1:], p[i:]) {
				return true
			}
		}
		return false
	}
	if len(p) == 0 {
		return false
	}
	if ok, _ := path.Match(g[0], p[0]); !ok {
		return false
	}
	return matchSegments(g[1:], p[1:])
}
