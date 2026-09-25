package plugin

import (
	"strings"
	"testing"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/asyncapi"
)

func TestParseParamsDefaults(t *testing.T) {
	p, err := ParseParams("")
	if err != nil {
		t.Fatal(err)
	}
	if p.Format != "yaml" || p.Path != "asyncapi.yaml" || p.Version != "0.0.0" || p.Perspective != asyncapi.Client || !p.WithProtovalidate || p.TrimUnusedSchemas || p.UseProtoNames {
		t.Errorf("defaults = %+v", p)
	}
}

func TestParseParams(t *testing.T) {
	p, err := ParseParams("format=json; path=out/api.json,services=a.**|b.v1,services=c.*,title=My API,version=1.0.0,server-url=localhost:7233,namespace=default,perspective=worker,trim-unused-schemas,with-protovalidate=false,use-proto-names=true")
	if err != nil {
		t.Fatal(err)
	}
	if p.Format != "json" || p.Path != "out/api.json" || strings.Join(p.Services, " ") != "a.** b.v1 c.*" || p.Title != "My API" ||
		p.Version != "1.0.0" || p.ServerURL != "localhost:7233" || p.Namespace != "default" || p.Perspective != asyncapi.Worker ||
		!p.TrimUnusedSchemas || p.WithProtovalidate || !p.UseProtoNames {
		t.Errorf("params = %+v", p)
	}
}

func TestParseParamsFormatInference(t *testing.T) {
	for param, want := range map[string][2]string{
		"path=docs/api.json":      {"json", "docs/api.json"},
		"path=docs/api.yml":       {"yaml", "docs/api.yml"},
		"format=json":             {"json", "asyncapi.json"},
		"format=yaml,path=x.json": {"yaml", "x.json"},
	} {
		p, err := ParseParams(param)
		if err != nil {
			t.Fatal(err)
		}
		if p.Format != want[0] || p.Path != want[1] {
			t.Errorf("%s: format=%s path=%s, want %v", param, p.Format, p.Path, want)
		}
	}
}

func TestParseParamsErrors(t *testing.T) {
	for param, want := range map[string]string{
		"fromat=json":             `unknown parameter "fromat"`,
		"format=xml":              "must be yaml or json",
		"path=../escape.yaml":     "relative path inside the output directory",
		"path=/abs.yaml":          "relative path inside the output directory",
		"perspective=server":      "must be client or worker",
		"trim-unused-schemas=no!": "must be true or false",
	} {
		_, err := ParseParams(param)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", param, err, want)
		}
	}
}

func TestMatchPackage(t *testing.T) {
	tests := []struct {
		glob, pkg string
		want      bool
	}{
		{"acme.v1", "acme.v1", true},
		{"acme.v1", "acme.v2", false},
		{"acme.*", "acme.v1", true},
		{"acme.*", "acme.orders.v1", false},
		{"acme.**", "acme", true},
		{"acme.**", "acme.orders.v1", true},
		{"**.v1", "acme.orders.v1", true},
		{"**.v1", "acme.orders.v2", false},
		{"acme.*.v1", "acme.orders.v1", true},
		{"acme.or*.v1", "acme.orders.v1", true},
		{"**", "", true},
		{"acme", "", false},
	}
	for _, tt := range tests {
		if got := matchPackage(tt.glob, tt.pkg); got != tt.want {
			t.Errorf("matchPackage(%q, %q) = %v, want %v", tt.glob, tt.pkg, got, tt.want)
		}
	}
}
