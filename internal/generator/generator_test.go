package generator

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/types/pluginpb"
)

var update = flag.Bool("update", false, "update golden files")

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"example", "", []string{"acme/orders/v1/orders.proto"}},
		{"fulfillment", "", []string{"acme/fulfillment/v1/fulfillment.proto"}},
		{"payments", "", []string{"acme/payments/v1/payments.proto"}},
		{"cart", "", []string{"acme/cart/v1/cart.proto"}},
		{"cart_worker", "perspective=worker", []string{"acme/cart/v1/cart.proto"}},
		{"signup", "", []string{"acme/signup/v1/signup.proto"}},
		{"signup_no_protovalidate", "protovalidate=false", []string{"acme/signup/v1/signup.proto"}},
		{"ping_json", "format=json,asyncapi_version=3.0.0,json_names=false,version=9.9.9", []string{"ping/v1/ping.proto"}},
		{"per_file", "", []string{"acme/billing/v1/billing.proto", "acme/shipping/v1/shipping.proto"}},
		{"merged", "merge=true,merge_file_name=acme,title=Acme Temporal API,version=1.2.3," +
			"server_url=https://temporal.acme.dev:7233,namespace=production,trim_unused_schemas=true",
			[]string{"acme/billing/v1/billing.proto", "acme/shipping/v1/shipping.proto"}},
	}
	schemas := compileAsyncAPISchemas(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := Run(request(t, tc.param, tc.files...), "test")
			if resp.Error != nil {
				t.Fatalf("plugin error: %s", resp.GetError())
			}
			wantFeatures := uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL | pluginpb.CodeGeneratorResponse_FEATURE_SUPPORTS_EDITIONS)
			if resp.GetSupportedFeatures() != wantFeatures {
				t.Errorf("supported features = %d, want %d", resp.GetSupportedFeatures(), wantFeatures)
			}
			if len(resp.File) == 0 {
				t.Fatal("no files generated")
			}
			dir := filepath.Join("testdata", "golden", tc.name)
			if *update {
				if err := os.RemoveAll(dir); err != nil {
					t.Fatal(err)
				}
			}
			for _, f := range resp.File {
				content := []byte(f.GetContent())
				validateDocument(t, schemas, f.GetName(), content)
				path := filepath.Join(dir, f.GetName())
				if *update {
					if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, content, 0o644); err != nil {
						t.Fatal(err)
					}
					continue
				}
				want, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("%v (run `go test ./... -update` to create golden files)", err)
				}
				if !bytes.Equal(want, content) {
					t.Errorf("%s differs from golden file %s (run `go test ./... -update` to accept):\n%s", f.GetName(), path, diff(string(want), string(content)))
				}
			}
			if !*update {
				// No stale golden files.
				generated := map[string]bool{}
				for _, f := range resp.File {
					generated[filepath.Join(dir, f.GetName())] = true
				}
				_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
					if err == nil && !info.IsDir() && !generated[p] {
						t.Errorf("golden file %s was not generated", p)
					}
					return nil
				})
			}
		})
	}
}

// compileAsyncAPISchemas loads the official AsyncAPI JSON Schemas.
func compileAsyncAPISchemas(t *testing.T) map[string]*jsonschema.Schema {
	t.Helper()
	out := map[string]*jsonschema.Schema{}
	for _, v := range []string{"3.0.0", "3.1.0"} {
		f, err := os.Open(filepath.Join("testdata", "asyncapi", v+".json"))
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		c := jsonschema.NewCompiler()
		url := "https://asyncapi.com/schemas/" + v + ".json"
		if err := c.AddResource(url, doc); err != nil {
			t.Fatal(err)
		}
		s, err := c.Compile(url)
		if err != nil {
			t.Fatalf("compile AsyncAPI %s schema: %v", v, err)
		}
		out[v] = s
	}
	return out
}

// validateDocument checks a generated document against the AsyncAPI schema
// of its version and verifies that every local $ref resolves.
func validateDocument(t *testing.T, schemas map[string]*jsonschema.Schema, name string, content []byte) {
	t.Helper()
	var doc any
	if strings.HasSuffix(name, ".json") {
		if err := json.Unmarshal(content, &doc); err != nil {
			t.Fatalf("%s: invalid JSON: %v", name, err)
		}
	} else {
		var y any
		if err := yaml.Unmarshal(content, &y); err != nil {
			t.Fatalf("%s: invalid YAML: %v", name, err)
		}
		// Normalise to JSON types.
		raw, err := json.Marshal(y)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
	}
	version, _ := doc.(map[string]any)["asyncapi"].(string)
	s, ok := schemas[version]
	if !ok {
		t.Fatalf("%s: unexpected asyncapi version %q", name, version)
	}
	if err := s.Validate(doc); err != nil {
		t.Errorf("%s is not a valid AsyncAPI %s document:\n%v", name, version, err)
	}
	checkRefs(t, name, doc, doc)
}

func checkRefs(t *testing.T, name string, root, node any) {
	switch n := node.(type) {
	case map[string]any:
		if ref, ok := n["$ref"].(string); ok {
			if resolveRef(root, ref) == nil {
				t.Errorf("%s: unresolved $ref %q", name, ref)
			}
		}
		for _, v := range n {
			checkRefs(t, name, root, v)
		}
	case []any:
		for _, v := range n {
			checkRefs(t, name, root, v)
		}
	}
}

func resolveRef(root any, ref string) any {
	if !strings.HasPrefix(ref, "#/") {
		return nil
	}
	cur := root
	for _, tok := range strings.Split(ref[2:], "/") {
		tok = strings.NewReplacer("~1", "/", "~0", "~").Replace(tok)
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		if cur, ok = m[tok]; !ok {
			return nil
		}
	}
	return cur
}

// diff returns a minimal line diff for failure messages.
func diff(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")
	var b strings.Builder
	for i := 0; i < len(w) || i < len(g); i++ {
		var wl, gl string
		if i < len(w) {
			wl = w[i]
		}
		if i < len(g) {
			gl = g[i]
		}
		if wl != gl {
			b.WriteString("line " + strconv.Itoa(i+1) + ":\n  want: " + wl + "\n  got:  " + gl + "\n")
			if b.Len() > 2000 {
				break
			}
		}
	}
	return b.String()
}

func TestValidatorRejectsInvalidDocuments(t *testing.T) {
	schemas := compileAsyncAPISchemas(t)
	for _, doc := range []string{
		`{"asyncapi":"3.1.0","info":{"title":"x","version":"1"},"operations":{"a":{"action":"publish","channel":{"$ref":"#/channels/x"}}}}`,
		// Unknown binding protocols must be x- extensions.
		`{"asyncapi":"3.0.0","info":{"title":"x","version":"1"},"components":{"operationBindings":{"a":{"temporal":{}}}}}`,
	} {
		var v any
		if err := json.Unmarshal([]byte(doc), &v); err != nil {
			t.Fatal(err)
		}
		version := v.(map[string]any)["asyncapi"].(string)
		if err := schemas[version].Validate(v); err == nil {
			t.Errorf("expected %s to be rejected", doc)
		}
	}
}

func TestErrors(t *testing.T) {
	cases := []struct {
		name, param string
		files       []string
		want        string
	}{
		{"duplicate", "", []string{"dup/v1/dup.proto"},
			`dup/v1/dup.proto:17:3: rpc dup.v1.Second.Poke: duplicate signal "Poke", already declared by rpc dup.v1.First.Poke at dup/v1/dup.proto:11:3`},
		{"duplicate across documents", "", []string{"acme/cart/v1/cart.proto", "dup/v1/dup.proto"}, `duplicate signal "Poke"`},
		{"invalid", "", []string{"invalid/v1/invalid.proto"}, strings.Join([]string{
			"invalid/v1/invalid.proto:13:3: rpc invalid.v1.Invalid.NoKind: (temporal.asyncapi.v1.operation) must set one of workflow, activity, signal, query or update",
			"invalid/v1/invalid.proto:17:3: rpc invalid.v1.Invalid.Unannotated: every rpc of a service with (temporal.asyncapi.v1.service) must declare (temporal.asyncapi.v1.operation)",
			"invalid/v1/invalid.proto:19:3: rpc invalid.v1.Invalid.SignalWithResult: signals return nothing: the rpc must return google.protobuf.Empty, not invalid.v1.M",
			"invalid/v1/invalid.proto:23:3: rpc invalid.v1.Invalid.QueryWithoutResult: queries must return a result, not google.protobuf.Empty",
			"invalid/v1/invalid.proto:27:3: rpc invalid.v1.Invalid.Streaming: streaming rpcs cannot be Temporal operations",
			`invalid/v1/invalid.proto:31:3: rpc invalid.v1.Invalid.BadDuration: workflow.run_timeout: "soon" is not a duration like "30s" or "1h30m"`,
			`invalid/v1/invalid.proto:35:3: rpc invalid.v1.Invalid.BadLink: workflows: "Missing" is not a workflow rpc of service invalid.v1.Invalid`,
		}, "\n")},
		{"invalid option value", "format=xml", []string{"ping/v1/ping.proto"}, `invalid value "xml" for option "format"`},
		{"unknown option", "colour=blue", []string{"ping/v1/ping.proto"}, `unknown option "colour"`},
		{"bad server url", "server_url=http://", []string{"ping/v1/ping.proto"}, `server_url "http://" has no host`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := Run(request(t, tc.param, tc.files...), "test")
			if !strings.Contains(resp.GetError(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", resp.GetError(), tc.want)
			}
			if len(resp.File) != 0 {
				t.Errorf("response has both an error and %d files", len(resp.File))
			}
		})
	}
}

func TestNothingToGenerate(t *testing.T) {
	req := requestFrom(t, map[string]string{
		"plain.proto": `syntax = "proto3"; package p; message M {} service S { rpc A(M) returns (M); }`,
	}, "", "plain.proto")
	resp := Run(req, "test")
	if resp.Error != nil || len(resp.File) != 0 {
		t.Fatalf("want no output for files without annotations, got error=%q files=%d", resp.GetError(), len(resp.File))
	}
}
