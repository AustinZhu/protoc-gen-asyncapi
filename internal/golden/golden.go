// Package golden holds the test harness shared by the protocol packages:
// in-process compilation of test protos, golden-file comparison, and
// validation of every generated document against the official AsyncAPI
// JSON Schemas.
package golden

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi/spec"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

	// Register the annotation extensions so options parse as typed values.
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/nats/asyncapi/v1"
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/temporal/asyncapi/v1"
)

// Request compiles files (resolved against importPaths, with in-memory
// sources taking precedence) into a CodeGeneratorRequest as protoc sends it.
func Request(t testing.TB, importPaths []string, sources map[string]string, param string, files ...string) *pluginpb.CodeGeneratorRequest {
	t.Helper()
	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(protocompile.CompositeResolver{
			&protocompile.SourceResolver{Accessor: protocompile.SourceAccessorFromMap(sources)},
			&protocompile.SourceResolver{ImportPaths: importPaths},
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	compiled, err := c.Compile(context.Background(), files...)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	req := &pluginpb.CodeGeneratorRequest{FileToGenerate: files, CompilerVersion: &pluginpb.Version{Major: proto.Int32(30)}}
	if param != "" {
		req.Parameter = proto.String(param)
	}
	seen := map[string]bool{}
	var add func(fd protoreflect.FileDescriptor)
	add = func(fd protoreflect.FileDescriptor) {
		if seen[fd.Path()] {
			return
		}
		seen[fd.Path()] = true
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			add(imports.Get(i).FileDescriptor)
		}
		req.ProtoFile = append(req.ProtoFile, protodesc.ToFileDescriptorProto(fd))
	}
	for _, f := range compiled {
		add(f)
	}
	// Round trip through the wire format like protoc does.
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	out := &pluginpb.CodeGeneratorRequest{}
	if err := (proto.UnmarshalOptions{Resolver: protoregistry.GlobalTypes}).Unmarshal(raw, out); err != nil {
		t.Fatal(err)
	}
	return out
}

// Run runs a plugin on a request with the fixed version "test".
func Run(pl core.Plugin, req *pluginpb.CodeGeneratorRequest) *pluginpb.CodeGeneratorResponse {
	return pl.Run(req, "test")
}

// Check validates every generated file and compares it with the golden
// files under dir; with update it rewrites them instead. Stale golden files
// fail the test.
func Check(t *testing.T, dir string, resp *pluginpb.CodeGeneratorResponse, update bool, importPaths []string) {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("plugin error: %s", resp.GetError())
	}
	want := uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL | pluginpb.CodeGeneratorResponse_FEATURE_SUPPORTS_EDITIONS)
	if resp.GetSupportedFeatures() != want {
		t.Errorf("supported features = %d, want %d", resp.GetSupportedFeatures(), want)
	}
	if len(resp.File) == 0 {
		t.Fatal("no files generated")
	}
	if update {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	generated := map[string]bool{}
	for _, f := range resp.File {
		content := []byte(f.GetContent())
		ValidateFile(t, f.GetName(), content, importPaths)
		path := filepath.Join(dir, f.GetName())
		generated[path] = true
		if update {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, content, 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		wantContent, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%v (run `go test ./... -update` to create golden files)", err)
		}
		if !bytes.Equal(wantContent, content) {
			t.Errorf("%s differs from golden file %s (run `go test ./... -update` to accept):\n%s", f.GetName(), path, Diff(string(wantContent), string(content)))
		}
	}
	if !update {
		_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && !generated[p] {
				t.Errorf("golden file %s was not generated", p)
			}
			return nil
		})
	}
}

// ValidateFile checks a generated file: AsyncAPI documents against the
// official JSON Schema of their version (with every local $ref resolving and
// Protobuf payloads compiling against importPaths), standalone JSON Schemas
// for well-formedness and resolvable $refs.
func ValidateFile(t *testing.T, name string, content []byte, importPaths []string) {
	t.Helper()
	doc, err := spec.Decode(content)
	if err != nil {
		t.Fatalf("%s: cannot decode: %v", name, err)
	}
	if strings.HasSuffix(name, ".schema.json") {
		m, _ := doc.(map[string]any)
		if m["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
			t.Errorf("%s: missing $schema", name)
		}
		if err := spec.CompileJSONSchema(content); err != nil {
			t.Errorf("%s is not a valid JSON Schema:\n%v", name, err)
		}
	} else if err := spec.Validate(content); err != nil {
		t.Errorf("%s is not a valid AsyncAPI document:\n%v", name, err)
	}
	checkRefs(t, name, doc, doc)
	compileProtobufPayloads(t, name, doc, importPaths)
}

// compileProtobufPayloads checks that Protobuf payload schemas compile.
func compileProtobufPayloads(t *testing.T, name string, doc any, importPaths []string) {
	t.Helper()
	msgs, _ := resolve(doc, "#/components/messages").(map[string]any)
	for id, m := range msgs {
		payload, _ := m.(map[string]any)["payload"].(map[string]any)
		format, _ := payload["schemaFormat"].(string)
		if !strings.HasPrefix(format, "application/vnd.google.protobuf") {
			continue
		}
		src, _ := payload["schema"].(string)
		resolver := protocompile.CompositeResolver{
			&protocompile.SourceResolver{Accessor: protocompile.SourceAccessorFromMap(map[string]string{"payload.proto": src})},
			&protocompile.SourceResolver{ImportPaths: importPaths},
		}
		c := protocompile.Compiler{Resolver: protocompile.WithStandardImports(resolver)}
		if _, err := c.Compile(context.Background(), "payload.proto"); err != nil {
			t.Errorf("%s: payload of %s does not compile: %v\n%s", name, id, err, src)
		}
	}
}

func checkRefs(t *testing.T, name string, root, node any) {
	switch n := node.(type) {
	case map[string]any:
		if ref, ok := n["$ref"].(string); ok && resolve(root, ref) == nil {
			t.Errorf("%s: unresolved $ref %q", name, ref)
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

func resolve(root any, ref string) any {
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

// Diff returns a minimal line diff for failure messages.
func Diff(want, got string) string {
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

// JSON decodes a generated document for assertions.
func JSON(t *testing.T, content string) map[string]any {
	t.Helper()
	doc, err := spec.Decode([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := doc.(map[string]any)
	return m
}
