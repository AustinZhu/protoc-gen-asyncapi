package plugin

import (
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/testutil"
)

var update = flag.Bool("update", false, "rewrite golden files under testdata/")

// TestGolden runs the plugin on every testdata/<case>/ directory. Each case
// holds .proto files (all of them are files to generate), an optional
// `params` file with the plugin parameter, and the expected output: either
// expected.asyncapi.yaml / expected.asyncapi.json, or expected.error for
// cases that must fail.
func TestGolden(t *testing.T) {
	root := filepath.Join(testutil.RepoRoot(), "testdata")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "third_party" {
			continue
		}
		dir := filepath.Join(root, e.Name())
		t.Run(e.Name(), func(t *testing.T) { runGolden(t, dir) })
	}
}

func runGolden(t *testing.T, dir string) {
	protos, err := filepath.Glob(filepath.Join(dir, "*.proto"))
	if err != nil || len(protos) == 0 {
		t.Fatalf("no .proto files in %s", dir)
	}
	sort.Strings(protos)
	for i, p := range protos {
		protos[i] = filepath.Base(p)
	}
	req := testutil.Request(t, append([]string{dir}, testutil.ImportPaths()...), nil, protos...)
	if b, err := os.ReadFile(filepath.Join(dir, "params")); err == nil {
		param := strings.TrimSpace(string(b))
		req.Parameter = &param
	}

	resp := Run(req)
	var got, golden string
	if resp.Error != nil {
		got, golden = resp.GetError()+"\n", filepath.Join(dir, "expected.error")
		if len(resp.File) != 0 {
			t.Errorf("response has both an error and %d files", len(resp.File))
		}
	} else {
		if len(resp.File) != 1 {
			t.Fatalf("got %d files, want 1", len(resp.File))
		}
		f := resp.File[0]
		ext := ".yaml"
		if strings.HasSuffix(f.GetName(), ".json") {
			ext = ".json"
		}
		got, golden = f.GetContent(), filepath.Join(dir, "expected.asyncapi"+ext)
		if want := expectedPath(dir); want != "" && filepath.Base(want) != "expected.asyncapi"+ext {
			t.Errorf("output file %s does not match golden %s", f.GetName(), filepath.Base(want))
		}
		if b, err := os.ReadFile(filepath.Join(dir, "expected.path")); err == nil && strings.TrimSpace(string(b)) != f.GetName() {
			t.Errorf("output path = %q, want %q", f.GetName(), strings.TrimSpace(string(b)))
		}
	}

	if *update {
		for _, stale := range []string{"expected.error", "expected.asyncapi.yaml", "expected.asyncapi.json"} {
			os.Remove(filepath.Join(dir, stale))
		}
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("missing golden file (run go test ./... -update): %v\ngot:\n%s", err, got)
	}
	if got != string(want) {
		t.Errorf("output differs from %s (run go test ./internal/plugin -update to accept):\n%s", golden, diff(string(want), got))
	}
}

func expectedPath(dir string) string {
	for _, name := range []string{"expected.asyncapi.yaml", "expected.asyncapi.json", "expected.error"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return filepath.Join(dir, name)
		}
	}
	return ""
}

// diff returns the first differing lines, enough to locate a mismatch.
func diff(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")
	for i := 0; i < len(w) || i < len(g); i++ {
		var wl, gl string
		if i < len(w) {
			wl = w[i]
		}
		if i < len(g) {
			gl = g[i]
		}
		if wl != gl {
			return "line " + strconv.Itoa(i+1) + ":\n- " + wl + "\n+ " + gl
		}
	}
	return ""
}
