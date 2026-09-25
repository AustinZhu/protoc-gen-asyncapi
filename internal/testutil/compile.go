// Package testutil compiles .proto sources in-process for tests, so the test
// suite does not depend on protoc or buf being installed.
package testutil

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	// Registers the temporal.v1.operation extension so options parse into typed values.
	_ "github.com/AustinZhu/protoc-gen-temporal-asyncapi/gen/temporalv1"
)

// RepoRoot returns the absolute path of the repository root.
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// ImportPaths are the include directories every compilation gets: the
// repository's own proto/ directory (for temporal/v1/options.proto) and the
// vendored third-party protos (for buf/validate/validate.proto).
func ImportPaths() []string {
	root := RepoRoot()
	return []string{filepath.Join(root, "proto"), filepath.Join(root, "testdata", "third_party")}
}

// Request compiles the named files and returns a CodeGeneratorRequest that
// looks like the one protoc would send: every transitive dependency in
// topological order in ProtoFile, source info retained, and FileToGenerate set
// to the given files.
func Request(t testing.TB, importPaths []string, sources map[string]string, files ...string) *pluginpb.CodeGeneratorRequest {
	t.Helper()
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(protocompile.CompositeResolver{
			&protocompile.SourceResolver{Accessor: protocompile.SourceAccessorFromMap(sources)},
			&protocompile.SourceResolver{ImportPaths: importPaths},
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	compiled, err := compiler.Compile(context.Background(), files...)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	req := &pluginpb.CodeGeneratorRequest{FileToGenerate: files}
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
		req.ProtoFile = append(req.ProtoFile, reparse(t, protodesc.ToFileDescriptorProto(fd)))
	}
	for _, f := range compiled {
		add(f)
	}
	return req
}

// reparse round-trips a descriptor through the wire format, as protoc's
// output would be parsed by a plugin, so extension options resolve against
// the global registry exactly as they do in production.
func reparse(t testing.TB, fdp *descriptorpb.FileDescriptorProto) *descriptorpb.FileDescriptorProto {
	t.Helper()
	b, err := proto.Marshal(fdp)
	if err != nil {
		t.Fatal(err)
	}
	out := &descriptorpb.FileDescriptorProto{}
	if err := (proto.UnmarshalOptions{Resolver: protoregistry.GlobalTypes}).Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	return out
}
