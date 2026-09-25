package generator

import (
	"context"
	"testing"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// importPaths are the directories test protos are resolved against.
var importPaths = []string{
	"testdata/protos",
	"../../proto",
	"../../examples/proto",
}

// request compiles files from importPaths and builds a CodeGeneratorRequest
// for them, as protoc would send it.
func request(t testing.TB, param string, files ...string) *pluginpb.CodeGeneratorRequest {
	t.Helper()
	return requestFrom(t, nil, param, files...)
}

// requestFrom is request with extra in-memory sources, which take precedence
// over importPaths.
func requestFrom(t testing.TB, sources map[string]string, param string, files ...string) *pluginpb.CodeGeneratorRequest {
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
	req := &pluginpb.CodeGeneratorRequest{FileToGenerate: files}
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
	// Round trip through the wire format like protoc does, so extension
	// options resolve against the global registry exactly as in production.
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

// files compiles sources and returns the descriptors of the named files plus
// the registry of everything compiled.
func filesFrom(t testing.TB, sources map[string]string, names ...string) ([]protoreflect.FileDescriptor, *protoregistry.Files) {
	t.Helper()
	req := requestFrom(t, sources, "", names...)
	registry, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: req.ProtoFile})
	if err != nil {
		t.Fatal(err)
	}
	var out []protoreflect.FileDescriptor
	for _, name := range names {
		fd, err := registry.FindFileByPath(name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, fd)
	}
	return out, registry
}
