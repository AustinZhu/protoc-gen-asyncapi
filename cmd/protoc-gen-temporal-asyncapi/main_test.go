package main

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/pluginpb"

	asyncapiv1 "github.com/AustinZhu/protoc-gen-temporal-asyncapi/proto/temporal/asyncapi/v1"
)

// TestRun exercises the stdin/stdout protocol end to end with a hand-built
// descriptor, so it depends on nothing but the plugin itself.
func TestRun(t *testing.T) {
	opts := &descriptorpb.MethodOptions{}
	proto.SetExtension(opts, asyncapiv1.E_Operation, &asyncapiv1.Operation{
		Kind: &asyncapiv1.Operation_Activity{Activity: &asyncapiv1.Activity{}},
	})
	file := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("ping.proto"),
		Package:     proto.String("ping.v1"),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"google/protobuf/empty.proto", "temporal/asyncapi/v1/options.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("PingInput")}},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("Pinger"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name:       proto.String("Ping"),
				InputType:  proto.String(".ping.v1.PingInput"),
				OutputType: proto.String(".google.protobuf.Empty"),
				Options:    opts,
			}},
		}},
	}
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{"ping.proto"},
		Parameter:      proto.String("format=json"),
		ProtoFile: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			protodesc.ToFileDescriptorProto(emptypb.File_google_protobuf_empty_proto),
			protodesc.ToFileDescriptorProto(asyncapiv1.File_temporal_asyncapi_v1_options_proto),
			file,
		},
	}
	in, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(bytes.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	resp := &pluginpb.CodeGeneratorResponse{}
	if err := proto.Unmarshal(out.Bytes(), resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Fatal(resp.GetError())
	}
	if len(resp.File) != 1 || resp.File[0].GetName() != "ping.asyncapi.json" || !strings.Contains(resp.File[0].GetContent(), `"activity.Ping"`) {
		t.Errorf("unexpected files: %v", resp.File)
	}
}

func TestRunBadOption(t *testing.T) {
	in, _ := proto.Marshal(&pluginpb.CodeGeneratorRequest{Parameter: proto.String("nope=1")})
	var out bytes.Buffer
	if err := run(bytes.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	resp := &pluginpb.CodeGeneratorResponse{}
	if err := proto.Unmarshal(out.Bytes(), resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.GetError(), `unknown option "nope"`) || len(resp.File) != 0 {
		t.Errorf("error = %q, files = %d", resp.GetError(), len(resp.File))
	}
}
