package main

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/testutil"
)

// TestRun exercises the stdin/stdout protocol end to end.
func TestRun(t *testing.T) {
	req := testutil.Request(t, testutil.ImportPaths(), map[string]string{"a.proto": `
syntax = "proto3";
package a.v1;
import "temporal/v1/options.proto";
message PingInput { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY}; }
`}, "a.proto")
	req.Parameter = proto.String("path=api.json")
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
	if resp.GetSupportedFeatures()&uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL) == 0 {
		t.Error("FEATURE_PROTO3_OPTIONAL not advertised")
	}
	if len(resp.File) != 1 || resp.File[0].GetName() != "api.json" || !strings.Contains(resp.File[0].GetContent(), `"activity.Ping"`) {
		t.Errorf("unexpected files: %v", resp.File)
	}
}

func TestRunBadParameter(t *testing.T) {
	in, _ := proto.Marshal(&pluginpb.CodeGeneratorRequest{Parameter: proto.String("nope=1")})
	var out bytes.Buffer
	if err := run(bytes.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	resp := &pluginpb.CodeGeneratorResponse{}
	if err := proto.Unmarshal(out.Bytes(), resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.GetError(), `unknown parameter "nope"`) || len(resp.File) != 0 {
		t.Errorf("error = %q, files = %d", resp.GetError(), len(resp.File))
	}
}
