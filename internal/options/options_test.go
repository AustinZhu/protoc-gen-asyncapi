package options_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/options"
	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/testutil"
)

const header = `syntax = "proto3";
package a.v1;
import "temporal/v1/options.proto";
`

// discover compiles the given files (the first is the one scanned) and runs Discover.
func discover(t *testing.T, sources map[string]string, scan ...string) ([]*options.Operation, error) {
	t.Helper()
	if len(scan) == 0 {
		scan = []string{"a.proto"}
	}
	req := testutil.Request(t, testutil.ImportPaths(), sources, scan...)
	registry, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: req.ProtoFile})
	if err != nil {
		t.Fatal(err)
	}
	var files []protoreflect.FileDescriptor
	for _, name := range scan {
		fd, err := registry.FindFileByPath(name)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, fd)
	}
	return options.Discover(files, registry)
}

func TestDiscover(t *testing.T) {
	ops, err := discover(t, map[string]string{"a.proto": header + `
message Plain { string x = 1; }

message StartInput {
  option (temporal.v1.operation) = {
    kind: OPERATION_KIND_WORKFLOW
    result: "Outer.Result"
    task_queue: "q"
    supports_continue_as_new: true
  };
  message Nested {
    option (temporal.v1.operation) = {kind: OPERATION_KIND_SIGNAL, name: "nested-signal", workflows: "Start"};
  }
}

message Outer {
  message Result {}
}

message Poke {
  option (temporal.v1.operation) = {kind: OPERATION_KIND_UPDATE, result: ".a.v1.Plain"};
}
`})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 3 {
		t.Fatalf("got %d operations, want 3", len(ops))
	}
	start, nested, poke := ops[0], ops[1], ops[2]
	if start.Kind != options.Workflow || start.Name != "Start" || start.TaskQueue != "q" || !start.SupportsContinueAsNew {
		t.Errorf("start = %+v", start)
	}
	if start.Result == nil || start.Result.FullName() != "a.v1.Outer.Result" {
		t.Errorf("start result = %v", start.Result)
	}
	if nested.Kind != options.Signal || nested.Name != "nested-signal" || nested.Result != nil || strings.Join(nested.Workflows, ",") != "Start" {
		t.Errorf("nested = %+v", nested)
	}
	if poke.Name != "Poke" || poke.Result.FullName() != "a.v1.Plain" {
		t.Errorf("poke = %+v", poke)
	}
}

func TestDefaultName(t *testing.T) {
	ops, err := discover(t, map[string]string{"a.proto": header + `
message ChargeCardInput { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY}; }
message RefundRequest { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY}; }
message ShipArgs { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY}; }
message BillParams { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY}; }
message Input { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY}; }
message Ping { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY}; }
`})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, op := range ops {
		names = append(names, op.Name)
	}
	if got, want := strings.Join(names, ","), "ChargeCard,Refund,Ship,Bill,Input,Ping"; got != want {
		t.Errorf("names = %s, want %s", got, want)
	}
}

func TestDiscoverErrors(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		scan  []string
		want  []string
	}{
		{
			name:  "missing kind",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {name: "M"}; }`},
			want:  []string{"a.proto:4:1: message a.v1.M:", "kind must be set"},
		},
		{
			name:  "blank name",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_SIGNAL, name: "  "}; }`},
			want:  []string{"name must not be blank"},
		},
		{
			name:  "unresolved result",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_QUERY, result: "Nope"}; }`},
			want:  []string{`result "Nope" not found in package "a.v1"`},
		},
		{
			name:  "result is an enum",
			files: map[string]string{"a.proto": header + `enum E { E_UNSPECIFIED = 0; } message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_QUERY, result: "E"}; }`},
			want:  []string{"which is not a message"},
		},
		{
			name: "result in another package",
			files: map[string]string{
				"a.proto": header + `import "b.proto"; message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_QUERY, result: "b.v1.R"}; b.v1.R r = 1; }`,
				"b.proto": `syntax = "proto3"; package b.v1; message R {}`,
			},
			want: []string{`result "b.v1.R" is in package "b.v1"; results must be in the input's package "a.v1"`},
		},
		{
			name: "result in another package, fully qualified",
			files: map[string]string{
				"a.proto": header + `import "b.proto"; message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_QUERY, result: ".b.v1.R"}; b.v1.R r = 1; }`,
				"b.proto": `syntax = "proto3"; package b.v1; message R {}`,
			},
			want: []string{`is in package "b.v1"`},
		},
		{
			name: "duplicate kind and name",
			files: map[string]string{"a.proto": header + `
message X { option (temporal.v1.operation) = {kind: OPERATION_KIND_WORKFLOW, name: "W"}; }
message Y { option (temporal.v1.operation) = {kind: OPERATION_KIND_WORKFLOW, name: "W"}; }`},
			want: []string{`a.proto:6:1: message a.v1.Y: duplicate workflow "W", already declared by message a.v1.X at a.proto:5:1`},
		},
		{
			name: "duplicate across files",
			files: map[string]string{
				"a.proto": header + `message X { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY, name: "Act"}; }`,
				"c.proto": `syntax = "proto3"; package c.v1; import "temporal/v1/options.proto";
message Y { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY, name: "Act"}; }`,
			},
			scan: []string{"a.proto", "c.proto"},
			want: []string{`c.proto:2:1: message c.v1.Y: duplicate activity "Act", already declared by message a.v1.X at a.proto:4:1`},
		},
		{
			name:  "signal with result",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_SIGNAL, result: "M"}; }`},
			want:  []string{"signals cannot declare a result"},
		},
		{
			name:  "continue-as-new on a query",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_QUERY, supports_continue_as_new: true}; }`},
			want:  []string{"supports_continue_as_new is only valid for workflows, not queries"},
		},
		{
			name:  "workflows on a workflow",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_WORKFLOW, workflows: "X"}; }`},
			want:  []string{"workflows is only valid for signals, queries and updates"},
		},
		{
			name:  "retry policy on a signal",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_SIGNAL, retry_policy: {maximum_attempts: 1}}; }`},
			want:  []string{"retry_policy is only valid for workflows and activities, not signals"},
		},
		{
			name:  "activity timeout on a workflow",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_WORKFLOW, timeouts: {heartbeat: "1s"}}; }`},
			want:  []string{"timeouts.heartbeat is only valid for activities, not workflows"},
		},
		{
			name:  "bad duration",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY, retry_policy: {initial_interval: "-1s"}}; }`},
			want:  []string{"retry_policy.initial_interval must be positive"},
		},
		{
			name:  "bad backoff",
			files: map[string]string{"a.proto": header + `message M { option (temporal.v1.operation) = {kind: OPERATION_KIND_ACTIVITY, retry_policy: {backoff_coefficient: 0.5}}; }`},
			want:  []string{"backoff_coefficient must be >= 1"},
		},
		{
			name: "all errors reported together",
			files: map[string]string{"a.proto": header + `
message M { option (temporal.v1.operation) = {name: "M"}; }
message N { option (temporal.v1.operation) = {kind: OPERATION_KIND_QUERY, result: "Nope"}; }`},
			want: []string{"a.v1.M", "a.v1.N"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := discover(t, tt.files, tt.scan...)
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
		})
	}
}

func TestNoAnnotations(t *testing.T) {
	ops, err := discover(t, map[string]string{"a.proto": `syntax = "proto3"; package a.v1; message M { string x = 1; }`})
	if err != nil || len(ops) != 0 {
		t.Errorf("ops = %v, err = %v", ops, err)
	}
}
