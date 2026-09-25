package generator

import (
	"strings"
	"testing"
)

const header = `syntax = "proto3";
package a.v1;
import "google/protobuf/empty.proto";
import "temporal/asyncapi/v1/options.proto";
`

// discover compiles the given files and runs Discover on the scanned ones
// (a.proto by default).
func discover(t *testing.T, sources map[string]string, scan ...string) ([]*Operation, error) {
	t.Helper()
	if len(scan) == 0 {
		scan = []string{"a.proto"}
	}
	files, _ := filesFrom(t, sources, scan...)
	return Discover(files)
}

func TestDiscover(t *testing.T) {
	ops, err := discover(t, map[string]string{"a.proto": header + `
message In {}
message Out {}

service Orders {
  option (temporal.asyncapi.v1.service) = {task_queue: "orders"};

  // Starts an order.
  rpc Process(In) returns (Out) {
    option (temporal.asyncapi.v1.operation).workflow = {
      continue_as_new: true
      execution_timeout: "24h"
      retry_policy: {maximum_attempts: 3}
    };
  }
  rpc Charge(In) returns (google.protobuf.Empty) {
    option (temporal.asyncapi.v1.operation).activity = {name: "charge-card", task_queue: "payments", heartbeat_timeout: "5s"};
  }
  rpc Cancel(In) returns (google.protobuf.Empty) {
    option (temporal.asyncapi.v1.operation).signal = {};
  }
  rpc Status(google.protobuf.Empty) returns (Out) {
    option (temporal.asyncapi.v1.operation).query = {name: "status"};
  }
  rpc Amend(In) returns (Out) {
    option (temporal.asyncapi.v1.operation).update = {workflows: "Process"};
  }
}

// Unannotated services and rpcs are ignored.
service Plain {
  rpc Nothing(In) returns (Out);
}
`})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 5 {
		t.Fatalf("got %d operations, want 5", len(ops))
	}
	wf, act, sig, q, up := ops[0], ops[1], ops[2], ops[3], ops[4]

	if wf.Kind != Workflow || wf.Name != "Process" || wf.TaskQueue != "orders" || !wf.ContinueAsNew ||
		wf.Result.FullName() != "a.v1.Out" || wf.Timeouts.Execution != "24h" || wf.RetryPolicy.GetMaximumAttempts() != 3 {
		t.Errorf("workflow = %+v", wf)
	}
	if wf.Description() != "Starts an order." {
		t.Errorf("workflow description = %q", wf.Description())
	}
	if act.Kind != Activity || act.Name != "charge-card" || act.TaskQueue != "payments" || act.Result != nil || act.Timeouts.Heartbeat != "5s" {
		t.Errorf("activity = %+v", act)
	}
	// Handlers without workflows default to the service's only workflow.
	if sig.Kind != Signal || sig.Name != "Cancel" || sig.TaskQueue != "" || strings.Join(sig.Workflows, ",") != "Process" {
		t.Errorf("signal = %+v", sig)
	}
	if q.Kind != Query || q.Name != "status" || q.Input.FullName() != "google.protobuf.Empty" || q.Result.FullName() != "a.v1.Out" {
		t.Errorf("query = %+v", q)
	}
	if up.Kind != Update || strings.Join(up.Workflows, ",") != "Process" {
		t.Errorf("update = %+v", up)
	}
}

func TestHandlerLinking(t *testing.T) {
	ops, err := discover(t, map[string]string{"a.proto": header + `
message M {}
service Two {
  rpc A(M) returns (M) { option (temporal.asyncapi.v1.operation).workflow = {name: "wf-a"}; }
  rpc B(M) returns (M) { option (temporal.asyncapi.v1.operation).workflow = {}; }
  // Ambiguous: no default link.
  rpc Poke(M) returns (google.protobuf.Empty) { option (temporal.asyncapi.v1.operation).signal = {}; }
  // Links use rpc names and resolve to Temporal names.
  rpc Both(M) returns (google.protobuf.Empty) { option (temporal.asyncapi.v1.operation).signal = {workflows: ["A", "B"]}; }
}
service Handlers {
  // No workflow in this service: stays unlinked.
  rpc Alone(M) returns (M) { option (temporal.asyncapi.v1.operation).query = {}; }
}
`})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, op := range ops {
		got[op.Name] = strings.Join(op.Workflows, ",")
	}
	want := map[string]string{"wf-a": "", "B": "", "Poke": "", "Both": "wf-a,B", "Alone": ""}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s workflows = %q, want %q", k, got[k], v)
		}
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
			name:  "no kind set",
			files: map[string]string{"a.proto": header + `message M {} service S { rpc R(M) returns (M) { option (temporal.asyncapi.v1.operation) = {}; } }`},
			want:  []string{"a.proto:5:", "rpc a.v1.S.R:", "must set one of workflow, activity, signal, query or update"},
		},
		{
			name:  "unannotated rpc in annotated service",
			files: map[string]string{"a.proto": header + `message M {} service S { option (temporal.asyncapi.v1.service) = {}; rpc R(M) returns (M); }`},
			want:  []string{"rpc a.v1.S.R: every rpc of a service with (temporal.asyncapi.v1.service) must declare"},
		},
		{
			name:  "streaming",
			files: map[string]string{"a.proto": header + `message M {} service S { rpc R(M) returns (stream M) { option (temporal.asyncapi.v1.operation).workflow = {}; } }`},
			want:  []string{"streaming rpcs cannot be Temporal operations"},
		},
		{
			name:  "signal with result",
			files: map[string]string{"a.proto": header + `message M {} service S { rpc R(M) returns (M) { option (temporal.asyncapi.v1.operation).signal = {}; } }`},
			want:  []string{"signals return nothing: the rpc must return google.protobuf.Empty, not a.v1.M"},
		},
		{
			name:  "query without result",
			files: map[string]string{"a.proto": header + `message M {} service S { rpc R(M) returns (google.protobuf.Empty) { option (temporal.asyncapi.v1.operation).query = {}; } }`},
			want:  []string{"queries must return a result"},
		},
		{
			name:  "blank name",
			files: map[string]string{"a.proto": header + `message M {} service S { rpc R(M) returns (M) { option (temporal.asyncapi.v1.operation).update = {name: "  "}; } }`},
			want:  []string{"name must not be blank"},
		},
		{
			name: "unknown workflow reference",
			files: map[string]string{"a.proto": header + `message M {} service S {
  rpc W(M) returns (M) { option (temporal.asyncapi.v1.operation).activity = {}; }
  rpc R(M) returns (google.protobuf.Empty) { option (temporal.asyncapi.v1.operation).signal = {workflows: ["W", "Nope"]}; }
}`},
			want: []string{`workflows: "W" is not a workflow rpc of service a.v1.S`, `workflows: "Nope" is not a workflow rpc`},
		},
		{
			name: "duplicate kind and name",
			files: map[string]string{"a.proto": header + `message M {} service S {
  rpc A(M) returns (M) { option (temporal.asyncapi.v1.operation).workflow = {name: "W"}; }
  rpc B(M) returns (M) { option (temporal.asyncapi.v1.operation).workflow = {name: "W"}; }
  rpc W(M) returns (M) { option (temporal.asyncapi.v1.operation).activity = {}; }
}`},
			want: []string{`a.proto:7:3: rpc a.v1.S.B: duplicate workflow "W", already declared by rpc a.v1.S.A at a.proto:6:3`},
		},
		{
			name: "duplicate across files",
			files: map[string]string{
				"a.proto": header + `message M {} service S { rpc Act(M) returns (M) { option (temporal.asyncapi.v1.operation).activity = {}; } }`,
				"c.proto": `syntax = "proto3"; package c.v1; import "temporal/asyncapi/v1/options.proto";
message M {} service T { rpc Act(M) returns (M) { option (temporal.asyncapi.v1.operation).activity = {}; } }`,
			},
			scan: []string{"a.proto", "c.proto"},
			want: []string{`rpc c.v1.T.Act: duplicate activity "Act", already declared by rpc a.v1.S.Act at a.proto:5:`},
		},
		{
			name:  "bad duration",
			files: map[string]string{"a.proto": header + `message M {} service S { rpc R(M) returns (M) { option (temporal.asyncapi.v1.operation).workflow = {run_timeout: "soon"}; } }`},
			want:  []string{`workflow.run_timeout: "soon" is not a duration like "30s" or "1h30m"`},
		},
		{
			name:  "non-positive duration",
			files: map[string]string{"a.proto": header + `message M {} service S { rpc R(M) returns (M) { option (temporal.asyncapi.v1.operation).activity = {retry_policy: {initial_interval: "-1s"}}; } }`},
			want:  []string{"activity.retry_policy.initial_interval must be positive"},
		},
		{
			name:  "bad backoff",
			files: map[string]string{"a.proto": header + `message M {} service S { rpc R(M) returns (M) { option (temporal.asyncapi.v1.operation).activity = {retry_policy: {backoff_coefficient: 0.5}}; } }`},
			want:  []string{"backoff_coefficient must be >= 1"},
		},
		{
			name: "all errors reported together",
			files: map[string]string{"a.proto": header + `message M {} service S {
  rpc A(M) returns (M) { option (temporal.asyncapi.v1.operation) = {}; }
  rpc B(M) returns (M) { option (temporal.asyncapi.v1.operation).signal = {}; }
}`},
			want: []string{"rpc a.v1.S.A:", "rpc a.v1.S.B:"},
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
	ops, err := discover(t, map[string]string{"a.proto": `syntax = "proto3"; package a.v1; message M { string x = 1; } service S { rpc R(M) returns (M); }`})
	if err != nil || len(ops) != 0 {
		t.Errorf("ops = %v, err = %v", ops, err)
	}
}
