package temporal

import (
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/golden"
)

var update = flag.Bool("update", false, "update golden files")

var importPaths = []string{
	"testdata/protos",
	"../../proto/asyncapi",
	"../../proto/temporal",
	"../../examples/temporal/proto",
}

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"example", "", []string{"acme/shop/v1/orders.proto"}},
		{"example_client", "perspective=client", []string{"acme/shop/v1/orders.proto"}},
		{"fulfillment", "", []string{"acme/fulfillment/v1/fulfillment.proto"}},
		{"payments", "", []string{"acme/payments/v1/payments.proto"}},
		{"cart", "", []string{"acme/cart/v1/cart.proto"}},
		{"signup", "", []string{"acme/signup/v1/signup.proto"}},
		{"signup_trimmed", "trim_unused_schemas=true,protovalidate=false", []string{"acme/signup/v1/signup.proto"}},
		{"ping_options", "title=Ping API,description=Pings a worker.,id=urn:acme:ping,server_url=https://temporal.acme.example:7233,namespace=pings,perspective=worker,enums_as_ints=true", []string{"ping/v1/ping.proto"}},
		{"ping_json", "format=json,asyncapi_version=3.0.0,json_names=false,version=9.9.9", []string{"ping/v1/ping.proto"}},
		{"ping_protobuf", "payload=protobuf", []string{"ping/v1/ping.proto"}},
		{"per_file", "", []string{"acme/billing/v1/billing.proto", "acme/shipping/v1/shipping.proto"}},
		{"merged", "merge=true,merge_file_name=acme,version=1.2.3", []string{"acme/billing/v1/billing.proto", "acme/shipping/v1/shipping.proto"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := golden.Run(Plugin, golden.Request(t, importPaths, nil, tc.param, tc.files...))
			golden.Check(t, filepath.Join("testdata", "golden", tc.name), resp, *update, importPaths)
		})
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
		{"unknown temporal server", "", []string{"bad_server.proto"}, `temporal server "nope" is not declared`},
		{"invalid option value", "format=xml", []string{"ping/v1/ping.proto"}, `invalid value "xml" for option "format"`},
		{"server_url without host", "server_url=https://", []string{"ping/v1/ping.proto"}, `server_url "https://" has no host`},
		{"unknown option", "colour=blue", []string{"ping/v1/ping.proto"}, `unknown option "colour"`},
	}
	sources := map[string]string{"bad_server.proto": `syntax = "proto3"; package b; import "temporal/asyncapi/v1/options.proto";
option (temporal.asyncapi.v1.document) = {servers: [{name: "nope", namespace: "x"}]};
message M {} service S { rpc W(M) returns (M) { option (temporal.asyncapi.v1.operation).workflow = {}; } }`}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := golden.Run(Plugin, golden.Request(t, importPaths, sources, tc.param, tc.files...))
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
	req := golden.Request(t, importPaths, map[string]string{
		"plain.proto": `syntax = "proto3"; package p; message M {} service S { rpc A(M) returns (M); }`,
	}, "", "plain.proto")
	resp := golden.Run(Plugin, req)
	if resp.Error != nil || len(resp.File) != 0 {
		t.Fatalf("want no output for files without annotations, got error=%q files=%d", resp.GetError(), len(resp.File))
	}
}
