package nats

import (
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/golden"
)

var update = flag.Bool("update", false, "update golden files")

// importPaths are the directories test protos are resolved against.
var importPaths = []string{
	"testdata/protos",
	"../../proto/asyncapi",
	"../../proto/nats",
	"../../examples/nats/proto",
}

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"orders", "", []string{"acme/orders/v1/orders.proto"}},
		{"orders_client", "perspective=client", []string{"acme/orders/v1/orders.proto"}},
		{"orders_json", "format=json,asyncapi_version=3.0.0,version=9.9.9", []string{"acme/orders/v1/orders.proto"}},
		{"orders_protobuf", "payload=protobuf", []string{"acme/orders/v1/orders.proto"}},
		{"orders_jsonschema", "format=jsonschema", []string{"acme/orders/v1/orders.proto"}},
		{"types", "", []string{"types/v1/types.proto"}},
		{"types_proto_names", "json_names=false,enum_values=numbers", []string{"types/v1/types.proto"}},
		{"types_annotated", "enum_values=both,proto_types=true", []string{"types/v1/types.proto"}},
		{"inventory", "", []string{"inventory/v1/inventory.proto"}},
		{"inventory_client", "perspective=client", []string{"inventory/v1/inventory.proto"}},
		{"merged", "merge=true,merge_file_name=shop,include_all=true", []string{
			"inventory/v1/inventory.proto", "billing/v1/billing.proto", "plain/plain.proto",
		}},
		{"per_file", "include_all=true", []string{"billing/v1/billing.proto", "plain/plain.proto", "shared/v1/nats.proto"}},
		{"protobuf_printer", "payload=protobuf,include_all=true", []string{"billing/v1/billing.proto", "plain/plain.proto", "types/v1/types.proto"}},
		{"services_filter", "include_all=true,services=inventory.**", []string{"inventory/v1/inventory.proto", "billing/v1/billing.proto"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := golden.Run(Plugin, golden.Request(t, importPaths, nil, tc.param, tc.files...))
			golden.Check(t, filepath.Join("testdata", "golden", tc.name), resp, *update, importPaths)
		})
	}
}

func TestErrors(t *testing.T) {
	const header = `syntax = "proto3";
package test.v1;
import "google/protobuf/empty.proto";
import "asyncapi/v3/annotations.proto";
import "nats/asyncapi/v1/annotations.proto";
message Msg { string id = 1; }
`
	cases := []struct {
		name, param, src, want string
	}{
		{"bidi", "", `service S { rpc M(stream Msg) returns (stream Msg) { option (nats.asyncapi.v1.operation) = {}; } }`,
			"test.proto:7:13: cannot infer the NATS pattern of a bidirectional streaming method"},
		{"empty token", "", `service S { rpc M(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {subject: "a..b"}; } }`,
			`subject "a..b" has an empty token`},
		{"partial param", "", `service S { rpc M(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {subject: "a.x{id}"}; } }`,
			"must span the whole token"},
		{"wildcard position", "", `service S { rpc M(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {subject: "a.>.b"}; } }`,
			"must be the last token"},
		{"unknown server", "", `service S { option (asyncapi.v3.service) = {servers: ["nope"]}; option (nats.asyncapi.v1.service) = {}; rpc M(Msg) returns (Msg); }`,
			`unknown server "nope"`},
		{"unknown security", "", `service S { rpc M(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {}; option (asyncapi.v3.operation) = {security: ["nope"]}; } }`,
			`unknown security scheme "nope"`},
		{"duplicate operation id", "", `service S {
			rpc A(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {}; option (asyncapi.v3.operation) = {operation_id: "x"}; }
			rpc B(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {}; option (asyncapi.v3.operation) = {operation_id: "x"}; } }`,
			`operation id "x" is already used by test.v1.S.A`},
		{"consumer without stream", "", `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (nats.asyncapi.v1.operation) = {consumer: {}}; } }`,
			"is not stored in a JetStream stream"},
		{"invalid duration", "", `option (nats.asyncapi.v1.document) = {streams: [{name: "S", subjects: ["a.>"], max_age: "5 minutes"}]};`,
			`stream "S": max_age: invalid duration "5 minutes"`},
		{"invalid size", "", `option (nats.asyncapi.v1.document) = {streams: [{name: "S", subjects: ["a.>"], max_bytes: "lots"}]};`,
			`max_bytes: invalid size "lots"`},
		{"stream mismatch", "", `option (nats.asyncapi.v1.document) = {streams: [{name: "S", subjects: ["a.>"]}]};
			service S { rpc M(Msg) returns (google.protobuf.Empty) { option (nats.asyncapi.v1.operation) = {subject: "b.c", stream: "S"}; } }`,
			`subject "b.c" is not captured by the subjects ["a.>"] of stream "S"`},
		{"ttl requires allow_msg_ttl", "", `option (nats.asyncapi.v1.document) = {streams: [{name: "S", subjects: ["a.>"]}]};
			service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (nats.asyncapi.v1.operation) = {subject: "a.b", publish: {ttl: true}}; } }`,
			`stream "S" must set allow_msg_ttl`},
		{"ordered durable", "", `option (nats.asyncapi.v1.document) = {streams: [{name: "S", subjects: ["a.>"]}]};
			service S { rpc M(Msg) returns (google.protobuf.Empty) { option (nats.asyncapi.v1.operation) = {subject: "a.b", consumer: {kind: CONSUMER_KIND_ORDERED, durable: "d"}}; } }`,
			"ordered consumers are ephemeral and cannot be durable"},
		{"invalid example", "", `message Ex { string a = 1 [(asyncapi.v3.field) = {examples: ["abc"]}]; }
			service S { rpc M(Ex) returns (Msg); option (nats.asyncapi.v1.service) = {}; }`,
			"example 1 is not valid JSON"},
		{"unknown parameter", "", `service S { rpc M(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {subject: "a.{id}"}; option (asyncapi.v3.operation) = {channel: {parameters: [{name: "nope"}]}}; } }`,
			`parameter "nope" does not appear in address "a.{id}"`},
		{"unknown reply message", "", `service S { rpc M(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {reply_messages: ["test.v1.Nope"]}; } }`,
			`reply message "test.v1.Nope" not found`},
		{"reply on publish", "", `service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (nats.asyncapi.v1.operation) = {reply_subject: "x"}; } }`,
			"only valid for REQUEST_REPLY operations"},
		{"bad correlation id", "", `message Ev { option (nats.asyncapi.v1.message) = {subject: "ev"}; option (asyncapi.v3.message) = {correlation_id: "header.x"}; }`,
			"is not a runtime expression"},
		{"duplicate server", "", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}, {name: "a", host: "h"}]};`,
			`server "a" is already declared`},
		{"unknown nats server", "", `option (nats.asyncapi.v1.document) = {servers: [{name: "a", tls: true}]};`,
			`nats server "a" is not declared`},
		{"auth without scheme", "", `option (nats.asyncapi.v1.document) = {auth: [{scheme: "x", type: AUTH_TYPE_JWT}]};`,
			`nats auth for "x": the security scheme is not declared`},
		{"scheme without type", "", `option (asyncapi.v3.document) = {security_schemes: [{name: "x"}]};`,
			`unknown security scheme type ""`},
		{"bad binding", "", `service S { rpc M(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {}; option (asyncapi.v3.operation) = {bindings: [{protocol: "temporal", json: "{}"}]}; } }`,
			`"temporal" is not a operation binding of AsyncAPI 3.1.0`},
		{"bad extension", "", `service S { rpc M(Msg) returns (Msg) { option (nats.asyncapi.v1.operation) = {}; option (asyncapi.v3.operation) = {extensions: [{key: "team", value: "\"a\""}]}; } }`,
			`extension "team" must start with "x-"`},
		{"invalid option value", "format=xml", ``, `invalid value "xml" for option "format"`},
		{"unknown option", "colour=blue", ``, `unknown option "colour"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := golden.Request(t, importPaths, map[string]string{"test.proto": header + tc.src}, tc.param, "test.proto")
			resp := golden.Run(Plugin, req)
			if !strings.Contains(resp.GetError(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", resp.GetError(), tc.want)
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
