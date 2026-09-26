package sqs

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
	"../../proto/sqs",
	"../../examples/sqs/proto",
}

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"example", "", []string{"acme/returns/v1/returns.proto"}},
		{"fulfilment", "", []string{"fulfilment/v1/fulfilment.proto"}},
		{"fulfilment_client", "perspective=client", []string{"fulfilment/v1/fulfilment.proto"}},
		{"streams", "", []string{"streams/v1/streams.proto"}},
		{"streams_client", "perspective=client", []string{"streams/v1/streams.proto"}},
		{"fulfilment_3.0.0_json", "asyncapi_version=3.0.0,format=json", []string{"fulfilment/v1/fulfilment.proto"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := golden.Run(Plugin, golden.Request(t, importPaths, nil, tc.param, tc.files...))
			golden.Check(t, filepath.Join("testdata", "golden", tc.name), resp, *update, importPaths)
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

func TestErrors(t *testing.T) {
	const header = `syntax = "proto3";
package test.v1;
import "google/protobuf/empty.proto";
import "asyncapi/v3/annotations.proto";
import "sqs/asyncapi/v1/annotations.proto";
message Msg { string id = 1; }
`
	doc := func(decl string) string {
		return `option (sqs.asyncapi.v1.document) = {` + decl + `};
`
	}
	queue := func(fields string) string { return doc(`queues: [{name: "q", ` + fields + `}]`) }
	fifo := func(fields string) string { return doc(`queues: [{name: "q.fifo", ` + fields + `}]`) }
	sub := func(opt string) string {
		return `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (sqs.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	pub := func(opt string) string {
		return `service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (sqs.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	cases := []struct{ name, src, want string }{
		{"client stream with response", `service S { rpc M(stream Msg) returns (Msg) { option (sqs.asyncapi.v1.operation) = {}; } }`,
			"test.proto:7:13: a client streaming rpc with a response has no AsyncAPI form; set (sqs.asyncapi.v1.operation).pattern"},
		{"process with empty", `service S { rpc M(stream Msg) returns (stream google.protobuf.Empty) { option (sqs.asyncapi.v1.operation) = {}; } }`,
			"neither may be google.protobuf.Empty"},
		{"output without process", `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (sqs.asyncapi.v1.operation) = {output: "x"}; } }`,
			"output is only valid for PROCESS operations"},
		{"fifo output without group", `service S { rpc M(stream Msg) returns (stream Msg) { option (sqs.asyncapi.v1.operation) = {output: "q.fifo"}; } }`,
			`output: queue "q.fifo" is FIFO: set send.message_group_id`},
		{"reply without queue", `service S { rpc M(Msg) returns (Msg) { option (sqs.asyncapi.v1.operation) = {}; } }`, "REQUEST_REPLY operations must set reply_queue"},
		{"reply on publish", pub(`queue: "q", reply_queue: "r"`), "reply_queue and reply_messages are only valid for REQUEST_REPLY operations"},
		{"unknown reply message", `service S { rpc M(Msg) returns (Msg) { option (sqs.asyncapi.v1.operation) = {reply_queue: "r", reply_messages: ["test.v1.Nope"]}; } }`,
			`reply message "test.v1.Nope" not found`},
		{"bad queue name", sub(`queue: "a.b"`), `queue "a.b" must be up to 80 letters`},
		{"long queue name", sub(`queue: "` + strings.Repeat("a", 81) + `"`), "must be up to 80 letters"},
		{"bad parameter", sub(`queue: "q-{a.b}"`), `invalid parameter name "a.b"`},
		{"fifo without group", pub(`queue: "q.fifo", send: {deduplication_id: "{id}"}`), `queue "q.fifo" is FIFO: set send.message_group_id`},
		{"fifo without dedup", pub(`queue: "q.fifo", send: {message_group_id: "g"}`), "is FIFO without content-based deduplication: set send.deduplication_id"},
		{"fifo content dedup", fifo(`content_based_deduplication: true`) + pub(`queue: "q.fifo", send: {message_group_id: "g"}`), ""},
		{"group on standard", pub(`queue: "q", send: {message_group_id: "g"}`), "message_group_id and deduplication_id only apply to FIFO queues"},
		{"delay on fifo", pub(`queue: "q.fifo", send: {message_group_id: "g", deduplication_id: "d", delay: "5s"}`), "FIFO queues have no per-message delay"},
		{"delay range", pub(`queue: "q", send: {delay: "16m"}`), "send.delay must be between 0s and 15m0s"},
		{"max messages", sub(`queue: "q", receive: {max_messages: 11}`), "receive.max_messages must be between 1 and 10"},
		{"wait time", sub(`queue: "q", receive: {wait_time: "30s"}`), "receive.wait_time must be between 0s and 20s"},
		{"unbalanced group", pub(`queue: "q.fifo", send: {message_group_id: "{a", deduplication_id: "d"}`), `send.message_group_id "{a" has unbalanced braces`},
		{"duplicate queue", doc(`queues: [{name: "q"}, {name: "q"}]`), `queue "q" is declared twice`},
		{"fifo options on standard", queue(`content_based_deduplication: true`), "only apply to FIFO queues"},
		{"high throughput scope", fifo(`fifo_throughput_limit: FIFO_THROUGHPUT_LIMIT_PER_MESSAGE_GROUP_ID`), "PER_MESSAGE_GROUP_ID requires deduplication_scope MESSAGE_GROUP"},
		{"message size", queue(`maximum_message_size: 512`), "maximum_message_size must be between 1 KiB and 1 MiB"},
		{"receive count without dlq", queue(`max_receive_count: 3`), "max_receive_count requires dead_letter_queue"},
		{"receive count range", queue(`dead_letter_queue: "d", max_receive_count: 2000`), "max_receive_count must be between 1 and 1000"},
		{"own dlq", queue(`dead_letter_queue: "q"`), "a queue cannot be its own dead-letter queue"},
		{"dlq type", queue(`dead_letter_queue: "d.fifo"`), `dead_letter_queue "d.fifo" must be a standard queue`},
		{"sse and kms", queue(`sqs_managed_sse: true, kms_key: "k"`), "sqs_managed_sse and kms_key are mutually exclusive"},
		{"reuse without kms", queue(`kms_data_key_reuse: "5m"`), "kms_data_key_reuse requires kms_key"},
		{"delay range queue", queue(`delivery_delay: "20m"`), "delivery_delay must be between"},
		{"retention range", queue(`message_retention: "30s"`), "message_retention must be between 1m0s"},
		{"long polling range", queue(`receive_wait_time: "21s"`), "receive_wait_time must be between"},
		{"redrive without permission", queue(`redrive_allow: {}`), "redrive_allow.permission is required"},
		{"redrive by queue without sources", queue(`redrive_allow: {permission: REDRIVE_PERMISSION_BY_QUEUE}`), "redrive_allow BY_QUEUE needs 1 to 10 source_queues"},
		{"redrive sources on allow all", queue(`redrive_allow: {permission: REDRIVE_PERMISSION_ALLOW_ALL, source_queues: ["a"]}`), "source_queues only apply to permission BY_QUEUE"},
		{"policy without effect", queue(`policy: [{principals: ["*"], actions: ["sqs:SendMessage"]}]`), "policy[0]: effect is required"},
		{"policy non-sqs action", queue(`policy: [{effect: EFFECT_ALLOW, principals: ["*"], actions: ["sns:Publish"]}]`), `action "sns:Publish" is not an SQS action`},
		{"unknown sqs server", doc(`servers: [{name: "a"}]`), `sqs server "a" is not declared`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := golden.Request(t, importPaths, map[string]string{"test.proto": header + tc.src}, "", "test.proto")
			resp := golden.Run(Plugin, req)
			if tc.want == "" {
				if resp.Error != nil {
					t.Fatalf("unexpected error %q", resp.GetError())
				}
				return
			}
			if !strings.Contains(resp.GetError(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", resp.GetError(), tc.want)
			}
		})
	}
}

func TestConflictingRegions(t *testing.T) {
	sources := map[string]string{
		"a.proto": `syntax = "proto3"; package a; import "sqs/asyncapi/v1/annotations.proto";
option (sqs.asyncapi.v1.document) = {region: "eu-west-1"};`,
		"b.proto": `syntax = "proto3"; package b; import "sqs/asyncapi/v1/annotations.proto";
option (sqs.asyncapi.v1.document) = {region: "us-east-1"};`,
	}
	resp := golden.Run(Plugin, golden.Request(t, importPaths, sources, "merge=true", "a.proto", "b.proto"))
	if want := `region "us-east-1" differs from region "eu-west-1"`; !strings.Contains(resp.GetError(), want) {
		t.Fatalf("error = %q, want it to contain %q", resp.GetError(), want)
	}
}
