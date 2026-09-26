package kafka

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
	"../../proto/kafka",
	"../../examples/kafka/proto",
}

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"example", "", []string{"acme/inventory/v1/inventory.proto"}},
		{"payments", "", []string{"payments/v1/payments.proto"}},
		{"payments_client", "perspective=client", []string{"payments/v1/payments.proto"}},
		{"streams", "", []string{"streams/v1/streams.proto"}},
		{"streams_client", "perspective=client", []string{"streams/v1/streams.proto"}},
		{"payments_3.0.0_json", "asyncapi_version=3.0.0,format=json", []string{"payments/v1/payments.proto"}},
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
import "kafka/asyncapi/v1/annotations.proto";
message Msg { string id = 1; }
`
	doc := func(decl string) string {
		return `option (kafka.asyncapi.v1.document) = {` + decl + `};
`
	}
	sub := func(opt string) string {
		return `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (kafka.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	pub := func(opt string) string {
		return `service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (kafka.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	topic := func(fields string) string { return doc(`topics: [{name: "events", ` + fields + `}]`) }
	keyed := func(key string) string {
		return `message Ev { option (kafka.asyncapi.v1.message) = {topic: "events", key: {` + key + `}}; string id = 1; repeated string tags = 2; }`
	}
	cases := []struct{ name, src, want string }{
		{"client stream with response", `service S { rpc M(stream Msg) returns (Msg) { option (kafka.asyncapi.v1.operation) = {}; } }`,
			"test.proto:7:13: a client streaming rpc with a response has no AsyncAPI form; set (kafka.asyncapi.v1.operation).pattern"},
		{"process with empty", `service S { rpc M(stream Msg) returns (stream google.protobuf.Empty) { option (kafka.asyncapi.v1.operation) = {}; } }`,
			"neither may be google.protobuf.Empty"},
		{"output without process", `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (kafka.asyncapi.v1.operation) = {output: "x"}; } }`,
			"output is only valid for PROCESS operations"},
		{"compacted output without key", topic(`cleanup_policy: [CLEANUP_POLICY_COMPACT]`) + `service S { rpc M(stream Msg) returns (stream Msg) { option (kafka.asyncapi.v1.operation) = {output: "events"}; } }`,
			`output topic "events" is compacted`},
		{"reply without topic", `service S { rpc M(Msg) returns (Msg) { option (kafka.asyncapi.v1.operation) = {}; } }`,
			"REQUEST_REPLY operations must set reply_topic"},
		{"reply on publish", pub(`reply_topic: "replies"`), "only valid for REQUEST_REPLY operations"},
		{"correlation header on publish", pub(`correlation_header: "id"`), "only valid for REQUEST_REPLY operations"},
		{"unknown reply message", `service S { rpc M(Msg) returns (Msg) { option (kafka.asyncapi.v1.operation) = {reply_topic: "replies", reply_messages: ["test.v1.Nope"]}; } }`,
			`reply message "test.v1.Nope" not found`},
		{"bad topic", sub(`topic: "a b"`), `topic "a b" must be 1 to 249 letters`},
		{"dot topic", sub(`topic: ".."`), `topic ".." must be 1 to 249 letters`},
		{"bad parameter", sub(`topic: "events.{a.b}"`), `invalid parameter name "a.b"`},
		{"compacted without key", topic(`cleanup_policy: [CLEANUP_POLICY_COMPACT]`) + pub(`topic: "events"`), `topic "events" is compacted: its records need keys`},
		{"compacted reply without key", topic(`cleanup_policy: [CLEANUP_POLICY_COMPACT]`) + `service S { rpc M(Msg) returns (Msg) { option (kafka.asyncapi.v1.operation) = {reply_topic: "events"}; } }`,
			`reply topic "events" is compacted`},
		{"compacted standalone without key", topic(`cleanup_policy: [CLEANUP_POLICY_COMPACT]`) + `message Ev { option (kafka.asyncapi.v1.message) = {topic: "events"}; }`,
			`topic "events" is compacted`},
		{"idempotent without acks all", pub(`produce: {idempotent: true, acks: ACKS_LEADER}`), "idempotent and transactional producers need acks=all"},
		{"transactional without acks all", pub(`produce: {transactional_id: "tx", acks: ACKS_NONE}`), "idempotent and transactional producers need acks=all"},
		{"producer compression", pub(`produce: {compression: COMPRESSION_PRODUCER}`), "PRODUCER only applies to topics"},
		{"negative batch", pub(`produce: {batch_size: -1}`), "batch_size and max_poll_records must be >= 0"},
		{"bad linger", pub(`produce: {linger: "soon"}`), `produce.linger: "soon" is not a duration`},
		{"auto commit without group", sub(`consume: {auto_commit: true}`), "consume.auto_commit needs a consumer group"},
		{"static member without group", sub(`consume: {group_instance_id: "a"}`), "consume.group_instance_id needs a consumer group"},
		{"assignment with KIP-848", sub(`group_id: "g", consume: {next_gen_rebalance: true, assignment_strategy: ASSIGNMENT_STRATEGY_RANGE}`), "does not apply to the KIP-848 group protocol"},
		{"duplicate topic", doc(`topics: [{name: "events"}, {name: "events"}]`), `topic "events" is declared twice`},
		{"negative partitions", topic(`partitions: -1`), "partitions, replicas, min_insync_replicas and max_message_bytes must be >= 0"},
		{"min isr above replicas", topic(`replicas: 2, min_insync_replicas: 3`), "min_insync_replicas 3 exceeds replicas 2"},
		{"retention bytes", topic(`retention_bytes: -2`), "retention_bytes must be -1 (no limit) or >= 0"},
		{"bad retention", topic(`retention: "forever"`), `retention: "forever" is not a duration`},
		{"repeated cleanup policy", topic(`cleanup_policy: [CLEANUP_POLICY_DELETE, CLEANUP_POLICY_DELETE]`), "cleanup_policy lists an unspecified or repeated policy"},
		{"typed config in configs", topic(`configs: {key: "retention.ms", value: "1"}`), `configs: "retention.ms" has a typed field`},
		{"key field missing", keyed(`field: "nope"`), `key.field: Ev has no field "nope"`},
		{"key field repeated", keyed(`field: "tags"`), `key.field "tags" must not be repeated or a map`},
		{"key message missing", keyed(`message: "test.v1.Nope"`), `key.message "test.v1.Nope" not found`},
		{"key schema invalid", keyed(`schema: '{"type": "text"}'`), "key.schema is not a valid JSON Schema"},
		{"key without kind", keyed(`description: "x"`), "key must set field, message or schema"},
		{"payload encoding without payload location", `message Ev { option (kafka.asyncapi.v1.message) = {topic: "events", schema_id_payload_encoding: "confluent"}; }`,
			"schema_id_payload_encoding requires schema_id_location PAYLOAD"},
		{"unknown kafka server", doc(`servers: [{name: "a"}]`), `kafka server "a" is not declared`},
		{"bad registry url", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
` + doc(`servers: [{name: "a", schema_registry_url: "registry:8081"}]`), `schema_registry_url "registry:8081" must be an http(s) URL`},
		{"auth without scheme", doc(`auth: [{scheme: "x", mechanism: MECHANISM_PLAIN}]`), `kafka auth for "x": the security scheme is not declared`},
		{"auth without mechanism", `option (asyncapi.v3.document) = {security_schemes: [{name: "x", type: SECURITY_SCHEME_TYPE_PLAIN}]};
` + doc(`auth: [{scheme: "x"}]`), `kafka auth for "x": mechanism is required`},
		{"oauthbearer without oauth scheme", `option (asyncapi.v3.document) = {security_schemes: [{name: "x"}]};
` + doc(`auth: [{scheme: "x", mechanism: MECHANISM_OAUTHBEARER}]`), "OAUTHBEARER needs an oauth2 or openIdConnect security scheme"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := golden.Request(t, importPaths, map[string]string{"test.proto": header + tc.src}, "", "test.proto")
			resp := golden.Run(Plugin, req)
			if !strings.Contains(resp.GetError(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", resp.GetError(), tc.want)
			}
		})
	}
}
