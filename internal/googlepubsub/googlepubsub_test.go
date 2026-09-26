package googlepubsub

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
	"../../proto/googlepubsub",
	"../../examples/googlepubsub/proto",
}

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"example", "", []string{"acme/analytics/v1/analytics.proto"}},
		{"analytics", "", []string{"analytics/v1/analytics.proto"}},
		{"analytics_client", "perspective=client", []string{"analytics/v1/analytics.proto"}},
		{"streams", "", []string{"streams/v1/streams.proto"}},
		{"streams_client", "perspective=client", []string{"streams/v1/streams.proto"}},
		{"analytics_3.0.0_json", "asyncapi_version=3.0.0,format=json", []string{"analytics/v1/analytics.proto"}},
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
import "googlepubsub/asyncapi/v1/annotations.proto";
message Msg { string id = 1; }
`
	doc := func(decl string) string {
		return `option (googlepubsub.asyncapi.v1.document) = {` + decl + `};
`
	}
	sub := func(opt string) string {
		return `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (googlepubsub.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	pub := func(opt string) string {
		return `service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (googlepubsub.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	subscription := func(fields string) string {
		return doc(`subscriptions: [{name: "sub", topic: "events", ` + fields + `}]`)
	}
	cases := []struct{ name, src, want string }{
		{"client stream with response", `service S { rpc M(stream Msg) returns (Msg) { option (googlepubsub.asyncapi.v1.operation) = {}; } }`,
			"test.proto:7:13: a client streaming rpc with a response has no AsyncAPI form; set (googlepubsub.asyncapi.v1.operation).pattern"},
		{"process with empty", `service S { rpc M(stream Msg) returns (stream google.protobuf.Empty) { option (googlepubsub.asyncapi.v1.operation) = {}; } }`,
			"neither may be google.protobuf.Empty"},
		{"output without process", `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (googlepubsub.asyncapi.v1.operation) = {output: "x"}; } }`,
			"output is only valid for PROCESS operations"},
		{"invalid output", `service S { rpc M(stream Msg) returns (stream Msg) { option (googlepubsub.asyncapi.v1.operation) = {output: "goog-x"}; } }`,
			`"goog-x"`},
		{"reply without topic", `service S { rpc M(Msg) returns (Msg) { option (googlepubsub.asyncapi.v1.operation) = {}; } }`,
			"REQUEST_REPLY operations must set reply_topic"},
		{"reply on publish", pub(`reply_topic: "replies"`), "reply_topic and reply_messages are only valid for REQUEST_REPLY operations"},
		{"unknown reply message", `service S { rpc M(Msg) returns (Msg) { option (googlepubsub.asyncapi.v1.operation) = {reply_topic: "replies", reply_messages: ["test.v1.Nope"]}; } }`,
			`reply message "test.v1.Nope" not found`},
		{"short topic", sub(`topic: "ab"`), `topic "ab" must be 3 to 255 letters`},
		{"topic starting with digit", sub(`topic: "1events"`), `topic "1events" must be 3 to 255 letters`},
		{"goog topic", sub(`topic: "google-events"`), `topic "google-events" must not start with "goog"`},
		{"bad parameter", sub(`topic: "events.{a.b}"`), `invalid parameter name "a.b"`},
		{"unbalanced topic", sub(`topic: "events.{a"`), "has unbalanced braces"},
		{"subscription on other topic", subscription(``) + sub(`topic: "orders", subscription: "sub"`), `subscription "sub" is attached to topic "events", not "orders"`},
		{"consume export", subscription(`bigquery: {table: "p.d.t"}`) + sub(`topic: "events", subscription: "sub"`), `subscription "sub" exports to bigquery and has no subscribers`},
		{"consume settings on push", subscription(`push: {endpoint: "https://h.example/x"}`) + sub(`topic: "events", subscription: "sub", consume: {synchronous_pull: true}`),
			`consume settings do not apply to push subscription "sub"`},
		{"goog attribute", pub(`publish: {attributes: {key: "googlish", value: "x"}}`), `publish attribute "googlish"`},
		{"negative batching", pub(`publish: {batch_max_messages: -1}`), "batching and flow control limits must be >= 0"},
		{"bad latency", pub(`publish: {batch_max_latency: "soon"}`), `publish.batch_max_latency: "soon" is not a duration`},
		{"bad ordering key", pub(`publish: {ordering_key: "{a"}`), `publish.ordering_key "{a" has unbalanced braces`},
		{"duplicate topic", doc(`topics: [{name: "events"}, {name: "events"}]`), `test.proto: topic "events": is declared twice`},
		{"topic retention range", doc(`topics: [{name: "events", message_retention_duration: "1m"}]`), "message_retention_duration must be between 10m0s and 31d"},
		{"in transit without regions", doc(`topics: [{name: "events", enforce_in_transit: true}]`), "enforce_in_transit requires allowed_persistence_regions"},
		{"schema without name", doc(`topics: [{name: "events", schema: {encoding: ENCODING_JSON}}]`), "schema.name is required"},
		{"bad label", doc(`topics: [{name: "events", labels: {key: "Team", value: "x"}}]`), `label "Team" must be lowercase`},
		{"duplicate subscription", doc(`subscriptions: [{name: "sub", topic: "events"}, {name: "sub", topic: "events"}]`), `subscription "sub": is declared twice`},
		{"subscription without topic", doc(`subscriptions: [{name: "sub"}]`), "topic is required"},
		{"ack deadline range", subscription(`ack_deadline: "5s"`), "ack_deadline must be between 10s and 10m0s"},
		{"subscription retention range", subscription(`message_retention_duration: "240h"`), "message_retention_duration must be between 10m0s and 7d"},
		{"expiration too short", subscription(`expiration: "1h"`), "expiration must be between 1d and 365d"},
		{"dead letter same topic", subscription(`dead_letter_policy: {topic: "events"}`), "dead_letter_policy.topic must differ from the subscription's topic"},
		{"dead letter attempts", subscription(`dead_letter_policy: {topic: "dead", max_delivery_attempts: 3}`), "max_delivery_attempts must be between 5 and 100"},
		{"retry order", subscription(`retry_policy: {minimum_backoff: "60s", maximum_backoff: "10s"}`), "minimum_backoff must not exceed maximum_backoff"},
		{"retry range", subscription(`retry_policy: {maximum_backoff: "700s"}`), "retry_policy.maximum_backoff must be between 0 and 10m0s"},
		{"exactly once push", subscription(`enable_exactly_once_delivery: true, push: {endpoint: "https://h.example/x"}`), "exactly-once delivery is only available to pull subscriptions"},
		{"http push", subscription(`push: {endpoint: "http://h.example/x"}`), `push.endpoint "http://h.example/x" must be an https URL`},
		{"audience without account", subscription(`push: {endpoint: "https://h.example/x", audience: "a"}`), "push.audience requires service_account_email"},
		{"metadata without no_wrapper", subscription(`push: {endpoint: "https://h.example/x", write_metadata: true}`), "push.write_metadata requires no_wrapper"},
		{"bad table", subscription(`bigquery: {table: "clicks"}`), `bigquery.table "clicks" must look like`},
		{"both schemas", subscription(`bigquery: {table: "p.d.t", use_topic_schema: true, use_table_schema: true}`), "use_topic_schema and use_table_schema are mutually exclusive"},
		{"topic schema missing", subscription(`bigquery: {table: "p.d.t", use_topic_schema: true}`), `bigquery.use_topic_schema requires topic "events" to have a schema`},
		{"bucket required", subscription(`cloud_storage: {}`), "cloud_storage.bucket is required"},
		{"storage duration", subscription(`cloud_storage: {bucket: "b", max_duration: "30s"}`), "cloud_storage.max_duration must be between 1m0s and 10m0s"},
		{"storage bytes", subscription(`cloud_storage: {bucket: "b", max_bytes: 10}`), "cloud_storage.max_bytes must be between 1 KiB and 10 GiB"},
		{"unknown server", doc(`servers: [{name: "a"}]`), `googlepubsub server "a" is not declared`},
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

func TestConflictingProjects(t *testing.T) {
	sources := map[string]string{
		"a.proto": `syntax = "proto3"; package a; import "googlepubsub/asyncapi/v1/annotations.proto";
option (googlepubsub.asyncapi.v1.document) = {project: "one"};`,
		"b.proto": `syntax = "proto3"; package b; import "googlepubsub/asyncapi/v1/annotations.proto";
option (googlepubsub.asyncapi.v1.document) = {project: "two"};`,
	}
	resp := golden.Run(Plugin, golden.Request(t, importPaths, sources, "merge=true", "a.proto", "b.proto"))
	if want := `project "two" differs from project "one"`; !strings.Contains(resp.GetError(), want) {
		t.Fatalf("error = %q, want it to contain %q", resp.GetError(), want)
	}
}
