package redis

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
	"../../proto/redis",
	"../../examples/redis/proto",
}

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"example", "", []string{"acme/notify/v1/notify.proto"}},
		{"chat", "", []string{"chat/v1/chat.proto"}},
		{"chat_client", "perspective=client", []string{"chat/v1/chat.proto"}},
		{"jobs", "", []string{"jobs/v1/jobs.proto"}},
		{"jobs_client", "perspective=client,asyncapi_version=3.0.0", []string{"jobs/v1/jobs.proto"}},
		{"sessions", "", []string{"sessions/v1/sessions.proto"}},
		{"sessions_client", "perspective=client,format=json", []string{"sessions/v1/sessions.proto"}},
		{"merged", "merge=true,merge_file_name=redis", []string{"chat/v1/chat.proto", "jobs/v1/jobs.proto", "sessions/v1/sessions.proto"}},
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
import "redis/asyncapi/v1/annotations.proto";
message Msg { string id = 1; }
`
	op := func(opt string) string {
		return `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (redis.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	cases := []struct {
		name, param, src, want string
	}{
		{"bidi", "", `service S { rpc M(stream Msg) returns (stream Msg) { option (redis.asyncapi.v1.operation) = {}; } }`,
			"test.proto:7:13: cannot infer the Redis pattern of a bidirectional streaming rpc"},
		{"whitespace", "", op(`channel: "a b"`), `channel "a b" must not contain whitespace`},
		{"unmatched brace", "", op(`channel: "a:{id"`), `channel "a:{id" has an unmatched '{'`},
		{"bad parameter", "", op(`channel: "a:{i.d}"`), `invalid parameter name "i.d"`},
		{"duplicate parameter", "", op(`channel: "{id}:{id}"`), `parameter "id" is used twice`},
		{"glob on publish", "", `service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (redis.asyncapi.v1.operation) = {channel: "a:*"}; } }`,
			`channel "a:*": glob patterns are only valid for Pub/Sub subscriptions`},
		{"glob on stream", "", op(`channel: "a:*", stream: {}`), "glob patterns are only valid for Pub/Sub subscriptions"},
		{"glob on sharded", "", op(`channel: "a:*", pubsub: {sharded: true}`), "glob patterns are only valid for Pub/Sub subscriptions"},
		{"transport conflict", "", `service S {
			rpc A(Msg) returns (google.protobuf.Empty) { option (redis.asyncapi.v1.operation) = {channel: "a", stream: {}}; }
			rpc B(Msg) returns (google.protobuf.Empty) { option (redis.asyncapi.v1.operation) = {channel: "a", list: {}}; } }`,
			`channel "a" is used both as stream and as list`},
		{"consumer without group", "", op(`stream: {consumer: "c"}`), "stream.consumer requires a consumer group"},
		{"dead letter without group", "", op(`stream: {dead_letter: "d"}`), "stream.dead_letter requires a consumer group"},
		{"dead letter without max deliveries", "", op(`stream: {group: "g", dead_letter: "d"}`), "stream.dead_letter requires max_deliveries"},
		{"bad start id", "", op(`stream: {start_id: "latest"}`), `stream.start_id "latest" is not`},
		{"bad block", "", op(`stream: {block: "soon"}`), `stream.block: "soon" is not a duration`},
		{"negative claim", "", op(`stream: {group: "g", claim_min_idle: "-1s"}`), "stream.claim_min_idle must be positive"},
		{"trim without strategy", "", op(`stream: {trim: {exact: true}}`), "stream.trim must set max_len or min_id"},
		{"exact trim with limit", "", op(`stream: {trim: {max_len: 5, exact: true, limit: 10}}`), "stream.trim.limit only applies to approximate trimming"},
		{"bad min id", "", op(`stream: {trim: {min_id: "yesterday"}}`), `stream.trim.min_id "yesterday"`},
		{"field and flatten", "", op(`stream: {field: "data", flatten: true}`), "stream.field and stream.flatten are mutually exclusive"},
		{"entry conflict", "", `service S {
			rpc A(Msg) returns (google.protobuf.Empty) { option (redis.asyncapi.v1.operation) = {channel: "a", stream: {field: "data"}}; }
			rpc B(Msg) returns (google.protobuf.Empty) { option (redis.asyncapi.v1.operation) = {channel: "a", stream: {flatten: true}}; } }`,
			`stream "a" already stores entries in field "data"`},
		{"group start conflict", "", `service S {
			rpc A(Msg) returns (google.protobuf.Empty) { option (redis.asyncapi.v1.operation) = {channel: "a", stream: {group: "g", start_id: "0"}}; }
			rpc B(Msg) returns (google.protobuf.Empty) { option (redis.asyncapi.v1.operation) = {channel: "a", stream: {group: "g", start_id: "$"}}; } }`,
			`consumer group "g" is created at "0" and "$"`},
		{"negative list max len", "", op(`list: {max_len: -1}`), "list.max_len must be >= 0"},
		{"reply on publish", "", `service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (redis.asyncapi.v1.operation) = {reply_channel: "x"}; } }`,
			"only valid for REQUEST_REPLY operations"},
		{"unknown reply message", "", `service S { rpc M(Msg) returns (Msg) { option (redis.asyncapi.v1.operation) = {reply_messages: ["test.v1.Nope"]}; } }`,
			`reply message "test.v1.Nope" not found`},
		{"glob reply", "", `service S { rpc M(Msg) returns (Msg) { option (redis.asyncapi.v1.operation) = {reply_channel: "r:*"}; } }`,
			`reply_channel "r:*" must not be a glob pattern`},
		{"keyspace without key", "", `service S { option (redis.asyncapi.v1.service) = {keyspace: [{events: ["del"]}]}; }`,
			"keyspace[0]: KEYSPACE notifications require a key"},
		{"keyevent without events", "", `service S { option (redis.asyncapi.v1.service) = {keyspace: [{kind: KEYSPACE_KIND_KEYEVENT}]}; }`,
			"keyspace[0]: KEYEVENT notifications require events"},
		{"bad event", "", `service S { option (redis.asyncapi.v1.service) = {keyspace: [{key: "k", events: ["Expired!"]}]}; }`,
			`event "Expired!" is not an event name`},
		{"duplicate keyspace", "", `service S { option (redis.asyncapi.v1.service) = {keyspace: [{name: "n", key: "a"}, {name: "n", key: "b"}]}; }`,
			`operation id "S.n" is already used`},
		{"unknown redis server", "", `option (redis.asyncapi.v1.document) = {servers: [{name: "a", tls: true}]};`,
			`redis server "a" is not declared`},
		{"cluster database", "", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
			option (redis.asyncapi.v1.document) = {servers: [{name: "a", mode: MODE_CLUSTER, database: 3}]};`,
			"Redis Cluster only has database 0"},
		{"sentinel without master", "", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
			option (redis.asyncapi.v1.document) = {servers: [{name: "a", mode: MODE_SENTINEL}]};`,
			"mode MODE_SENTINEL requires sentinel_master"},
		{"master without sentinel", "", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
			option (redis.asyncapi.v1.document) = {servers: [{name: "a", sentinel_master: "m"}]};`,
			"sentinel_master requires mode MODE_SENTINEL"},
		{"bad resp", "", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
			option (redis.asyncapi.v1.document) = {servers: [{name: "a", resp: 4}]};`,
			"resp must be 2 or 3"},
		{"bad keyspace flags", "", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
			option (redis.asyncapi.v1.document) = {servers: [{name: "a", notify_keyspace_events: "Exq"}]};`,
			`notify_keyspace_events "Exq" has unknown flags`},
		{"auth without scheme", "", `option (redis.asyncapi.v1.document) = {auth: [{scheme: "x", type: AUTH_TYPE_ACL}]};`,
			`redis auth for "x": the security scheme is not declared`},
		{"auth without type", "", `option (asyncapi.v3.document) = {security_schemes: [{name: "x", type: SECURITY_SCHEME_TYPE_USER_PASSWORD}]};
			option (redis.asyncapi.v1.document) = {auth: [{scheme: "x"}]};`,
			`redis auth for "x": type is required`},
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

func TestAddress(t *testing.T) {
	a, err := parseAddress("chat:{room}:*:{user}")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := a.pattern(), "chat:*:*:*"; got != want {
		t.Errorf("pattern = %q, want %q", got, want)
	}
	if !a.glob || len(a.params) != 2 {
		t.Errorf("glob = %v, params = %v", a.glob, a.params)
	}
	if b, _ := parseAddress("orders:{id}"); b.glob {
		t.Error("parameters alone are not globs")
	}
	for in, want := range map[string]string{
		"chat:rooms:{room}": "chat.rooms.room",
		"moderation:*":      "moderation.any",
		"a?b":               "aoneb",
		"x[ab]":             "x_ab",
	} {
		if got := channelID(in); got != want {
			t.Errorf("channelID(%q) = %q, want %q", in, got, want)
		}
	}
	if got := joinKey("", ":a:", "b"); got != "a:b" {
		t.Errorf("joinKey = %q", got)
	}
}
