package mqtt

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
	"../../proto/mqtt",
	"../../examples/mqtt/proto",
}

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"example", "", []string{"acme/sensors/v1/sensors.proto"}},
		{"devices", "", []string{"iot/v1/devices.proto"}},
		{"devices_client", "perspective=client", []string{"iot/v1/devices.proto"}},
		{"streams", "", []string{"streams/v1/streams.proto"}},
		{"streams_client", "perspective=client", []string{"streams/v1/streams.proto"}},
		{"devices_3.0.0_json", "asyncapi_version=3.0.0,format=json", []string{"iot/v1/devices.proto"}},
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
import "mqtt/asyncapi/v1/annotations.proto";
message Msg { string id = 1; }
`
	// v3 declares a single MQTT 3.1.1 server, which disables MQTT 5
	// features.
	const v3 = `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
option (mqtt.asyncapi.v1.document) = {servers: [{name: "a", version: VERSION_3_1_1}]};
`
	server := func(fields string) string {
		return `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
option (mqtt.asyncapi.v1.document) = {servers: [{name: "a", ` + fields + `}]};
`
	}
	sub := func(opt string) string {
		return `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (mqtt.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	pub := func(opt string) string {
		return `service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (mqtt.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	rpc := `service S { rpc M(Msg) returns (Msg) { option (mqtt.asyncapi.v1.operation) = {}; } }`
	cases := []struct{ name, src, want string }{
		{"client stream with response", `service S { rpc M(stream Msg) returns (Msg) { option (mqtt.asyncapi.v1.operation) = {}; } }`,
			"test.proto:7:13: a client streaming rpc with a response has no AsyncAPI form; set (mqtt.asyncapi.v1.operation).pattern"},
		{"process with empty", `service S { rpc M(stream Msg) returns (stream google.protobuf.Empty) { option (mqtt.asyncapi.v1.operation) = {}; } }`,
			"neither may be google.protobuf.Empty"},
		{"output without process", `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (mqtt.asyncapi.v1.operation) = {output: "x"}; } }`,
			"output is only valid for PROCESS operations"},
		{"wildcard output", `service S { rpc M(stream Msg) returns (stream Msg) { option (mqtt.asyncapi.v1.operation) = {output: "a/+"}; } }`,
			`output: topic "a/+": wildcards are only valid in subscriptions`},
		{"hash not last", sub(`topic: "a/#/b"`), `topic "a/#/b": "#" must be the last level`},
		{"partial wildcard", sub(`topic: "a/b+"`), `wildcard in level "b+" must span the whole level`},
		{"partial parameter", sub(`topic: "a/x{id}"`), `parameter in level "x{id}" must span the whole level`},
		{"bad parameter", sub(`topic: "a/{b.c}"`), `invalid parameter name "b.c"`},
		{"duplicate parameter", sub(`topic: "{a}/{a}"`), `parameter "a" is used twice`},
		{"wildcard on publish", pub(`topic: "a/+"`), `topic "a/+": wildcards are only valid in subscriptions`},
		{"publish to $", pub(`topic: "$SYS/x"`), `topics starting with "$" are reserved for the broker`},
		{"shared group with slash", sub(`topic: "a", shared_group: "g/h"`), `shared_group "g/h" must not contain`},
		{"no local on shared", sub(`topic: "a", shared_group: "g", no_local: true`), "no_local is a protocol error on shared subscriptions"},
		{"bad expiry", pub(`topic: "a", message_expiry: "soon"`), `message_expiry: "soon" is not a duration`},
		{"reply on publish", pub(`topic: "a", reply_topic: "b"`), "reply_topic and reply_messages are only valid for REQUEST_REPLY operations"},
		{"wildcard reply topic", `service S { rpc M(Msg) returns (Msg) { option (mqtt.asyncapi.v1.operation) = {topic: "a", reply_topic: "b/#"}; } }`,
			`reply_topic: topic "b/#": wildcards are only valid in subscriptions`},
		{"unknown reply message", `service S { rpc M(Msg) returns (Msg) { option (mqtt.asyncapi.v1.operation) = {topic: "a", reply_messages: ["test.v1.Nope"]}; } }`,
			`reply message "test.v1.Nope" not found`},
		{"v3 request/response", v3 + rpc, "request/response (response topic and correlation data) is an MQTT 5 feature, but every declared server speaks MQTT 3.1.1"},
		{"v3 shared group", v3 + sub(`topic: "a", shared_group: "g"`), "shared subscriptions is an MQTT 5 feature"},
		{"v3 message expiry", v3 + pub(`topic: "a", message_expiry: "1h"`), "message_expiry is an MQTT 5 feature"},
		{"v3 retain handling", v3 + sub(`topic: "a", retain_handling: RETAIN_HANDLING_SEND_IF_NEW`), "retain_handling is an MQTT 5 feature"},
		{"v3 content type", v3 + `message Ev { option (mqtt.asyncapi.v1.message) = {topic: "ev", content_type: "text/plain"}; }`,
			"the payload format indicator and content type is an MQTT 5 feature"},
		{"v3 server session expiry", server(`version: VERSION_3_1_1, session_expiry: "1h"`), "session_expiry is an MQTT 5 feature, but the server speaks MQTT 3.1.1"},
		{"v3 server will delay", server(`version: VERSION_3_1_1, last_will: {topic: "w", delay: "5s"}`), "last_will.delay is an MQTT 5 feature"},
		{"keep alive range", server(`keep_alive: "24h"`), "keep_alive must be between 0 and 65535s"},
		{"will on wildcard", server(`last_will: {topic: "w/#"}`), `last_will: topic "w/#": wildcards are only valid in subscriptions`},
		{"will without topic", server(`last_will: {message: "bye"}`), "last_will: topic must not be empty"},
		{"unknown mqtt server", `option (mqtt.asyncapi.v1.document) = {servers: [{name: "a"}]};`, `mqtt server "a" is not declared`},
		{"auth without scheme", `option (mqtt.asyncapi.v1.document) = {auth: [{scheme: "x", method: AUTH_METHOD_USERNAME_PASSWORD}]};`,
			`mqtt auth for "x": the security scheme is not declared`},
		{"auth without method", `option (asyncapi.v3.document) = {security_schemes: [{name: "x", type: SECURITY_SCHEME_TYPE_USER_PASSWORD}]};
option (mqtt.asyncapi.v1.document) = {auth: [{scheme: "x"}]};`, `mqtt auth for "x": method is required`},
		{"enhanced without method", `option (asyncapi.v3.document) = {security_schemes: [{name: "x"}]};
option (mqtt.asyncapi.v1.document) = {auth: [{scheme: "x", method: AUTH_METHOD_ENHANCED}]};`, "enhanced authentication needs enhanced_method"},
		{"enhanced unknown method without type", `option (asyncapi.v3.document) = {security_schemes: [{name: "x"}]};
option (mqtt.asyncapi.v1.document) = {auth: [{scheme: "x", method: AUTH_METHOD_ENHANCED, enhanced_method: "KERBEROS"}]};`, `set the security scheme's type for authentication method "KERBEROS"`},
		{"enhanced method on password", `option (asyncapi.v3.document) = {security_schemes: [{name: "x"}]};
option (mqtt.asyncapi.v1.document) = {auth: [{scheme: "x", method: AUTH_METHOD_USERNAME_PASSWORD, enhanced_method: "SCRAM-SHA-256"}]};`, "enhanced_method requires method ENHANCED"},
		{"v3 enhanced auth", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}], security_schemes: [{name: "x"}]};
option (mqtt.asyncapi.v1.document) = {servers: [{name: "a", version: VERSION_3_1_1}], auth: [{scheme: "x", method: AUTH_METHOD_ENHANCED, enhanced_method: "SCRAM-SHA-256"}]};`,
			"enhanced authentication is an MQTT 5 feature"},
		{"standalone on wildcard", `message Ev { option (mqtt.asyncapi.v1.message) = {topic: "ev/+"}; }`, "wildcards are only valid in subscriptions"},
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
