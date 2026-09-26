package amqp

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
	"../../proto/amqp",
	"../../examples/amqp/proto",
}

func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"example", "", []string{"acme/shipping/v1/shipping.proto"}},
		{"billing", "", []string{"billing/v1/billing.proto"}},
		{"billing_client", "perspective=client", []string{"billing/v1/billing.proto"}},
		{"streams", "", []string{"streams/v1/streams.proto"}},
		{"streams_client", "perspective=client", []string{"streams/v1/streams.proto"}},
		{"billing_3.0.0_json", "asyncapi_version=3.0.0,format=json", []string{"billing/v1/billing.proto"}},
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
import "amqp/asyncapi/v1/annotations.proto";
message Msg { string id = 1; }
`
	topo := func(decl string) string {
		return `option (amqp.asyncapi.v1.document) = {` + decl + `};
`
	}
	sub := func(opt string) string {
		return `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (amqp.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	pub := func(opt string) string {
		return `service S { rpc M(google.protobuf.Empty) returns (stream Msg) { option (amqp.asyncapi.v1.operation) = {` + opt + `}; } }`
	}
	const topic = `exchanges: [{name: "ev", type: EXCHANGE_TYPE_TOPIC}]`
	cases := []struct{ name, src, want string }{
		{"client stream with response", `service S { rpc M(stream Msg) returns (Msg) { option (amqp.asyncapi.v1.operation) = {}; } }`,
			"test.proto:7:13: a client streaming rpc with a response has no AsyncAPI form; set (amqp.asyncapi.v1.operation).pattern"},
		{"process with empty", `service S { rpc M(stream Msg) returns (stream google.protobuf.Empty) { option (amqp.asyncapi.v1.operation) = {}; } }`,
			"neither may be google.protobuf.Empty"},
		{"output without process", `service S { rpc M(Msg) returns (google.protobuf.Empty) { option (amqp.asyncapi.v1.operation) = {output: "x"}; } }`,
			"output is only valid for PROCESS operations"},
		{"wildcard output", `service S { rpc M(stream Msg) returns (stream Msg) { option (amqp.asyncapi.v1.operation) = {output: "a.*"}; } }`,
			`output routing key "a.*" must not contain wildcards`},
		{"process on fanout", topo(`exchanges: [{name: "f", type: EXCHANGE_TYPE_FANOUT}]`) + `service S { rpc M(stream Msg) returns (stream Msg) { option (amqp.asyncapi.v1.operation) = {exchange: "f"}; } }`,
			`exchange "f" ignores routing keys: the output of a processor would reach its input`},
		{"undeclared exchange", sub(`exchange: "nope"`), `exchange "nope" is not declared in (amqp.asyncapi.v1.document).exchanges`},
		{"undeclared service exchange", `service S { option (amqp.asyncapi.v1.service) = {exchange: "nope"}; rpc M(Msg) returns (google.protobuf.Empty); }`,
			`exchange "nope" is not declared`},
		{"internal exchange", topo(`exchanges: [{name: "in", internal: true}]`) + sub(`exchange: "in"`), `exchange "in" is internal`},
		{"default exchange parameters", sub(`routing_key: "a.{id}"`), "the default exchange delivers to the queue named after the routing key, so it cannot have parameters"},
		{"default exchange queue mismatch", sub(`routing_key: "a", queue: "b"`), `routing key "a" does not reach queue "b"`},
		{"wildcard on publish", topo(topic) + pub(`exchange: "ev", routing_key: "a.*"`), "wildcards are binding patterns, only valid for SUBSCRIBE and PROCESS operations"},
		{"wildcard on direct", topo(`exchanges: [{name: "d"}]`) + sub(`exchange: "d", routing_key: "a.#"`), `wildcards need a topic exchange, "d" is a direct exchange`},
		{"bad parameter", topo(topic) + sub(`exchange: "ev", routing_key: "a.{b.c}"`), `invalid parameter name "b.c"`},
		{"unbalanced braces", topo(topic) + sub(`exchange: "ev", routing_key: "a.{b"`), "has unbalanced braces"},
		{"reserved queue", sub(`queue: "amq.mine"`), `queue name "amq.mine" uses the reserved amq. prefix`},
		{"priority range", pub(`publish: {priority: 300}`), "publish.priority must be between 0 and 255"},
		{"priority above queue max", topo(`queues: [{name: "q", max_priority: 5}]`) + pub(`queue: "q", publish: {priority: 7}`), `publish.priority 7 exceeds the max_priority 5 of queue "q"`},
		{"bad expiration", pub(`publish: {expiration: "soon"}`), `publish.expiration: "soon" is not a duration`},
		{"delay without delayed exchange", topo(topic) + pub(`exchange: "ev", routing_key: "a", publish: {delay: "5s"}`), `publish.delay needs a delayed-message exchange, "ev" is not one`},
		{"empty cc", pub(`publish: {cc: [""]}`), `publish cc and bcc must be routing keys, got ""`},
		{"negative prefetch", sub(`consume: {prefetch: -1}`), "consume.prefetch must be >= 0"},
		{"stream offset on classic", topo(`queues: [{name: "q"}]`) + sub(`queue: "q", consume: {stream_offset: "first"}`), "consume.stream_offset needs a stream queue"},
		{"bad stream offset", topo(`queues: [{name: "q", type: QUEUE_TYPE_STREAM}]`) + sub(`queue: "q", consume: {stream_offset: "beginning", prefetch: 1}`), `consume.stream_offset "beginning" is not`},
		{"stream without prefetch", topo(`queues: [{name: "q", type: QUEUE_TYPE_STREAM}]`) + sub(`queue: "q"`), `stream queue "q": consumers must set a prefetch`},
		{"reply on publish", pub(`reply_queue: "r"`), "reply_queue and reply_messages are only valid for REQUEST_REPLY operations"},
		{"unknown reply message", `service S { rpc M(Msg) returns (Msg) { option (amqp.asyncapi.v1.operation) = {reply_messages: ["test.v1.Nope"]}; } }`,
			`reply message "test.v1.Nope" not found`},
		{"duplicate exchange", topo(`exchanges: [{name: "a"}, {name: "a"}]`), `test.proto: exchange "a": is declared twice`},
		{"duplicate queue", topo(`queues: [{name: "a"}, {name: "a"}]`), `test.proto: queue "a": is declared twice`},
		{"reserved exchange", topo(`exchanges: [{name: "amq.mine"}]`), `exchange name "amq.mine" uses the reserved amq. prefix`},
		{"unknown alternate exchange", topo(`exchanges: [{name: "a", alternate_exchange: "b"}]`), `alternate exchange "b" is not declared`},
		{"unknown dead letter exchange", topo(`queues: [{name: "q", dead_letter_exchange: "dlx"}]`), `dead letter exchange "dlx" is not declared`},
		{"dead letter key without exchange", topo(`queues: [{name: "q", dead_letter_routing_key: "k"}]`), "dead_letter_routing_key requires dead_letter_exchange"},
		{"binding to unknown exchange", topo(`queues: [{name: "q", bindings: [{exchange: "x"}]}]`), `bindings[0]: exchange "x" is not declared`},
		{"binding wildcard on fanout", topo(`exchanges: [{name: "f", type: EXCHANGE_TYPE_FANOUT}], queues: [{name: "q", bindings: [{exchange: "f", routing_key: "a.*"}]}]`),
			`wildcards need a topic exchange, "f" is a fanout exchange`},
		{"match on topic", topo(topic + `, queues: [{name: "q", bindings: [{exchange: "ev", match: MATCH_ALL}]}]`), `match and headers need a headers exchange`},
		{"bad header value", topo(`exchanges: [{name: "h", type: EXCHANGE_TYPE_HEADERS}], queues: [{name: "q", bindings: [{exchange: "h", headers: {key: "a", value: "eu"}}]}]`),
			`"a" is not a JSON value`},
		{"built-in exchanges are known", topo(`queues: [{name: "q", bindings: [{exchange: "amq.topic", routing_key: "a.*"}]}]`) + `service S { rpc M(Msg) returns (Msg) { option (amqp.asyncapi.v1.operation) = {reply_messages: ["test.v1.Nope"]}; } }`,
			`reply message "test.v1.Nope" not found`},
		{"quorum priority", topo(`queues: [{name: "q", type: QUEUE_TYPE_QUORUM, max_priority: 5}]`), "max_priority only applies to classic queues"},
		{"quorum exclusive", topo(`queues: [{name: "q", type: QUEUE_TYPE_QUORUM, exclusive: true}]`), "quorum queues cannot be exclusive or auto-delete"},
		{"stream dead letter", topo(`exchanges: [{name: "dlx"}], queues: [{name: "q", type: QUEUE_TYPE_STREAM, dead_letter_exchange: "dlx"}]`), "stream queues do not dead-letter"},
		{"delivery limit on classic", topo(`queues: [{name: "q", delivery_limit: 3}]`), "delivery_limit only applies to quorum queues"},
		{"max age on classic", topo(`queues: [{name: "q", max_age: "1h"}]`), "max_age only applies to stream queues"},
		{"bad ttl", topo(`queues: [{name: "q", message_ttl: "-1s"}]`), "message_ttl must not be negative"},
		{"unknown exchange binding", topo(`exchange_bindings: [{source: "a", destination: "b"}]`), `exchange_bindings[0]: exchange "a" is not declared`},
		{"unknown amqp server", topo(`servers: [{name: "a", tls: true}]`), `amqp server "a" is not declared`},
		{"bad heartbeat", `option (asyncapi.v3.document) = {servers: [{name: "a", host: "h"}]};
` + topo(`servers: [{name: "a", heartbeat: "often"}]`), `amqp server "a": heartbeat: "often" is not a duration`},
		{"auth without scheme", topo(`auth: [{scheme: "x", mechanism: MECHANISM_PLAIN}]`), `amqp auth for "x": the security scheme is not declared`},
		{"auth without mechanism", `option (asyncapi.v3.document) = {security_schemes: [{name: "x", type: SECURITY_SCHEME_TYPE_USER_PASSWORD}]};
` + topo(`auth: [{scheme: "x"}]`), `amqp auth for "x": mechanism is required`},
		{"standalone on unknown exchange", `message Ev { option (amqp.asyncapi.v1.message) = {exchange: "nope", routing_key: "a"}; }`, `exchange "nope" is not declared`},
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
