package cli

import (
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/amqp"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/golden"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/googlepubsub"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/kafka"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/nats"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/redis"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/temporal"
)

// tagsProto declares services of every protocol with declared tags at every
// level, protocol tags (Temporal kinds and workflows) and synthetic
// operations (NATS micro discovery, Redis keyspace notifications).
const tagsProto = `syntax = "proto3";
package tags.v1;
import "asyncapi/v3/annotations.proto";
import "amqp/asyncapi/v1/annotations.proto";
import "google/protobuf/empty.proto";
import "googlepubsub/asyncapi/v1/annotations.proto";
import "kafka/asyncapi/v1/annotations.proto";
import "nats/asyncapi/v1/annotations.proto";
import "redis/asyncapi/v1/annotations.proto";
import "temporal/asyncapi/v1/options.proto";

option (asyncapi.v3.document) = {
  info: {title: "Tags", version: "1.0.0"}
  tags: [{name: "platform", description: "Declared on the document."}]
  servers: [
    {name: "nats", host: "nats:4222", protocol: "nats"},
    {name: "redis", host: "redis:6379", protocol: "redis"},
    {name: "rabbitmq", host: "rabbitmq:5672", protocol: "amqp"},
    {name: "pubsub", host: "pubsub.googleapis.com:443", protocol: "googlepubsub"},
    {name: "kafka", host: "kafka:9092", protocol: "kafka"},
    {name: "temporal", host: "temporal:7233", protocol: "temporal"}
  ]
};

// Workflows.
service Flows {
  option (temporal.asyncapi.v1.service) = {task_queue: "q"};
  option (asyncapi.v3.service) = {tags: [{name: "billing"}]};
  // Runs.
  rpc Run(M) returns (M) { option (temporal.asyncapi.v1.operation).workflow = {}; }
  // Pokes.
  rpc Poke(M) returns (google.protobuf.Empty) { option (temporal.asyncapi.v1.operation).signal = {}; }
}

// A micro service.
service Api {
  option (nats.asyncapi.v1.service) = {micro: {name: "api"}};
  // Gets.
  rpc Get(M) returns (M) {
    option (nats.asyncapi.v1.operation) = {};
    option (asyncapi.v3.operation) = {tags: [{name: "reads"}]};
  }
}

// Cache events. Its declared tag has the service's own name.
service Cache {
  option (redis.asyncapi.v1.service) = {keyspace: [{name: "expiry", kind: KEYSPACE_KIND_KEYEVENT, events: ["expired"]}]};
  option (asyncapi.v3.service) = {tags: [{name: "Cache", description: "Declared explicitly."}]};
  // Invalidates.
  rpc Invalidate(M) returns (google.protobuf.Empty) { option (redis.asyncapi.v1.operation) = {}; }
}

// Billing over RabbitMQ.
service Billing {
  option (asyncapi.v3.service) = {tags: [{name: "payments"}]};
  // Charges.
  rpc Charge(M) returns (M) { option (amqp.asyncapi.v1.operation) = {}; }
}

// Clicks on Google Pub/Sub.
service Clicks {
  option (asyncapi.v3.service) = {tags: [{name: "analytics"}]};
  // Tracks.
  rpc Track(google.protobuf.Empty) returns (stream M) { option (googlepubsub.asyncapi.v1.operation) = {}; }
}

// Ledger on Kafka.
service Ledger {
  option (asyncapi.v3.service) = {tags: [{name: "accounting"}]};
  // Posts.
  rpc Post(google.protobuf.Empty) returns (stream M) { option (kafka.asyncapi.v1.operation) = {}; }
}

message M { string id = 1; }
`

type tagDoc struct {
	Info struct {
		Tags []struct {
			Name        string `yaml:"name"`
			Description string `yaml:"description"`
		} `yaml:"tags"`
	} `yaml:"info"`
	Operations map[string]struct {
		Tags []struct {
			Name        string `yaml:"name"`
			Description string `yaml:"description"`
		} `yaml:"tags"`
	} `yaml:"operations"`
}

func generateTags(t *testing.T, param string) tagDoc {
	t.Helper()
	resp := golden.Run(All, golden.Request(t, importPaths, map[string]string{"tags.proto": tagsProto}, param, "tags.proto"))
	if resp.Error != nil {
		t.Fatal(resp.GetError())
	}
	var doc tagDoc
	if err := yaml.Unmarshal([]byte(resp.File[0].GetContent()), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func (d tagDoc) opTags(id string) string {
	var names []string
	for _, tg := range d.Operations[id].Tags {
		names = append(names, tg.Name)
	}
	return strings.Join(names, ",")
}

func (d tagDoc) infoTags() string {
	var names []string
	for _, tg := range d.Info.Tags {
		names = append(names, tg.Name)
	}
	return strings.Join(names, ",")
}

func TestWithoutDefaultTags(t *testing.T) {
	const withDefaults = "platform,Workflows,workflow:Run,Flows,Signals,Api,Cache,Billing,Clicks,Ledger"
	for _, tc := range []struct {
		param string
		ops   map[string]string
		info  string
	}{
		{
			// Default: every operation leads with its service's tag, which
			// info.tags declares.
			param: "",
			ops: map[string]string{
				"workflow.Run":       "Flows,Workflows,workflow:Run,billing",
				"signal.Poke":        "Flows,Signals,workflow:Run,billing",
				"Api.Get":            "Api,reads",
				"Api.discovery.PING": "Api",
				"Cache.Invalidate":   "Cache",
				"Cache.expiry":       "Cache",
				"Billing.Charge":     "Billing,payments",
				"Clicks.Track":       "Clicks,analytics",
				"Ledger.Post":        "Ledger,accounting",
			},
			info: withDefaults,
		},
		{
			// Service-name tags go, from rpc and synthetic operations and
			// from info.tags; declared and protocol tags stay.
			param: "without_default_tags=true",
			ops: map[string]string{
				"workflow.Run":       "Workflows,workflow:Run,billing",
				"signal.Poke":        "Signals,workflow:Run,billing",
				"Api.Get":            "reads",
				"Api.discovery.PING": "",
				"Cache.Invalidate":   "Cache",
				"Cache.expiry":       "",
				"Billing.Charge":     "payments",
				"Clicks.Track":       "analytics",
				"Ledger.Post":        "accounting",
			},
			info: "platform,Workflows,workflow:Run,Signals",
		},
		{
			// A bare option name means true.
			param: "without_default_tags",
			ops:   map[string]string{"Api.Get": "reads"},
			info:  "platform,Workflows,workflow:Run,Signals",
		},
		{
			param: "without_default_tags=false",
			ops:   map[string]string{"Api.Get": "Api,reads"},
			info:  withDefaults,
		},
	} {
		t.Run(tc.param, func(t *testing.T) {
			doc := generateTags(t, tc.param)
			for id, want := range tc.ops {
				if _, ok := doc.Operations[id]; !ok {
					t.Errorf("operation %s is missing", id)
					continue
				}
				if got := doc.opTags(id); got != want {
					t.Errorf("%s tags = %q, want %q", id, got, want)
				}
			}
			if got := doc.infoTags(); got != tc.info {
				t.Errorf("info.tags = %q, want %q", got, tc.info)
			}
		})
	}
}

func TestWithoutDefaultTagsKeepsDeclaredTag(t *testing.T) {
	// A declared tag that happens to carry the service's name is not a
	// default tag: it stays, with its declared description.
	doc := generateTags(t, "without_default_tags=true")
	tags := doc.Operations["Cache.Invalidate"].Tags
	if len(tags) != 1 || tags[0].Name != "Cache" || tags[0].Description != "Declared explicitly." {
		t.Errorf("Cache.Invalidate tags = %+v", tags)
	}
}

// TestWithoutDefaultTagsEveryPlugin checks the option is shared by every
// binary, each dropping the service tags of its own protocol.
func TestWithoutDefaultTagsEveryPlugin(t *testing.T) {
	for _, tc := range []struct {
		pl  core.Plugin
		op  string
		tag string // protocol or declared tag kept
	}{
		{All, "Api.Get", "reads"},
		{amqp.Plugin, "Billing.Charge", "payments"},
		{googlepubsub.Plugin, "Clicks.Track", "analytics"},
		{kafka.Plugin, "Ledger.Post", "accounting"},
		{nats.Plugin, "Api.Get", "reads"},
		{redis.Plugin, "Cache.Invalidate", "Cache"},
		{temporal.Plugin, "workflow.Run", "Workflows"},
	} {
		t.Run(tc.pl.Name, func(t *testing.T) {
			resp := golden.Run(tc.pl, golden.Request(t, importPaths, map[string]string{"tags.proto": tagsProto}, "without_default_tags=true", "tags.proto"))
			if resp.Error != nil {
				t.Fatal(resp.GetError())
			}
			var doc tagDoc
			if err := yaml.Unmarshal([]byte(resp.File[0].GetContent()), &doc); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(","+doc.opTags(tc.op)+",", ","+tc.tag+",") {
				t.Errorf("%s tags = %q, want %q kept", tc.op, doc.opTags(tc.op), tc.tag)
			}
			for _, name := range []string{"Flows", "Api"} {
				if strings.Contains(","+doc.infoTags()+",", ","+name+",") {
					t.Errorf("info.tags = %q still declares %q", doc.infoTags(), name)
				}
			}
			if !strings.Contains(core.Usage(tc.pl.AllOptions(core.DefaultParams(), tc.pl.Protocols())), "without_default_tags") {
				t.Errorf("%s --help does not list without_default_tags", tc.pl.Name)
			}
		})
	}
}

func TestWithoutDefaultTagsInvalidValue(t *testing.T) {
	resp := golden.Run(All, golden.Request(t, importPaths, map[string]string{"tags.proto": tagsProto}, "without_default_tags=maybe", "tags.proto"))
	if want := `invalid value "maybe" for option "without_default_tags"`; !strings.Contains(resp.GetError(), want) {
		t.Errorf("error = %q, want it to contain %q", resp.GetError(), want)
	}
}
