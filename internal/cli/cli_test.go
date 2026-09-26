package cli

import (
	"bytes"
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/amqp"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/golden"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/googlepubsub"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/kafka"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/mqtt"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/nats"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/redis"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/sqs"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/temporal"
)

var update = flag.Bool("update", false, "update golden files")

var importPaths = []string{
	"testdata/protos",
	"../../proto/amqp",
	"../../proto/asyncapi",
	"../../proto/googlepubsub",
	"../../proto/kafka",
	"../../proto/mqtt",
	"../../proto/nats",
	"../../proto/redis",
	"../../proto/sqs",
	"../../proto/temporal",
	"../../examples/amqp/proto",
	"../../examples/googlepubsub/proto",
	"../../examples/kafka/proto",
	"../../examples/mqtt/proto",
	"../../examples/nats/proto",
	"../../examples/redis/proto",
	"../../examples/sqs/proto",
	"../../examples/temporal/proto",
}

// TestGolden covers what only the combined plugin does: several protocols in
// one document.
func TestGolden(t *testing.T) {
	cases := []struct {
		name  string
		param string
		files []string
	}{
		{"mixed", "", []string{"mixed/v1/mixed.proto"}},
		{"mixed_without_default_tags", "without_default_tags=true", []string{"mixed/v1/mixed.proto"}},
		{"examples", "", []string{"acme/orders/v1/orders.proto", "acme/notify/v1/notify.proto", "acme/shipping/v1/shipping.proto", "acme/analytics/v1/analytics.proto", "acme/inventory/v1/inventory.proto", "acme/sensors/v1/sensors.proto", "acme/returns/v1/returns.proto", "acme/shop/v1/orders.proto"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := golden.Run(All, golden.Request(t, importPaths, nil, tc.param, tc.files...))
			golden.Check(t, filepath.Join("testdata", "golden", tc.name), resp, *update, importPaths)
		})
	}
}

// TestSingleProtocolPluginsIgnoreOthers checks that each protocol binary
// only documents its own services.
func TestSingleProtocolPluginsIgnoreOthers(t *testing.T) {
	req := golden.Request(t, importPaths, nil, "", "mixed/v1/mixed.proto")
	for _, tc := range []struct {
		name   string
		resp   *pluginpb.CodeGeneratorResponse
		has    string
		hasNot string
	}{
		{"nats", golden.Run(nats.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Events.Completed", "OrderWorkflows"},
		{"nats without redis", golden.Run(nats.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Events.Completed", "Receipts.Send"},
		{"temporal", golden.Run(temporal.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "workflow.ProcessOrder", "Events.Completed"},
		{"redis", golden.Run(redis.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Receipts.Send", "Events.Completed"},
		{"amqp", golden.Run(amqp.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Invoicing.Issue", "Receipts.Send"},
		{"nats without amqp", golden.Run(nats.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Events.Completed", "Invoicing"},
		{"googlepubsub", golden.Run(googlepubsub.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Analytics.Export", "Invoicing.Issue"},
		{"nats without googlepubsub", golden.Run(nats.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Events.Completed", "Analytics"},
		{"kafka", golden.Run(kafka.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "History.Record", "Analytics.Export"},
		{"nats without kafka", golden.Run(nats.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Events.Completed", "History"},
		{"mqtt", golden.Run(mqtt.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Warehouse.Notify", "History.Record"},
		{"nats without mqtt", golden.Run(nats.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Events.Completed", "Warehouse"},
		{"sqs", golden.Run(sqs.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "ReturnsQueue.Handle", "Warehouse.Notify"},
		{"nats without sqs", golden.Run(nats.Plugin, proto.Clone(req).(*pluginpb.CodeGeneratorRequest)), "Events.Completed", "ReturnsQueue"},
	} {
		if tc.resp.Error != nil {
			t.Fatalf("%s: %s", tc.name, tc.resp.GetError())
		}
		content := tc.resp.File[0].GetContent()
		if !strings.Contains(content, tc.has) || strings.Contains(content, tc.hasNot) {
			t.Errorf("%s: want %q and not %q in:\n%s", tc.name, tc.has, tc.hasNot, content)
		}
	}
}

func TestServerProtocolRequiredWhenMixed(t *testing.T) {
	src := strings.Replace(mustRead(t), `protocol: "nats"`, ``, 1)
	req := golden.Request(t, importPaths, map[string]string{"mixed/v1/mixed.proto": src}, "", "mixed/v1/mixed.proto")
	resp := golden.Run(All, req)
	if !strings.Contains(resp.GetError(), `server "nats": protocol is required when the document describes several protocols`) {
		t.Errorf("error = %q", resp.GetError())
	}
}

// TestRun exercises the stdin/stdout protocol end to end.
func TestRun(t *testing.T) {
	req := golden.Request(t, importPaths, nil, "format=json", "mixed/v1/mixed.proto")
	in, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := Run(All, bytes.NewReader(in), &out, "test"); err != nil {
		t.Fatal(err)
	}
	resp := &pluginpb.CodeGeneratorResponse{}
	if err := proto.Unmarshal(out.Bytes(), resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil || len(resp.File) != 1 || resp.File[0].GetName() != "mixed/v1/mixed.asyncapi.json" {
		t.Fatalf("unexpected response: %v", resp)
	}
}

func mustRead(t *testing.T) string {
	t.Helper()
	b, err := readFile("testdata/protos/mixed/v1/mixed.proto")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
