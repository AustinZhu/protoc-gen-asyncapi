package core

import (
	"strings"
	"testing"
)

func TestSummarize(t *testing.T) {
	cases := []struct{ in, summary, desc string }{
		{"", "", ""},
		{"Places an order.", "Places an order.", ""},
		{"Places an order.\nIt is validated.", "Places an order.", "Places an order.\nIt is validated."},
		{"Places an order.\n\nDetails.", "Places an order.", "Places an order.\n\nDetails."},
		{"No full stop", "No full stop", ""},
		{"Uses v1.2 of the API.", "Uses v1.2 of the API.", ""},
	}
	for _, c := range cases {
		s, d := summarize(c.in)
		if s != c.summary || d != c.desc {
			t.Errorf("summarize(%q) = %q, %q; want %q, %q", c.in, s, d, c.summary, c.desc)
		}
	}
}

func TestSnakeCase(t *testing.T) {
	cases := map[string]string{
		"PlaceOrder":    "place_order",
		"GetHTTPStatus": "get_http_status",
		"V2Orders":      "v2_orders",
		"already_snake": "already_snake",
		"OrderService":  "order_service",
	}
	for in, want := range cases {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParams(t *testing.T) {
	p := DefaultParams()
	opts := p.Options()
	err := parseParams("format=json, merge,merge_file_name=shop,perspective=client,payload=protobuf,asyncapi_version=3.0.0,"+
		"version=1.2.3,content_type=text/plain,json_names=false,enum_values=both,proto_types,services=a.**|b.C,services=c.*,paths=source_relative", opts)
	if err != nil {
		t.Fatal(err)
	}
	if p.Format != "json" || !p.Merge || p.MergeFileName != "shop" || p.Perspective != "client" || p.Payload != "protobuf" ||
		p.AsyncAPIVersion != "3.0.0" || p.Version != "1.2.3" || p.ContentType != "text/plain" || p.JSONNames ||
		p.EnumValues != EnumBoth || !p.ProtoTypes || strings.Join(p.Services, " ") != "a.** b.C c.*" {
		t.Errorf("unexpected params %+v", p)
	}
	for param, want := range map[string]string{
		"merge=maybe":            `(want true or false)`,
		"format=xml":             `(want yaml or json or jsonschema)`,
		"asyncapi_version=2.6.0": `(want 3.1.0 or 3.0.0)`,
		"merge_file_name=a/b":    `want a file name`,
		"colour=blue":            `unknown option "colour"`,
	} {
		if err := parseParams(param, DefaultParams().Options()); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", param, err, want)
		}
	}
}

func TestMatchService(t *testing.T) {
	cases := []struct {
		glob, name string
		want       bool
	}{
		{"acme.v1.Orders", "acme.v1.Orders", true},
		{"acme.v1.*", "acme.v1.Orders", true},
		{"acme.*", "acme.v1.Orders", false},
		{"acme.**", "acme.v1.Orders", true},
		{"**.Orders", "acme.v1.Orders", true},
		{"acme.*.Ord*", "acme.v1.Orders", true},
		{"**", "Orders", true},
	}
	for _, c := range cases {
		if got := MatchService(c.glob, c.name); got != c.want {
			t.Errorf("MatchService(%q, %q) = %v, want %v", c.glob, c.name, got, c.want)
		}
	}
}
