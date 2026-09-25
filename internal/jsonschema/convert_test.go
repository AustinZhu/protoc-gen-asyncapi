package jsonschema_test

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/jsonschema"
	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/testutil"
)

const source = `
syntax = "proto3";
package t.v1;

import "buf/validate/validate.proto";
import "google/protobuf/any.proto";
import "google/protobuf/duration.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";

// A message.
message M {
  double f_double = 1;
  float f_float = 2;
  int32 f_int32 = 3;
  sint32 f_sint32 = 4;
  sfixed32 f_sfixed32 = 5;
  int64 f_int64 = 6;
  sint64 f_sint64 = 7;
  sfixed64 f_sfixed64 = 8;
  uint32 f_uint32 = 9;
  fixed32 f_fixed32 = 10;
  uint64 f_uint64 = 11;
  fixed64 f_fixed64 = 12;
  bool f_bool = 13;
  // Leading comment.
  string f_string = 14;
  bytes f_bytes = 15;
  E f_enum = 16;
  N f_message = 17;
  repeated string f_repeated = 18;
  map<string, N> f_map = 19;
  map<bool, int32> f_bool_map = 20;
  optional string f_optional = 21; // Trailing comment.
  oneof choice {
    string a = 22;
    int32 b = 23;
  }
  google.protobuf.Timestamp f_timestamp = 24;
  google.protobuf.Duration f_duration = 25;
  google.protobuf.Empty f_empty = 26;
  google.protobuf.Int64Value f_int64_value = 27;
  google.protobuf.StringValue f_string_value = 28;
  google.protobuf.Struct f_struct = 29;
  google.protobuf.Value f_value = 30;
  google.protobuf.Any f_any = 31;
  repeated google.protobuf.Timestamp f_timestamps = 32;
  M recursive = 33;
  google.protobuf.NullValue f_null = 34;
}

message N {
  string x = 1;
}

enum E {
  E_UNSPECIFIED = 0;
  // The one.
  E_ONE = 1;
}

message V {
  string s = 1 [(buf.validate.field).string = {min_len: 1, max_len: 5, pattern: "^a"}, (buf.validate.field).required = true];
  int32 i = 2 [(buf.validate.field).int32 = {gt: 0, lte: 10}];
  double d = 3 [(buf.validate.field).double = {gte: 1.5, lt: 2.5}];
  repeated int64 r = 4 [(buf.validate.field).repeated = {min_items: 1, max_items: 3}];
  int32 outside = 5 [(buf.validate.field).int32 = {lt: 0, gt: 10}];
  string u = 6 [(buf.validate.field).string.uuid = true];
  oneof must {
    option (buf.validate.oneof).required = true;
    string p = 7;
    string q = 8;
  }
}
`

func compile(t *testing.T) (protoreflect.FileDescriptor, *dynamicpb.Types) {
	t.Helper()
	req := testutil.Request(t, testutil.ImportPaths(), map[string]string{"t.proto": source}, "t.proto")
	files, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: req.ProtoFile})
	if err != nil {
		t.Fatal(err)
	}
	fd, err := files.FindFileByPath("t.proto")
	if err != nil {
		t.Fatal(err)
	}
	return fd, dynamicpb.NewTypes(files)
}

// js marshals v to compact JSON for comparison.
func js(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func prop(t *testing.T, s *jsonschema.Schema, name string) *jsonschema.Schema {
	t.Helper()
	p, ok := s.Properties.Get(name)
	if !ok {
		t.Fatalf("no property %q", name)
	}
	return p
}

func TestFieldMapping(t *testing.T) {
	fd, _ := compile(t)
	m, defs := jsonschema.MessageSchema(fd.Messages().ByName("M"), jsonschema.Options{})

	tests := map[string]string{
		"fDouble":      `{"type":"number"}`,
		"fFloat":       `{"type":"number"}`,
		"fInt32":       `{"type":"integer","format":"int32"}`,
		"fSint32":      `{"type":"integer","format":"int32"}`,
		"fSfixed32":    `{"type":"integer","format":"int32"}`,
		"fInt64":       `{"type":["integer","string"],"format":"int64","pattern":"^-?[0-9]+$"}`,
		"fSint64":      `{"type":["integer","string"],"format":"int64","pattern":"^-?[0-9]+$"}`,
		"fSfixed64":    `{"type":["integer","string"],"format":"int64","pattern":"^-?[0-9]+$"}`,
		"fUint32":      `{"type":"integer","format":"uint32","minimum":0}`,
		"fFixed32":     `{"type":"integer","format":"uint32","minimum":0}`,
		"fUint64":      `{"type":["integer","string"],"format":"uint64","pattern":"^[0-9]+$","minimum":0}`,
		"fFixed64":     `{"type":["integer","string"],"format":"uint64","pattern":"^[0-9]+$","minimum":0}`,
		"fBool":        `{"type":"boolean"}`,
		"fString":      `{"description":"Leading comment.","type":"string"}`,
		"fBytes":       `{"type":"string","contentEncoding":"base64"}`,
		"fEnum":        `{"$ref":"#/components/schemas/t.v1.E"}`,
		"fMessage":     `{"$ref":"#/components/schemas/t.v1.N"}`,
		"fRepeated":    `{"type":"array","items":{"type":"string"}}`,
		"fMap":         `{"type":"object","additionalProperties":{"$ref":"#/components/schemas/t.v1.N"}}`,
		"fBoolMap":     `{"type":"object","propertyNames":{"enum":["true","false"]},"additionalProperties":{"type":"integer","format":"int32"}}`,
		"fOptional":    `{"description":"Trailing comment.","type":"string"}`,
		"a":            `{"type":"string"}`,
		"fTimestamp":   `{"type":"string","format":"date-time"}`,
		"fDuration":    `{"type":"string","pattern":"^-?[0-9]+(\\.[0-9]+)?s$"}`,
		"fEmpty":       `{"type":"object","additionalProperties":false}`,
		"fInt64Value":  `{"type":["integer","string","null"],"format":"int64","pattern":"^-?[0-9]+$"}`,
		"fStringValue": `{"type":["string","null"]}`,
		"fStruct":      `{"type":"object"}`,
		"fValue":       `{}`,
		"fAny":         `{"type":"object","properties":{"@type":{"description":"Type URL of the packed message, e.g. type.googleapis.com/acme.v1.Order.","type":"string"}},"required":["@type"],"additionalProperties":true}`,
		"fTimestamps":  `{"type":"array","items":{"type":"string","format":"date-time"}}`,
		"recursive":    `{"$ref":"#/components/schemas/t.v1.M"}`,
		"fNull":        `{"type":"null"}`,
	}
	for name, want := range tests {
		if got := js(t, prop(t, m, name)); got != want {
			t.Errorf("%s:\n got %s\nwant %s", name, got, want)
		}
	}
	if m.Description != "A message." || m.Title != "M" || m.Type != "object" {
		t.Errorf("message header = %q %q %v", m.Title, m.Description, m.Type)
	}
	if len(m.Required) != 0 {
		t.Errorf("proto3 fields must not be required, got %v", m.Required)
	}
	// The oneof lists each member and a "none set" alternative; the proto3
	// optional field's synthetic oneof is ignored.
	if got, want := js(t, m.OneOf), `[{"required":["a"]},{"required":["b"]},{"not":{"anyOf":[{"required":["a"]},{"required":["b"]}]}}]`; got != want {
		t.Errorf("oneOf:\n got %s\nwant %s", got, want)
	}
	if _, ok := defs["t.v1.N"]; !ok {
		t.Errorf("defs missing t.v1.N: %v", keys(defs))
	}
	if got := js(t, defs["t.v1.E"]); got != "{\"title\":\"E\",\"description\":\"- `E_ONE`: The one.\",\"type\":\"string\",\"enum\":[\"E_UNSPECIFIED\",\"E_ONE\"]}" {
		t.Errorf("enum = %s", got)
	}
	if _, ok := defs["t.v1.M"]; ok {
		t.Error("defs must not include the message itself")
	}
	for k := range defs {
		if k == "google.protobuf.Timestamp" {
			t.Error("well-known types must be inlined, not defined")
		}
	}
}

func TestUseProtoNames(t *testing.T) {
	fd, _ := compile(t)
	m, _ := jsonschema.MessageSchema(fd.Messages().ByName("N"), jsonschema.Options{UseProtoNames: true})
	m2, _ := jsonschema.MessageSchema(fd.Messages().ByName("M"), jsonschema.Options{UseProtoNames: true})
	if _, ok := m.Properties.Get("x"); !ok {
		t.Error("missing x")
	}
	if _, ok := m2.Properties.Get("f_double"); !ok {
		t.Error("missing f_double")
	}
}

func TestProtovalidate(t *testing.T) {
	fd, types := compile(t)
	md := fd.Messages().ByName("V")

	off, _ := jsonschema.MessageSchema(md, jsonschema.Options{})
	if got := js(t, prop(t, off, "s")); got != `{"type":"string"}` {
		t.Errorf("enrichment off: s = %s", got)
	}
	if off.Required != nil {
		t.Errorf("enrichment off: required = %v", off.Required)
	}

	on, _ := jsonschema.MessageSchema(md, jsonschema.Options{Protovalidate: types})
	tests := map[string]string{
		"s":       `{"type":"string","pattern":"^a","minLength":1,"maxLength":5}`,
		"i":       `{"type":"integer","format":"int32","exclusiveMinimum":0,"maximum":10}`,
		"d":       `{"type":"number","minimum":1.5,"exclusiveMaximum":2.5}`,
		"r":       `{"type":"array","items":{"type":["integer","string"],"format":"int64","pattern":"^-?[0-9]+$"},"minItems":1,"maxItems":3}`,
		"outside": `{"type":"integer","format":"int32"}`,
		"u":       `{"type":"string","format":"uuid"}`,
	}
	for name, want := range tests {
		if got := js(t, prop(t, on, name)); got != want {
			t.Errorf("%s:\n got %s\nwant %s", name, got, want)
		}
	}
	if js(t, on.Required) != `["s"]` {
		t.Errorf("required = %v", on.Required)
	}
	if got := js(t, on.OneOf); got != `[{"required":["p"]},{"required":["q"]}]` {
		t.Errorf("required oneof = %s", got)
	}
}

func keys(m map[string]*jsonschema.Schema) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
