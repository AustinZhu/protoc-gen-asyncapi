package asyncapi_test

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi/spec"
)

var update = flag.Bool("update", false, "update golden files")

func str(s string) *string { return &s }
func u64(v uint64) *uint64 { return &v }

func props(kv ...any) *asyncapi.Map[*asyncapi.Schema] {
	m := asyncapi.NewMap[*asyncapi.Schema]()
	for i := 0; i < len(kv); i += 2 {
		m.Set(kv[i].(string), kv[i+1].(*asyncapi.Schema))
	}
	return m
}

func one[V any](k string, v V) *asyncapi.Map[V] {
	m := asyncapi.NewMap[V]()
	m.Set(k, v)
	return m
}

// kitchenSink builds a document using every object of the model, with and
// without references, so the model is proven against the official schemas.
func kitchenSink(version string) *asyncapi.Document {
	x := asyncapi.Extensions{"x-owner": "team-a"}
	docs := &asyncapi.ExternalDocs{URL: "https://example.com/docs", Description: "Docs", Extensions: x}
	tag := &asyncapi.Tag{Name: "orders", Description: "Orders", ExternalDocs: docs, Extensions: x}
	kafka := asyncapi.NewBindings("kafka", map[string]any{"bindingVersion": "0.5.0"})
	kafka.Set("x-custom", map[string]any{"a": 1})

	schema := &asyncapi.Schema{
		ID: "urn:order", Comment: "c", Title: "Order", Description: "An order.", Type: "object",
		Properties: props(
			"id", &asyncapi.Schema{Type: "string", Format: "uuid", MinLength: u64(1), MaxLength: u64(64), Pattern: "^[a-z]", Examples: []any{"a"}, ReadOnly: true},
			"qty", &asyncapi.Schema{Type: []string{"integer", "string"}, Minimum: 0, ExclusiveMaximum: 100, MultipleOf: 1, Default: 1},
			"tags", &asyncapi.Schema{Type: "array", Items: &asyncapi.Schema{Type: "string"}, MinItems: u64(0), MaxItems: u64(3), UniqueItems: true, Contains: &asyncapi.Schema{Const: "x"}},
			"tuple", &asyncapi.Schema{Type: "array", Items: []*asyncapi.Schema{{Type: "string"}}, AdditionalItems: false},
			"meta", &asyncapi.Schema{Type: "object", AdditionalProperties: &asyncapi.Schema{Type: "string"}, PropertyNames: &asyncapi.Schema{Pattern: "^[a-z]+$"}, MinProperties: u64(0), MaxProperties: u64(5), PatternProperties: props("^x-", &asyncapi.Schema{})},
			"kind", &asyncapi.Schema{Enum: []any{"a", "b"}, WriteOnly: true, Deprecated: true, ContentMediaType: "text/plain", ContentEncoding: "base64"},
			"ref", asyncapi.SchemaRef("Other"),
		),
		Required:      []string{"id"},
		Dependencies:  one[any]("meta", []string{"id"}),
		AllOf:         []*asyncapi.Schema{{Required: []string{"id"}}},
		AnyOf:         []*asyncapi.Schema{{Required: []string{"id"}}, {Required: []string{"qty"}}},
		OneOf:         []*asyncapi.Schema{{Required: []string{"kind"}}, {Not: &asyncapi.Schema{Required: []string{"kind"}}}},
		If:            &asyncapi.Schema{Required: []string{"kind"}},
		Then:          &asyncapi.Schema{Required: []string{"id"}},
		Else:          &asyncapi.Schema{},
		Definitions:   props("inner", &asyncapi.Schema{Type: "string"}),
		Discriminator: "kind",
		ExternalDocs:  docs,
		Extensions:    asyncapi.Extensions{"x-protobuf-message": "acme.v1.Order"},
	}
	msg := &asyncapi.Message{
		Headers:       &asyncapi.Schema{Type: "object", Properties: props("trace", &asyncapi.Schema{Type: "string"})},
		Payload:       asyncapi.SchemaRef("Order"),
		CorrelationID: &asyncapi.CorrelationID{Description: "id", Location: "$message.payload#/id", Extensions: x},
		ContentType:   "application/json", Name: "Order", Title: "Order", Summary: "An order.", Description: "Long.",
		Tags: []*asyncapi.Tag{tag, {Ref: "#/components/tags/audit"}}, ExternalDocs: docs, Deprecated: true,
		Bindings:   asyncapi.NewBindings("kafka", map[string]any{"key": map[string]any{"type": "string"}}),
		Examples:   []*asyncapi.MessageExample{{Name: "one", Summary: "One", Headers: map[string]any{"trace": "t"}, Payload: map[string]any{"id": "a"}}},
		Traits:     []*asyncapi.MessageTrait{{Ref: "#/components/messageTraits/common"}, {Name: "n", Deprecated: true, Bindings: kafka}},
		Extensions: x,
	}
	protoMsg := &asyncapi.Message{Payload: &asyncapi.MultiFormatSchema{
		SchemaFormat: "application/vnd.google.protobuf;version=3", Schema: "message Order { string id = 1; }",
	}}
	secUser := &asyncapi.SecurityScheme{Type: asyncapi.SecurityUserPassword, Description: "creds", Extensions: x}
	schemes := asyncapi.NewMap[*asyncapi.SecurityScheme]()
	schemes.Set("user", secUser)
	schemes.Set("apikey", &asyncapi.SecurityScheme{Type: asyncapi.SecurityAPIKey, In: "user"})
	schemes.Set("httpkey", &asyncapi.SecurityScheme{Type: asyncapi.SecurityHTTPAPIKey, Name: "X-Key", In: "header"})
	schemes.Set("bearer", &asyncapi.SecurityScheme{Type: asyncapi.SecurityHTTP, Scheme: "bearer", BearerFormat: "JWT"})
	schemes.Set("oauth", &asyncapi.SecurityScheme{Type: asyncapi.SecurityOAuth2, Scopes: []string{"read"}, Flows: &asyncapi.OAuthFlows{
		Implicit:          &asyncapi.OAuthFlow{AuthorizationURL: "https://a.example/auth", AvailableScopes: one("read", "Read access")},
		Password:          &asyncapi.OAuthFlow{TokenURL: "https://a.example/token", AvailableScopes: one("read", "Read")},
		ClientCredentials: &asyncapi.OAuthFlow{TokenURL: "https://a.example/token", RefreshURL: "https://a.example/refresh", AvailableScopes: one("read", "Read")},
		AuthorizationCode: &asyncapi.OAuthFlow{AuthorizationURL: "https://a.example/auth", TokenURL: "https://a.example/token", AvailableScopes: one("read", "Read"), Extensions: x},
	}})
	schemes.Set("oidc", &asyncapi.SecurityScheme{Type: asyncapi.SecurityOpenIDConnect, OpenIDConnectURL: "https://a.example/.well-known", Scopes: []string{"read"}})
	for _, t := range []string{asyncapi.SecurityX509, asyncapi.SecuritySymmetricEncryption, asyncapi.SecurityAsymmetricEncryption,
		asyncapi.SecurityPlain, asyncapi.SecurityScramSHA256, asyncapi.SecurityScramSHA512, asyncapi.SecurityGSSAPI} {
		schemes.Set(strings.ToLower(t), &asyncapi.SecurityScheme{Type: t})
	}

	serverBindings := asyncapi.NewBindings("kafka", map[string]any{"schemaRegistryUrl": "https://r.example"})
	opBindings := asyncapi.NewBindings("nats", map[string]any{"queue": "q", "bindingVersion": "0.1.0"})
	if version != "3.0.0" {
		serverBindings.Set("ros2", map[string]any{"rmwImplementation": "rmw_fastrtps_cpp", "domainId": 0})
	}

	channel := &asyncapi.Channel{
		Address:  str("orders.{region}"),
		Messages: one("order", msg),
		Title:    "Orders", Summary: "Orders.", Description: "All orders.",
		Servers:    []*asyncapi.Reference{asyncapi.Ref("#/servers/prod")},
		Parameters: one("region", &asyncapi.Parameter{Enum: []string{"eu", "us"}, Default: "eu", Description: "Region", Examples: []string{"eu"}, Location: "$message.payload#/region", Extensions: x}),
		Tags:       []*asyncapi.Tag{tag}, ExternalDocs: docs,
		Bindings:   asyncapi.NewBindings("kafka", map[string]any{"topic": "orders"}),
		Extensions: x,
	}
	channel.Messages.Set("ref", asyncapi.MessageRef("order"))
	replyCh := &asyncapi.Channel{Messages: one("order", asyncapi.MessageRef("order"))} // address: null

	doc := &asyncapi.Document{
		AsyncAPI: version, ID: "urn:example",
		Info: asyncapi.Info{
			Title: "Kitchen sink", Version: "1.0.0", Description: "Everything.", TermsOfService: "https://example.com/tos",
			Contact: &asyncapi.Contact{Name: "Team", URL: "https://example.com", Email: "team@example.com", Extensions: x},
			License: &asyncapi.License{Name: "Apache-2.0", URL: "https://www.apache.org/licenses/LICENSE-2.0", Extensions: x},
			Tags:    []*asyncapi.Tag{tag}, ExternalDocs: docs, Extensions: x,
		},
		Servers: one("prod", &asyncapi.Server{
			Host: "{env}.example.com:9092", Protocol: "kafka", ProtocolVersion: "3.6", Pathname: "/", Title: "Prod", Summary: "Prod.", Description: "Production.",
			Variables: one("env", &asyncapi.ServerVariable{Enum: []string{"prod"}, Default: "prod", Description: "Env", Examples: []string{"prod"}, Extensions: x}),
			Security:  []*asyncapi.SecurityScheme{asyncapi.SecuritySchemeRef("user"), {Type: asyncapi.SecurityX509}},
			Tags:      []*asyncapi.Tag{tag}, ExternalDocs: docs, Bindings: serverBindings, Extensions: x,
		}),
		DefaultContentType: "application/json",
		Channels:           one("orders", channel),
		Operations: one("sendOrder", &asyncapi.Operation{
			Action: asyncapi.ActionSend, Channel: asyncapi.Ref("#/channels/orders"),
			Title: "Send", Summary: "Send.", Description: "Send an order.",
			Security: []*asyncapi.SecurityScheme{asyncapi.SecuritySchemeRef("oauth")},
			Tags:     []*asyncapi.Tag{tag}, ExternalDocs: docs, Bindings: opBindings,
			Traits:   []*asyncapi.OperationTrait{{Ref: "#/components/operationTraits/common"}, {Title: "T", Summary: "S", Description: "D", Tags: []*asyncapi.Tag{tag}, ExternalDocs: docs, Bindings: opBindings, Extensions: x}},
			Messages: []*asyncapi.Reference{asyncapi.Ref("#/channels/orders/messages/order")},
			Reply: &asyncapi.OperationReply{
				Address:    &asyncapi.OperationReplyAddress{Description: "Inbox", Location: "$message.header#/replyTo", Extensions: x},
				Channel:    asyncapi.Ref("#/channels/replies"),
				Messages:   []*asyncapi.Reference{asyncapi.Ref("#/channels/replies/messages/order")},
				Extensions: x,
			},
			Extensions: x,
		}),
		Components: &asyncapi.Components{
			Schemas:           props("Order", schema, "Other", &asyncapi.Schema{Type: "null"}),
			Servers:           one("staging", &asyncapi.Server{Host: "staging.example.com", Protocol: "kafka"}),
			Channels:          one("dlq", &asyncapi.Channel{Address: str("orders.dlq")}),
			Operations:        one("receiveDlq", &asyncapi.Operation{Action: asyncapi.ActionReceive, Channel: asyncapi.Ref("#/channels/orders")}),
			Messages:          one("order", msg),
			SecuritySchemes:   schemes,
			ServerVariables:   one("env", &asyncapi.ServerVariable{Default: "prod"}),
			Parameters:        one("region", &asyncapi.Parameter{Description: "Region"}),
			CorrelationIDs:    one("byId", &asyncapi.CorrelationID{Location: "$message.payload#/id"}),
			Replies:           one("inbox", &asyncapi.OperationReply{Address: &asyncapi.OperationReplyAddress{Ref: "#/components/replyAddresses/inbox"}}),
			ReplyAddresses:    one("inbox", &asyncapi.OperationReplyAddress{Location: "$message.header#/replyTo"}),
			ExternalDocs:      one("docs", docs),
			Tags:              one("audit", &asyncapi.Tag{Name: "audit"}),
			OperationTraits:   one("common", &asyncapi.OperationTrait{Summary: "Common"}),
			MessageTraits:     one("common", &asyncapi.MessageTrait{Headers: &asyncapi.Schema{Type: "object"}, CorrelationID: &asyncapi.CorrelationID{Ref: "#/components/correlationIds/byId"}, ContentType: "application/json", Examples: []*asyncapi.MessageExample{{Payload: map[string]any{}}}}),
			ServerBindings:    one("kafka", asyncapi.NewBindings("kafka", map[string]any{})),
			ChannelBindings:   one("kafka", asyncapi.NewBindings("kafka", map[string]any{})),
			OperationBindings: one("kafka", asyncapi.NewBindings("kafka", map[string]any{})),
			MessageBindings:   one("kafka", asyncapi.NewBindings("kafka", map[string]any{})),
			Extensions:        x,
		},
		Extensions: x,
	}
	doc.Channels.Set("replies", replyCh)
	doc.Components.Messages.Set("proto", protoMsg)
	doc.Components.Channels.Set("ref", &asyncapi.Channel{Ref: "#/channels/orders"})
	doc.Components.Servers.Set("ref", asyncapi.ServerRef("prod"))
	doc.Components.OperationTraits.Set("ref", &asyncapi.OperationTrait{Ref: "#/components/operationTraits/common"})
	return doc
}

func TestKitchenSink(t *testing.T) {
	for _, v := range asyncapi.Versions {
		t.Run(v, func(t *testing.T) {
			doc := kitchenSink(v)
			if err := asyncapi.Validate(doc); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			for _, enc := range []struct {
				name string
				fn   func() ([]byte, error)
			}{
				{"yaml", func() ([]byte, error) { return asyncapi.MarshalYAML(doc, "# header\n") }},
				{"json", func() ([]byte, error) { return asyncapi.MarshalJSON(doc) }},
			} {
				out, err := enc.fn()
				if err != nil {
					t.Fatal(err)
				}
				if err := spec.Validate(out); err != nil {
					t.Errorf("%s output is not a valid AsyncAPI %s document:\n%v\n%s", enc.name, v, err, out)
				}
				// Kept as golden files so tools/asyncapi-validate also runs
				// them through the official JavaScript parser.
				golden := filepath.Join("testdata", "golden", "kitchen_sink_"+v+"."+enc.name)
				if *update {
					if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(golden, out, 0o644); err != nil {
						t.Fatal(err)
					}
				} else if want, err := os.ReadFile(golden); err != nil || !bytes.Equal(want, out) {
					t.Errorf("%s differs from %s (run go test ./internal/asyncapi -update)", enc.name, golden)
				}
			}
		})
	}
}

func TestReferencesRenderAlone(t *testing.T) {
	out, err := asyncapi.MarshalJSON(&asyncapi.Tag{Ref: "#/components/tags/a", Name: "ignored"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(strings.Fields(string(out)), ""); got != `{"$ref":"#/components/tags/a"}` {
		t.Errorf("reference rendered as %s", got)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(d *asyncapi.Document)
		want   string
	}{
		{"version", func(d *asyncapi.Document) { d.AsyncAPI = "2.6.0" }, `unsupported version "2.6.0"`},
		{"ros2 on 3.0.0", func(d *asyncapi.Document) {
			d.AsyncAPI = "3.0.0"
			s, _ := d.Servers.Get("prod")
			s.Bindings.Set("ros2", map[string]any{})
		}, `"ros2" is not a server binding of AsyncAPI 3.0.0`},
		{"unknown binding", func(d *asyncapi.Document) {
			o, _ := d.Operations.Get("sendOrder")
			o.Bindings.Set("temporal", map[string]any{})
		}, `"temporal" is not a operation binding`},
		{"pulsar is channel-only", func(d *asyncapi.Document) {
			o, _ := d.Operations.Get("sendOrder")
			o.Bindings.Set("pulsar", map[string]any{})
		}, `"pulsar" is not a operation binding`},
		{"server extension key", func(d *asyncapi.Document) {
			s, _ := d.Servers.Get("prod")
			s.Extensions = asyncapi.Extensions{"owner": 1}
		}, `extension "owner" must start with "x-"`},
		{"dangling ref", func(d *asyncapi.Document) {
			c, _ := d.Channels.Get("orders")
			c.Servers = []*asyncapi.Reference{asyncapi.Ref("#/servers/nope")}
		}, `$ref "#/servers/nope" does not resolve`},
		{"foreign message", func(d *asyncapi.Document) {
			o, _ := d.Operations.Get("sendOrder")
			o.Messages = append(o.Messages, asyncapi.Ref("#/channels/replies/messages/order"))
		}, `is not a message of channel "#/channels/orders"`},
		{"undeclared parameter", func(d *asyncapi.Document) {
			c, _ := d.Channels.Get("orders")
			c.Address = str("orders.{region}.{id}")
		}, `address parameter {id} has no parameters entry`},
		{"unused parameter", func(d *asyncapi.Document) {
			c, _ := d.Channels.Get("orders")
			c.Address = str("orders")
		}, `parameter "region" does not appear in address`},
		{"bad location", func(d *asyncapi.Document) {
			m, _ := d.Components.Messages.Get("order")
			m.CorrelationID = &asyncapi.CorrelationID{Location: "payload.id"}
		}, `location "payload.id" is not a runtime expression`},
		{"scheme field not allowed", func(d *asyncapi.Document) {
			s, _ := d.Components.SecuritySchemes.Get("user")
			s.Scopes = []string{"x"}
		}, `"scopes" is not allowed for security scheme type "userPassword"`},
		{"scheme field required", func(d *asyncapi.Document) {
			d.Components.SecuritySchemes.Set("h", &asyncapi.SecurityScheme{Type: asyncapi.SecurityHTTP})
		}, `"scheme" is required for security scheme type "http"`},
		{"bad key", func(d *asyncapi.Document) { d.Channels.Set("orders/all", &asyncapi.Channel{}) }, `key "orders/all" must match`},
		{"duplicate tag", func(d *asyncapi.Document) {
			o, _ := d.Operations.Get("sendOrder")
			o.Tags = append(o.Tags, &asyncapi.Tag{Name: "orders"})
		}, `duplicate tag "orders"`},
		{"oauth flow urls", func(d *asyncapi.Document) {
			s, _ := d.Components.SecuritySchemes.Get("oauth")
			s.Flows.Implicit.TokenURL = "https://a.example/token"
		}, `flows.implicit: tokenUrl is not allowed`},
		{"duplicate tag via ref", func(d *asyncapi.Document) {
			o, _ := d.Operations.Get("sendOrder")
			d.Components.Tags.Set("orders", &asyncapi.Tag{Name: "orders"})
			o.Tags = append(o.Tags, &asyncapi.Tag{Ref: "#/components/tags/orders"})
		}, `duplicate tag "orders" (via #/components/tags/orders)`},
		{"license name", func(d *asyncapi.Document) { d.Info.License.Name = "" }, `license: name is required`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := kitchenSink("3.1.0")
			tc.mutate(d)
			err := asyncapi.Validate(d)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
