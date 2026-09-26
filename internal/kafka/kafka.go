// Package kafka documents Apache Kafka APIs described by Protobuf services:
// topics, record keys, producers, consumer groups and request/reply.
package kafka

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
	kafkav1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/kafka/asyncapi/v1"
)

// BindingKey is the key of the official Kafka bindings.
const BindingKey = "kafka"

// BindingVersion is the version of the official Kafka bindings.
const BindingVersion = "0.5.0"

// ExtensionKey holds Kafka settings the official bindings do not define.
const ExtensionKey = "x-kafka"

const emptyFullName = "google.protobuf.Empty"

var (
	paramRE     = regexp.MustCompile(`\{([^{}]*)\}`)
	paramNameRE = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)
	topicRE     = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
	unsafeIDRE  = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
)

// Protocol implements core.Protocol for Kafka.
type Protocol struct {
	// Per document state, reset by Begin.
	b        *core.Builder
	settings []*fileSettings
	topics   map[string]*kafkav1.Topic
	chans    map[*core.Channel]string // topic name
	order    []*core.Channel
}

type fileSettings struct {
	file *protogen.File
	doc  *kafkav1.Document
}

// New returns the Kafka protocol.
func New() core.Protocol { return &Protocol{} }

func (p *Protocol) Name() string { return "kafka" }

func (p *Protocol) Options() []core.Option { return nil }

func documentOptions(f *protogen.File) *kafkav1.Document {
	return core.Extension[*kafkav1.Document](f.Desc.Options(), kafkav1.E_Document)
}

func serviceOptions(s *protogen.Service) *kafkav1.Service {
	return core.Extension[*kafkav1.Service](s.Desc.Options(), kafkav1.E_Service)
}

func operationOptions(m *protogen.Method) *kafkav1.Operation {
	return core.Extension[*kafkav1.Operation](m.Desc.Options(), kafkav1.E_Operation)
}

func messageOptions(m *protogen.Message) *kafkav1.Message {
	if m == nil {
		return nil
	}
	return core.Extension[*kafkav1.Message](m.Desc.Options(), kafkav1.E_Message)
}

// Claims documents services with Kafka annotations.
func (p *Protocol) Claims(_ *core.Builder, s *protogen.Service) bool {
	if serviceOptions(s) != nil {
		return true
	}
	for _, m := range s.Methods {
		if operationOptions(m) != nil {
			return true
		}
	}
	return false
}

// HasContent reports messages with their own topic, or Kafka document
// options in the documented files.
func (p *Protocol) HasContent(b *core.Builder) bool {
	for _, f := range b.Files {
		if documentOptions(f) != nil {
			return true
		}
		found := false
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if messageOptions(m).GetTopic() != "" {
				found = true
			}
		})
		if found {
			return true
		}
	}
	return false
}

func (p *Protocol) Begin(b *core.Builder) error {
	p.b = b
	p.settings = nil
	p.topics = map[string]*kafkav1.Topic{}
	p.chans = map[*core.Channel]string{}
	p.order = nil
	for _, f := range b.Imports() {
		if d := documentOptions(f); d != nil {
			p.settings = append(p.settings, &fileSettings{file: f, doc: d})
		}
	}
	if err := p.serverDetails(); err != nil {
		return err
	}
	var errs []string
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, t := range set.doc.GetTopics() {
			if _, err := parseTopic(t.GetName()); err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", where, err))
				continue
			}
			if _, dup := p.topics[t.GetName()]; dup {
				errs = append(errs, fmt.Sprintf("%s: topic %q is declared twice", where, t.GetName()))
				continue
			}
			if err := checkTopic(t); err != nil {
				errs = append(errs, fmt.Sprintf("%s: topic %q: %v", where, t.GetName(), err))
			}
			p.topics[t.GetName()] = t
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "\n"))
	}
	// Declared topics are documented even when no rpc uses them.
	for _, set := range p.settings {
		if !b.OwnFile(set.file) {
			continue
		}
		for _, t := range set.doc.GetTopics() {
			if _, err := p.topicChannel(set.file.Desc, t.GetName(), core.ChannelSpec{}); err != nil {
				return err
			}
		}
	}
	return nil
}

var mechanisms = map[kafkav1.Mechanism]struct{ typ, name string }{
	kafkav1.Mechanism_MECHANISM_PLAIN:         {asyncapi.SecurityPlain, "PLAIN"},
	kafkav1.Mechanism_MECHANISM_SCRAM_SHA_256: {asyncapi.SecurityScramSHA256, "SCRAM-SHA-256"},
	kafkav1.Mechanism_MECHANISM_SCRAM_SHA_512: {asyncapi.SecurityScramSHA512, "SCRAM-SHA-512"},
	kafkav1.Mechanism_MECHANISM_GSSAPI:        {asyncapi.SecurityGSSAPI, "GSSAPI"},
	kafkav1.Mechanism_MECHANISM_OAUTHBEARER:   {"", "OAUTHBEARER"},
	kafkav1.Mechanism_MECHANISM_MTLS:          {asyncapi.SecurityX509, "MTLS"},
}

// serverDetails adds the official server bindings and the mechanism of
// security schemes.
func (p *Protocol) serverDetails() error {
	b := p.b
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, srv := range set.doc.GetServers() {
			out, ok := b.Doc.Servers.Get(srv.GetName())
			if !ok {
				return fmt.Errorf("%s: kafka server %q is not declared in the servers of the asyncapi.v3.document file option", where, srv.GetName())
			}
			bd := asyncapi.NewMap[any]()
			if u := srv.GetSchemaRegistryUrl(); u != "" {
				if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
					return fmt.Errorf("%s: kafka server %q: schema_registry_url %q must be an http(s) URL", where, srv.GetName(), u)
				}
				bd.Set("schemaRegistryUrl", u)
			}
			if srv.GetSchemaRegistryVendor() != "" {
				bd.Set("schemaRegistryVendor", srv.GetSchemaRegistryVendor())
			}
			bd.Set("bindingVersion", BindingVersion)
			if out.Bindings == nil {
				out.Bindings = &asyncapi.Bindings{}
			}
			out.Bindings.Set(BindingKey, bd)
		}
		for _, a := range set.doc.GetAuth() {
			var sc *asyncapi.SecurityScheme
			if c := b.Doc.Components; c != nil {
				sc, _ = c.SecuritySchemes.Get(a.GetScheme())
			}
			if sc == nil {
				return fmt.Errorf("%s: kafka auth for %q: the security scheme is not declared in the asyncapi.v3.document file option", where, a.GetScheme())
			}
			mech, ok := mechanisms[a.GetMechanism()]
			if !ok {
				return fmt.Errorf("%s: kafka auth for %q: mechanism is required", where, a.GetScheme())
			}
			switch {
			case mech.typ == "" && sc.Type != asyncapi.SecurityOAuth2 && sc.Type != asyncapi.SecurityOpenIDConnect:
				return fmt.Errorf("%s: kafka auth for %q: OAUTHBEARER needs an oauth2 or openIdConnect security scheme", where, a.GetScheme())
			case sc.Type == "":
				sc.Type = mech.typ
			}
			if sc.Extensions == nil {
				sc.Extensions = asyncapi.Extensions{}
			}
			sc.Extensions["x-kafka-mechanism"] = mech.name
		}
	}
	return nil
}

// parseTopic validates a topic name template and returns its parameters.
func parseTopic(name string) ([]string, error) {
	var params []string
	seen := map[string]bool{}
	for _, m := range paramRE.FindAllStringSubmatch(name, -1) {
		if !paramNameRE.MatchString(m[1]) {
			return nil, fmt.Errorf("topic %q: invalid parameter name %q (allowed: letters, digits, '_' and '-')", name, m[1])
		}
		if seen[m[1]] {
			return nil, fmt.Errorf("topic %q: parameter %q is used twice", name, m[1])
		}
		seen[m[1]] = true
		params = append(params, m[1])
	}
	plain := paramRE.ReplaceAllString(name, "p")
	switch {
	case strings.ContainsAny(plain, "{}"):
		return nil, fmt.Errorf("topic %q has unbalanced braces", name)
	case !topicRE.MatchString(plain) || plain == "." || plain == "..":
		return nil, fmt.Errorf("topic %q must be 1 to 249 letters, digits, '.', '_' and '-'", name)
	}
	return params, nil
}

// millis parses a duration into milliseconds.
func millis(field, value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration like \"30s\" or \"168h\"", field, value)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %q", field, value)
	}
	return d.Milliseconds(), nil
}

// typedConfigs are the configurations Topic sets with typed fields.
var typedConfigs = []string{
	"cleanup.policy", "retention.ms", "retention.bytes", "delete.retention.ms", "max.message.bytes",
	"min.insync.replicas", "compression.type", "confluent.key.schema.validation", "confluent.value.schema.validation",
	"confluent.key.subject.name.strategy", "confluent.value.subject.name.strategy",
}

func checkTopic(t *kafkav1.Topic) error {
	switch {
	case t.GetPartitions() < 0 || t.GetReplicas() < 0 || t.GetMinInsyncReplicas() < 0 || t.GetMaxMessageBytes() < 0:
		return fmt.Errorf("partitions, replicas, min_insync_replicas and max_message_bytes must be >= 0")
	case t.GetMinInsyncReplicas() > 0 && t.GetReplicas() > 0 && t.GetMinInsyncReplicas() > t.GetReplicas():
		return fmt.Errorf("min_insync_replicas %d exceeds replicas %d", t.GetMinInsyncReplicas(), t.GetReplicas())
	case t.GetRetentionBytes() < -1:
		return fmt.Errorf("retention_bytes must be -1 (no limit) or >= 0")
	}
	if r := t.GetRetention(); r != "-1" {
		if _, err := millis("retention", r); err != nil {
			return err
		}
	}
	if _, err := millis("delete_retention", t.GetDeleteRetention()); err != nil {
		return err
	}
	seen := map[kafkav1.CleanupPolicy]bool{}
	for _, c := range t.GetCleanupPolicy() {
		if c == kafkav1.CleanupPolicy_CLEANUP_POLICY_UNSPECIFIED || seen[c] {
			return fmt.Errorf("cleanup_policy lists an unspecified or repeated policy")
		}
		seen[c] = true
	}
	for k := range t.GetConfigs() {
		for _, typed := range typedConfigs {
			if k == typed {
				return fmt.Errorf("configs: %q has a typed field; set it instead", k)
			}
		}
	}
	return nil
}

func compacted(t *kafkav1.Topic) bool {
	for _, c := range t.GetCleanupPolicy() {
		if c == kafkav1.CleanupPolicy_CLEANUP_POLICY_COMPACT {
			return true
		}
	}
	return false
}

func lower(e fmt.Stringer, prefix string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(e.String(), prefix)), "_", "-")
}

// topicChannel returns the channel of a topic.
func (p *Protocol) topicChannel(d protoreflect.Descriptor, name string, spec core.ChannelSpec) (*core.Channel, error) {
	params, err := parseTopic(name)
	if err != nil {
		return nil, core.Errorf(d, "%v", err)
	}
	if t := p.topics[name]; t.GetDescription() != "" {
		spec.Meta = append(spec.Meta, &asyncapiv3.Channel{Description: t.GetDescription()})
	}
	spec.DeclaredBy, spec.Address, spec.Params = d, name, params
	spec.DefaultID = strings.Trim(unsafeIDRE.ReplaceAllString(strings.NewReplacer("{", "", "}", "").Replace(name), "_"), "._")
	spec.Key = "kafka\x00" + name
	ch, err := p.b.Channel(spec)
	if err != nil {
		return nil, err
	}
	if _, ok := p.chans[ch]; !ok {
		p.chans[ch] = name
		p.order = append(p.order, ch)
	}
	return ch, nil
}

func (p *Protocol) Finish(b *core.Builder) error {
	// Messages with their own topic get a channel even without operation.
	for _, f := range b.Files {
		var list []*protogen.Message
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if messageOptions(m).GetTopic() != "" {
				list = append(list, m)
			}
		})
		for _, m := range list {
			name := messageOptions(m).GetTopic()
			if compacted(p.topics[name]) && messageOptions(m).GetKey() == nil {
				return core.Errorf(m.Desc, "topic %q is compacted: its records need keys; set (kafka.asyncapi.v1.message).key", name)
			}
			meta := core.MessageOptions(m).GetChannel()
			ch, err := p.topicChannel(m.Desc, name, core.ChannelSpec{Meta: metas(meta), Servers: meta.GetServers()})
			if err != nil {
				return err
			}
			msg, err := b.Message(m)
			if err != nil {
				return err
			}
			ch.AddMessage(msg)
		}
	}
	done := map[*core.Message]bool{}
	for _, ch := range p.order {
		p.emitChannel(ch)
		for _, m := range ch.Messages {
			if done[m] {
				continue
			}
			done[m] = true
			if err := p.emitMessage(m); err != nil {
				return err
			}
		}
	}
	return nil
}

// emitChannel renders the official channel binding of a declared topic.
func (p *Protocol) emitChannel(ch *core.Channel) {
	t := p.topics[p.chans[ch]]
	if t == nil {
		return
	}
	bd := asyncapi.NewMap[any]()
	if t.GetPartitions() > 0 {
		bd.Set("partitions", t.GetPartitions())
	}
	if t.GetReplicas() > 0 {
		bd.Set("replicas", t.GetReplicas())
	}
	cfg := asyncapi.NewMap[any]()
	if len(t.GetCleanupPolicy()) > 0 {
		var list []string
		for _, c := range t.GetCleanupPolicy() {
			list = append(list, lower(c, "CLEANUP_POLICY_"))
		}
		cfg.Set("cleanup.policy", list)
	}
	switch r := t.GetRetention(); r {
	case "":
	case "-1":
		cfg.Set("retention.ms", int64(-1))
	default:
		ms, _ := millis("", r) // validated by Begin
		cfg.Set("retention.ms", ms)
	}
	if t.GetRetentionBytes() != 0 {
		cfg.Set("retention.bytes", t.GetRetentionBytes())
	}
	if t.GetDeleteRetention() != "" {
		ms, _ := millis("", t.GetDeleteRetention())
		cfg.Set("delete.retention.ms", ms)
	}
	if t.GetMaxMessageBytes() > 0 {
		cfg.Set("max.message.bytes", t.GetMaxMessageBytes())
	}
	if t.GetMinInsyncReplicas() > 0 {
		cfg.Set("min.insync.replicas", t.GetMinInsyncReplicas())
	}
	if c := t.GetCompression(); c != kafkav1.Compression_COMPRESSION_UNSPECIFIED {
		cfg.Set("compression.type", lower(c, "COMPRESSION_"))
	}
	if t.GetKeySchemaValidation() {
		cfg.Set("confluent.key.schema.validation", true)
	}
	if t.GetKeySubjectNameStrategy() != "" {
		cfg.Set("confluent.key.subject.name.strategy", t.GetKeySubjectNameStrategy())
	}
	if t.GetValueSchemaValidation() {
		cfg.Set("confluent.value.schema.validation", true)
	}
	if t.GetValueSubjectNameStrategy() != "" {
		cfg.Set("confluent.value.subject.name.strategy", t.GetValueSubjectNameStrategy())
	}
	for _, k := range core.SortedKeys(t.GetConfigs()) {
		cfg.Set(k, t.GetConfigs()[k])
	}
	if cfg.Len() > 0 {
		bd.Set("topicConfiguration", cfg)
	}
	bd.Set("bindingVersion", BindingVersion)
	if ch.Obj.Bindings == nil {
		ch.Obj.Bindings = &asyncapi.Bindings{}
	}
	ch.Obj.Bindings.Set(BindingKey, bd)
}

// emitMessage renders the official message binding: the key and schema
// registry details.
func (p *Protocol) emitMessage(m *core.Message) error {
	if m.Proto == nil {
		return nil
	}
	o := messageOptions(m.Proto)
	bd := asyncapi.NewMap[any]()
	if k := o.GetKey(); k != nil {
		key, err := p.keySchema(m.Proto, k)
		if err != nil {
			return err
		}
		bd.Set("key", key)
	}
	if loc := o.GetSchemaIdLocation(); loc != kafkav1.SchemaIdLocation_SCHEMA_ID_LOCATION_UNSPECIFIED {
		bd.Set("schemaIdLocation", lower(loc, "SCHEMA_ID_LOCATION_"))
	}
	if o.GetSchemaIdPayloadEncoding() != "" {
		if o.GetSchemaIdLocation() != kafkav1.SchemaIdLocation_SCHEMA_ID_LOCATION_PAYLOAD {
			return core.Errorf(m.Proto.Desc, "schema_id_payload_encoding requires schema_id_location PAYLOAD")
		}
		bd.Set("schemaIdPayloadEncoding", o.GetSchemaIdPayloadEncoding())
	}
	if o.GetSchemaLookupStrategy() != "" {
		bd.Set("schemaLookupStrategy", o.GetSchemaLookupStrategy())
	}
	if bd.Len() == 0 {
		return nil
	}
	bd.Set("bindingVersion", BindingVersion)
	if m.Obj.Bindings == nil {
		m.Obj.Bindings = &asyncapi.Bindings{}
	}
	m.Obj.Bindings.Set(BindingKey, bd)
	return nil
}

// keySchema returns the schema of a record key.
func (p *Protocol) keySchema(m *protogen.Message, k *kafkav1.Key) (*asyncapi.Schema, error) {
	var s *asyncapi.Schema
	switch v := k.GetKey().(type) {
	case *kafkav1.Key_Field:
		var field *protogen.Field
		for _, f := range m.Fields {
			if string(f.Desc.Name()) == v.Field || f.Desc.JSONName() == v.Field {
				field = f
			}
		}
		if field == nil {
			return nil, core.Errorf(m.Desc, "key.field: %s has no field %q", m.Desc.Name(), v.Field)
		}
		if field.Desc.IsList() || field.Desc.IsMap() {
			return nil, core.Errorf(m.Desc, "key.field %q must not be repeated or a map", v.Field)
		}
		fs, err := p.b.FieldSchema(field)
		if err != nil {
			return nil, err
		}
		s = fs
	case *kafkav1.Key_Message:
		km := p.b.FindMessage(protoreflect.FullName(strings.TrimPrefix(v.Message, ".")))
		if km == nil {
			return nil, core.Errorf(m.Desc, "key.message %q not found (use the full Protobuf name, e.g. \"acme.v1.OrderKey\")", v.Message)
		}
		ref, err := p.b.Schema(km)
		if err != nil {
			return nil, err
		}
		s = &asyncapi.Schema{AllOf: []*asyncapi.Schema{ref}}
	case *kafkav1.Key_Schema:
		ks, err := core.DecodeSchema("key.schema", v.Schema)
		if err != nil {
			return nil, core.Errorf(m.Desc, "%v", err)
		}
		s = ks
	default:
		return nil, core.Errorf(m.Desc, "key must set field, message or schema")
	}
	if k.GetDescription() != "" {
		s.Description = k.GetDescription()
	}
	return s, nil
}

func metas[T any](xs ...*T) []*T {
	var out []*T
	for _, x := range xs {
		if x != nil {
			out = append(out, x)
		}
	}
	return out
}
