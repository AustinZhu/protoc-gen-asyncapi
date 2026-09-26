// Package mqtt documents MQTT 3.1.1 and 5 APIs described by Protobuf
// services: topics and filters, QoS, retained messages, shared
// subscriptions and MQTT 5 request/response.
package mqtt

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	mqttv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/mqtt/asyncapi/v1"
)

// BindingKey is the key of the official MQTT bindings.
const BindingKey = "mqtt"

// BindingVersion is the version of the official MQTT bindings.
const BindingVersion = "0.2.0"

// ExtensionKey holds MQTT details the official bindings do not define.
const ExtensionKey = "x-mqtt"

const emptyFullName = "google.protobuf.Empty"

var (
	paramNameRE = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)
	unsafeIDRE  = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
)

// Protocol implements core.Protocol for MQTT.
type Protocol struct {
	// Per document state, reset by Begin.
	b        *core.Builder
	settings []*fileSettings
	// v3 is set when every declared server speaks MQTT 3.1.1.
	v3    bool
	chans map[*core.Channel]*topic
	order []*core.Channel
	// responses holds the response topic of request messages: a topic
	// template, or "" when each requester chooses one.
	responses map[*core.Message]*string
}

type fileSettings struct {
	file *protogen.File
	doc  *mqttv1.Document
}

// New returns the MQTT protocol.
func New() core.Protocol { return &Protocol{} }

func (p *Protocol) Name() string { return "mqtt" }

func (p *Protocol) Options() []core.Option { return nil }

func documentOptions(f *protogen.File) *mqttv1.Document {
	return core.Extension[*mqttv1.Document](f.Desc.Options(), mqttv1.E_Document)
}

func serviceOptions(s *protogen.Service) *mqttv1.Service {
	return core.Extension[*mqttv1.Service](s.Desc.Options(), mqttv1.E_Service)
}

func operationOptions(m *protogen.Method) *mqttv1.Operation {
	return core.Extension[*mqttv1.Operation](m.Desc.Options(), mqttv1.E_Operation)
}

func messageOptions(m *protogen.Message) *mqttv1.Message {
	if m == nil {
		return nil
	}
	return core.Extension[*mqttv1.Message](m.Desc.Options(), mqttv1.E_Message)
}

// Claims documents services with MQTT annotations.
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

// HasContent reports messages with their own topic, or MQTT document
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
	p.chans = map[*core.Channel]*topic{}
	p.order = nil
	p.responses = map[*core.Message]*string{}
	servers, v3 := 0, 0
	for _, f := range b.Imports() {
		d := documentOptions(f)
		if d == nil {
			continue
		}
		p.settings = append(p.settings, &fileSettings{file: f, doc: d})
		for _, s := range d.GetServers() {
			servers++
			if s.GetVersion() == mqttv1.Version_VERSION_3_1_1 {
				v3++
			}
		}
	}
	p.v3 = servers > 0 && v3 == servers
	return p.serverDetails()
}

// requireV5 reports an MQTT 5 feature used while every server speaks
// MQTT 3.1.1.
func (p *Protocol) requireV5(used bool, feature string) error {
	if used && p.v3 {
		return fmt.Errorf("%s is an MQTT 5 feature, but every declared server speaks MQTT 3.1.1", feature)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

// topic is a parsed topic or topic filter template such as
// "devices/{device_id}/telemetry".
type topic struct {
	raw    string
	levels []string
	params []string
	// wildcard reports "+" or "#" levels.
	wildcard bool
}

// parseTopic validates a topic or topic filter template.
func parseTopic(s string) (*topic, error) {
	switch {
	case s == "":
		return nil, fmt.Errorf("topic must not be empty")
	case len(s) > math.MaxUint16:
		return nil, fmt.Errorf("topic is longer than 65535 bytes")
	case strings.ContainsRune(s, 0):
		return nil, fmt.Errorf("topic %q must not contain NUL", s)
	}
	t := &topic{raw: s, levels: strings.Split(s, "/")}
	seen := map[string]bool{}
	for i, l := range t.levels {
		switch {
		case l == "+":
			t.wildcard = true
		case l == "#":
			if i != len(t.levels)-1 {
				return nil, fmt.Errorf("topic %q: \"#\" must be the last level", s)
			}
			t.wildcard = true
		case strings.HasPrefix(l, "{") && strings.HasSuffix(l, "}"):
			name := l[1 : len(l)-1]
			if !paramNameRE.MatchString(name) {
				return nil, fmt.Errorf("topic %q: invalid parameter name %q (allowed: letters, digits, '_' and '-')", s, name)
			}
			if seen[name] {
				return nil, fmt.Errorf("topic %q: parameter %q is used twice", s, name)
			}
			seen[name] = true
			t.params = append(t.params, name)
		case strings.ContainsAny(l, "{}"):
			return nil, fmt.Errorf("topic %q: parameter in level %q must span the whole level, e.g. \"devices/{id}\"", s, l)
		case strings.ContainsAny(l, "+#"):
			return nil, fmt.Errorf("topic %q: wildcard in level %q must span the whole level", s, l)
		}
	}
	return t, nil
}

// filter returns the topic filter subscribers use: parameters become "+".
func (t *topic) filter() string {
	out := make([]string, len(t.levels))
	for i, l := range t.levels {
		if strings.HasPrefix(l, "{") && strings.HasSuffix(l, "}") {
			l = "+"
		}
		out[i] = l
	}
	return strings.Join(out, "/")
}

// publishable reports whether messages may be published to the topic.
func (t *topic) publishable() error {
	switch {
	case t.wildcard:
		return fmt.Errorf("topic %q: wildcards are only valid in subscriptions", t.raw)
	case strings.HasPrefix(t.raw, "$"):
		return fmt.Errorf("topic %q: topics starting with \"$\" are reserved for the broker", t.raw)
	}
	return nil
}

func joinTopic(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.Trim(p, "/"); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "/")
}

var idReplacer = strings.NewReplacer("/", ".", "{", "", "}", "", "+", "any", "#", "all", "$", "")

// topicChannel returns the channel of a topic.
func (p *Protocol) topicChannel(d protoreflect.Descriptor, name string, spec core.ChannelSpec) (*core.Channel, error) {
	t, err := parseTopic(name)
	if err != nil {
		return nil, core.Errorf(d, "%v", err)
	}
	id := strings.Trim(unsafeIDRE.ReplaceAllString(idReplacer.Replace(name), "_"), "._")
	if id == "" {
		id = "topic"
	}
	spec.DeclaredBy, spec.Address, spec.Params, spec.DefaultID = d, name, t.params, id
	spec.Key = "mqtt\x00" + name
	ch, err := p.b.Channel(spec)
	if err != nil {
		return nil, err
	}
	if _, ok := p.chans[ch]; !ok {
		p.chans[ch] = t
		p.order = append(p.order, ch)
	}
	return ch, nil
}

// ---------------------------------------------------------------------------
// Servers and authentication
// ---------------------------------------------------------------------------

func seconds(field, value string, max int64) (int64, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration like \"30s\" or \"24h\"", field, value)
	}
	s := int64(d / time.Second)
	if d < 0 || s > max {
		return 0, fmt.Errorf("%s must be between 0 and %ds, got %q", field, max, value)
	}
	return s, nil
}

func qos(q mqttv1.Qos) int { return int(q) - 1 }

func (p *Protocol) serverDetails() error {
	b := p.b
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, srv := range set.doc.GetServers() {
			out, ok := b.Doc.Servers.Get(srv.GetName())
			if !ok {
				return fmt.Errorf("%s: mqtt server %q is not declared in the servers of the asyncapi.v3.document file option", where, srv.GetName())
			}
			bd, err := serverBinding(srv)
			if err != nil {
				return fmt.Errorf("%s: mqtt server %q: %v", where, srv.GetName(), err)
			}
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
			w := fmt.Sprintf("%s: mqtt auth for %q", where, a.GetScheme())
			if sc == nil {
				return fmt.Errorf("%s: the security scheme is not declared in the asyncapi.v3.document file option", w)
			}
			typ, method := "", ""
			switch a.GetMethod() {
			case mqttv1.AuthMethod_AUTH_METHOD_USERNAME_PASSWORD:
				typ, method = asyncapi.SecurityUserPassword, "username-password"
			case mqttv1.AuthMethod_AUTH_METHOD_CLIENT_CERTIFICATE:
				typ, method = asyncapi.SecurityX509, "client-certificate"
			case mqttv1.AuthMethod_AUTH_METHOD_ENHANCED:
				if err := p.requireV5(true, "enhanced authentication"); err != nil {
					return fmt.Errorf("%s: %v", w, err)
				}
				method = a.GetEnhancedMethod()
				if method == "" {
					return fmt.Errorf("%s: enhanced authentication needs enhanced_method, e.g. \"SCRAM-SHA-256\"", w)
				}
				switch method {
				case "SCRAM-SHA-256":
					typ = asyncapi.SecurityScramSHA256
				case "SCRAM-SHA-512":
					typ = asyncapi.SecurityScramSHA512
				}
				if typ == "" && sc.Type == "" {
					return fmt.Errorf("%s: set the security scheme's type for authentication method %q", w, method)
				}
			default:
				return fmt.Errorf("%s: method is required", w)
			}
			if a.GetEnhancedMethod() != "" && a.GetMethod() != mqttv1.AuthMethod_AUTH_METHOD_ENHANCED {
				return fmt.Errorf("%s: enhanced_method requires method ENHANCED", w)
			}
			if sc.Type == "" {
				sc.Type = typ
			}
			if sc.Extensions == nil {
				sc.Extensions = asyncapi.Extensions{}
			}
			sc.Extensions["x-mqtt-auth"] = method
		}
	}
	return nil
}

// serverBinding renders the official server binding of a server's client
// connection settings.
func serverBinding(srv *mqttv1.Server) (*asyncapi.Map[any], error) {
	v5 := srv.GetVersion() != mqttv1.Version_VERSION_3_1_1
	lw := srv.GetLastWill()
	for _, f := range []struct {
		used bool
		name string
	}{
		{srv.GetSessionExpiry() != "", "session_expiry"}, {srv.GetMaximumPacketSize() != 0, "maximum_packet_size"},
		{srv.GetReceiveMaximum() != 0, "receive_maximum"}, {srv.GetTopicAliasMaximum() != 0, "topic_alias_maximum"},
		{lw.GetDelay() != "", "last_will.delay"},
	} {
		if f.used && !v5 {
			return nil, fmt.Errorf("%s is an MQTT 5 feature, but the server speaks MQTT 3.1.1", f.name)
		}
	}
	keepAlive, err := seconds("keep_alive", srv.GetKeepAlive(), math.MaxUint16)
	if err != nil {
		return nil, err
	}
	expiry, err := seconds("session_expiry", srv.GetSessionExpiry(), math.MaxUint32)
	if err != nil {
		return nil, err
	}
	willDelay, err := seconds("last_will.delay", lw.GetDelay(), math.MaxUint32)
	if err != nil {
		return nil, err
	}

	bd := asyncapi.NewMap[any]()
	x := asyncapi.NewMap[any]()
	if srv.GetClientId() != "" {
		bd.Set("clientId", srv.GetClientId())
	}
	if srv.CleanStart != nil {
		bd.Set("cleanSession", srv.GetCleanStart())
	}
	if lw != nil {
		t, err := parseTopic(lw.GetTopic())
		if err != nil {
			return nil, fmt.Errorf("last_will: %v", err)
		}
		if err := t.publishable(); err != nil {
			return nil, fmt.Errorf("last_will: %v", err)
		}
		w := asyncapi.NewMap[any]()
		w.Set("topic", lw.GetTopic())
		if lw.GetQos() != mqttv1.Qos_QOS_UNSPECIFIED {
			w.Set("qos", qos(lw.GetQos()))
		}
		if lw.GetMessage() != "" {
			w.Set("message", lw.GetMessage())
		}
		if lw.GetRetain() {
			w.Set("retain", true)
		}
		bd.Set("lastWill", w)
		if lw.GetDelay() != "" {
			x.Set("willDelayInterval", willDelay)
		}
	}
	if srv.GetKeepAlive() != "" {
		bd.Set("keepAlive", keepAlive)
	}
	if srv.GetSessionExpiry() != "" {
		bd.Set("sessionExpiryInterval", expiry)
	}
	if srv.GetMaximumPacketSize() != 0 {
		bd.Set("maximumPacketSize", srv.GetMaximumPacketSize())
	}
	bd.Set("bindingVersion", BindingVersion)
	switch srv.GetVersion() {
	case mqttv1.Version_VERSION_3_1_1:
		x.Set("protocolVersion", "3.1.1")
	case mqttv1.Version_VERSION_5:
		x.Set("protocolVersion", "5")
	}
	if srv.GetReceiveMaximum() != 0 {
		x.Set("receiveMaximum", srv.GetReceiveMaximum())
	}
	if srv.GetTopicAliasMaximum() != 0 {
		x.Set("topicAliasMaximum", srv.GetTopicAliasMaximum())
	}
	if x.Len() > 0 {
		bd.Set(ExtensionKey, x)
	}
	return bd, nil
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

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
			meta := core.MessageOptions(m).GetChannel()
			ch, err := p.topicChannel(m.Desc, messageOptions(m).GetTopic(), core.ChannelSpec{Meta: metas(meta), Servers: meta.GetServers()})
			if err != nil {
				return err
			}
			if err := p.chans[ch].publishable(); err != nil {
				return core.Errorf(m.Desc, "%v", err)
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

// emitMessage renders the official message binding.
func (p *Protocol) emitMessage(m *core.Message) error {
	bd := asyncapi.NewMap[any]()
	var o *mqttv1.Message
	if m.Proto != nil {
		o = messageOptions(m.Proto)
		if err := p.requireV5(o.GetUtf8() || o.GetContentType() != "", "the payload format indicator and content type"); err != nil {
			return core.Errorf(m.Proto.Desc, "%v", err)
		}
	}
	if o.GetUtf8() {
		bd.Set("payloadFormatIndicator", 1)
	}
	if _, ok := p.responses[m]; ok {
		bd.Set("correlationData", &asyncapi.Schema{
			Type: "string", Format: "binary",
			Description: "Correlation Data, returned unchanged in the response.",
		})
	}
	if o.GetContentType() != "" {
		bd.Set("contentType", o.GetContentType())
	}
	if rt, ok := p.responses[m]; ok {
		if *rt != "" {
			bd.Set("responseTopic", *rt)
		} else {
			bd.Set("responseTopic", &asyncapi.Schema{Type: "string", Description: "Response Topic chosen by the requester."})
		}
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

func metas[T any](xs ...*T) []*T {
	var out []*T
	for _, x := range xs {
		if x != nil {
			out = append(out, x)
		}
	}
	return out
}
