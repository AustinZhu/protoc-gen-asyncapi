// Package amqp documents AMQP 0-9-1 (RabbitMQ) APIs described by Protobuf
// services: exchanges, queues and their bindings, publishers, consumers and
// request/reply.
package amqp

import (
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	amqpv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/amqp/asyncapi/v1"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
)

// BindingVersion is the version of the official AMQP bindings.
const BindingVersion = "0.3.0"

// ServerBindingKey is the key of the AMQP server binding: AsyncAPI reserves
// the amqp server binding, so server details go in an x- binding.
const ServerBindingKey = "x-amqp"

// ExtensionKey holds RabbitMQ details inside the official bindings.
const ExtensionKey = "x-rabbitmq"

const emptyFullName = "google.protobuf.Empty"

// Protocol implements core.Protocol for AMQP 0-9-1.
type Protocol struct {
	// Per document state, reset by Begin.
	b        *core.Builder
	settings []*fileSettings
	topo     *topology
	chans    map[*core.Channel]*channelData
	order    []*core.Channel
}

type fileSettings struct {
	file *protogen.File
	doc  *amqpv1.Document
}

// channelData holds the AMQP side of a channel: a routing key on an
// exchange, or a queue.
type channelData struct {
	exchange *exchange // routing key channels
	queue    *queue    // queue channels
	params   []string
}

// New returns the AMQP protocol.
func New() core.Protocol { return &Protocol{} }

func (p *Protocol) Name() string { return "amqp" }

func (p *Protocol) Options() []core.Option { return nil }

func documentOptions(f *protogen.File) *amqpv1.Document {
	return core.Extension[*amqpv1.Document](f.Desc.Options(), amqpv1.E_Document)
}

func serviceOptions(s *protogen.Service) *amqpv1.Service {
	return core.Extension[*amqpv1.Service](s.Desc.Options(), amqpv1.E_Service)
}

func operationOptions(m *protogen.Method) *amqpv1.Operation {
	return core.Extension[*amqpv1.Operation](m.Desc.Options(), amqpv1.E_Operation)
}

func messageOptions(m *protogen.Message) *amqpv1.Message {
	if m == nil {
		return nil
	}
	return core.Extension[*amqpv1.Message](m.Desc.Options(), amqpv1.E_Message)
}

// standalone reports a message with its own channel.
func standalone(o *amqpv1.Message) bool { return o.GetExchange() != "" || o.GetRoutingKey() != "" }

// Claims documents services with AMQP annotations.
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

// HasContent reports messages with their own channel, or AMQP document
// options in the documented files.
func (p *Protocol) HasContent(b *core.Builder) bool {
	for _, f := range b.Files {
		if documentOptions(f) != nil {
			return true
		}
		found := false
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if standalone(messageOptions(m)) {
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
	p.topo = newTopology()
	p.chans = map[*core.Channel]*channelData{}
	p.order = nil
	for _, f := range b.Imports() {
		if d := documentOptions(f); d != nil {
			p.settings = append(p.settings, &fileSettings{file: f, doc: d})
		}
	}
	if err := p.serverDetails(); err != nil {
		return err
	}
	if err := p.collect(); err != nil {
		return err
	}
	// Declared queues are documented even when no rpc consumes them.
	for _, set := range p.settings {
		if !b.OwnFile(set.file) {
			continue
		}
		for _, q := range set.doc.GetQueues() {
			if _, err := p.queueChannel(set.file.Desc, q.GetName()); err != nil {
				return err
			}
		}
	}
	return nil
}

// serverDetails adds the x-amqp server bindings and the SASL mechanism of
// security schemes.
func (p *Protocol) serverDetails() error {
	b := p.b
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, srv := range set.doc.GetServers() {
			out, ok := b.Doc.Servers.Get(srv.GetName())
			if !ok {
				return fmt.Errorf("%s: amqp server %q is not declared in the servers of the asyncapi.v3.document file option", where, srv.GetName())
			}
			heartbeat, err := duration("heartbeat", srv.GetHeartbeat())
			if err != nil {
				return fmt.Errorf("%s: amqp server %q: %v", where, srv.GetName(), err)
			}
			x := asyncapi.NewMap[any]()
			if srv.GetVhost() != "" {
				x.Set("vhost", srv.GetVhost())
			}
			if srv.GetTls() {
				x.Set("tls", true)
			}
			if srv.GetHeartbeat() != "" {
				x.Set("heartbeat", heartbeat/1000)
			}
			if srv.GetFrameMax() != 0 {
				x.Set("frameMax", srv.GetFrameMax())
			}
			if srv.GetChannelMax() != 0 {
				x.Set("channelMax", srv.GetChannelMax())
			}
			x.Set("bindingVersion", BindingVersion)
			if out.Bindings == nil {
				out.Bindings = &asyncapi.Bindings{}
			}
			out.Bindings.Set(ServerBindingKey, x)
		}
		for _, a := range set.doc.GetAuth() {
			var sc *asyncapi.SecurityScheme
			if c := b.Doc.Components; c != nil {
				sc, _ = c.SecuritySchemes.Get(a.GetScheme())
			}
			if sc == nil {
				return fmt.Errorf("%s: amqp auth for %q: the security scheme is not declared in the asyncapi.v3.document file option", where, a.GetScheme())
			}
			typ := asyncapi.SecurityUserPassword
			switch a.GetMechanism() {
			case amqpv1.Mechanism_MECHANISM_PLAIN, amqpv1.Mechanism_MECHANISM_AMQPLAIN:
			case amqpv1.Mechanism_MECHANISM_EXTERNAL:
				typ = asyncapi.SecurityX509
			default:
				return fmt.Errorf("%s: amqp auth for %q: mechanism is required", where, a.GetScheme())
			}
			if sc.Type == "" {
				sc.Type = typ
			}
			if sc.Extensions == nil {
				sc.Extensions = asyncapi.Extensions{}
			}
			sc.Extensions["x-amqp-mechanism"] = strings.TrimPrefix(a.GetMechanism().String(), "MECHANISM_")
		}
	}
	return nil
}

var paramRE = regexp.MustCompile(`\{([^{}]*)\}`)

// parseKey validates a routing key template and returns its parameters.
func parseKey(key string) ([]string, error) {
	if len(key) > 255 {
		return nil, fmt.Errorf("routing key %q is longer than 255 bytes", key)
	}
	if strings.ContainsAny(key, " \t\r\n") {
		return nil, fmt.Errorf("routing key %q must not contain whitespace", key)
	}
	var params []string
	seen := map[string]bool{}
	for _, m := range paramRE.FindAllStringSubmatch(key, -1) {
		name := m[1]
		if !paramNameRE.MatchString(name) {
			return nil, fmt.Errorf("routing key %q: invalid parameter name %q (allowed: letters, digits, '_' and '-')", key, name)
		}
		if seen[name] {
			return nil, fmt.Errorf("routing key %q: parameter %q is used twice", key, name)
		}
		seen[name] = true
		params = append(params, name)
	}
	if strings.ContainsAny(paramRE.ReplaceAllString(key, ""), "{}") {
		return nil, fmt.Errorf("routing key %q has unbalanced braces", key)
	}
	return params, nil
}

var paramNameRE = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)

// bindingKey is the binding key matching a routing key template: on topic
// exchanges parameters match any word.
func bindingKey(key string, topic bool) string {
	if topic {
		return paramRE.ReplaceAllString(key, "*")
	}
	return key
}

var unsafeIDRE = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func idPart(s string) string {
	r := strings.NewReplacer("{", "", "}", "", "*", "any", "#", "all")
	id := strings.Trim(unsafeIDRE.ReplaceAllString(r.Replace(s), "_"), "._")
	return id
}

// routingChannel returns the channel of a routing key on an exchange.
func (p *Protocol) routingChannel(d protoreflect.Descriptor, exchangeName, key string, spec core.ChannelSpec) (*core.Channel, error) {
	params, err := parseKey(key)
	if err != nil {
		return nil, core.Errorf(d, "%v", err)
	}
	x := p.topo.exchange(exchangeName)
	id := core.FirstNonEmpty(idPart(exchangeName), "default") + "." + core.FirstNonEmpty(idPart(key), "all")
	spec.DeclaredBy, spec.Address, spec.Params, spec.DefaultID = d, key, params, id
	spec.Key = "exchange\x00" + exchangeName + "\x00" + key
	ch, err := p.b.Channel(spec)
	if err != nil {
		return nil, err
	}
	if p.chans[ch] == nil {
		p.chans[ch] = &channelData{exchange: x, params: params}
		p.order = append(p.order, ch)
	}
	return ch, nil
}

// queueChannel returns the channel of a queue.
func (p *Protocol) queueChannel(d protoreflect.Descriptor, name string) (*core.Channel, error) {
	if name != directReplyTo {
		if err := checkName("queue", name); err != nil {
			return nil, core.Errorf(d, "%v", err)
		}
	}
	q := p.topo.queue(name)
	spec := core.ChannelSpec{
		DeclaredBy: d, Address: name, DefaultID: "queue." + idPart(name), Key: "queue\x00" + name,
	}
	if q.decl != nil && q.decl.GetDescription() != "" {
		spec.Meta = []*asyncapiv3.Channel{{Description: q.decl.GetDescription()}}
	}
	ch, err := p.b.Channel(spec)
	if err != nil {
		return nil, err
	}
	if p.chans[ch] == nil {
		p.chans[ch] = &channelData{queue: q}
		p.order = append(p.order, ch)
	}
	return ch, nil
}

func (p *Protocol) Finish(b *core.Builder) error {
	// Messages with their own channel get one even without operation.
	for _, f := range b.Files {
		var list []*protogen.Message
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if standalone(messageOptions(m)) {
				list = append(list, m)
			}
		})
		for _, m := range list {
			o := messageOptions(m)
			if o.GetExchange() != "" && !p.topo.known(o.GetExchange()) {
				return core.Errorf(m.Desc, "exchange %q is not declared", o.GetExchange())
			}
			meta := core.MessageOptions(m).GetChannel()
			ch, err := p.routingChannel(m.Desc, o.GetExchange(), o.GetRoutingKey(), core.ChannelSpec{Meta: metas(meta), Servers: meta.GetServers()})
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
	for _, ch := range p.order {
		p.emitChannel(ch)
		for _, m := range ch.Messages {
			p.emitMessage(m)
		}
	}
	return nil
}

// emitChannel renders the official channel binding, with RabbitMQ details
// under x-rabbitmq.
func (p *Protocol) emitChannel(ch *core.Channel) {
	cd := p.chans[ch]
	bd := asyncapi.NewMap[any]()
	x := asyncapi.NewMap[any]()
	if cd.queue != nil {
		q := cd.queue
		bd.Set("is", "queue")
		qo := asyncapi.NewMap[any]()
		qo.Set("name", q.name)
		if d := q.decl; d != nil {
			typ := d.GetType()
			qo.Set("durable", d.GetDurable() || typ == amqpv1.QueueType_QUEUE_TYPE_QUORUM || typ == amqpv1.QueueType_QUEUE_TYPE_STREAM)
			qo.Set("exclusive", d.GetExclusive())
			qo.Set("autoDelete", d.GetAutoDelete())
			if d.GetVhost() != "" {
				qo.Set("vhost", d.GetVhost())
			}
			x.Set("queueType", queueTypeName(typ))
			if args := queueArguments(d); args.Len() > 0 {
				x.Set("arguments", args)
			}
		}
		bd.Set("queue", qo)
		if len(q.bindings) > 0 {
			var list []any
			for _, b := range q.bindings {
				list = append(list, renderBinding(b))
			}
			x.Set("bindings", list)
		}
	} else {
		xo := asyncapi.NewMap[any]()
		ex := cd.exchange
		bd.Set("is", "routingKey")
		xo.Set("name", ex.name)
		switch {
		case ex.name == "":
			xo.Set("type", "default")
		case ex.typeKnow:
			xo.Set("type", strings.ToLower(strings.TrimPrefix(ex.typ.String(), "EXCHANGE_TYPE_")))
		}
		if d := ex.decl; d != nil {
			xo.Set("durable", d.GetDurable())
			xo.Set("autoDelete", d.GetAutoDelete())
			if d.GetVhost() != "" {
				xo.Set("vhost", d.GetVhost())
			}
			if d.GetInternal() {
				x.Set("internal", true)
			}
			if d.GetAlternateExchange() != "" {
				x.Set("alternateExchange", d.GetAlternateExchange())
			}
			if d.GetDelayed() {
				x.Set("delayed", true)
			}
			if args := jsonArguments(d.GetArguments()); args.Len() > 0 {
				x.Set("arguments", args)
			}
		}
		bd.Set("exchange", xo)
		var bound []any
		for _, xb := range p.topo.xbindings {
			if xb.GetSource() == ex.name {
				m := asyncapi.NewMap[any]()
				m.Set("destination", xb.GetDestination())
				if xb.GetRoutingKey() != "" {
					m.Set("routingKey", xb.GetRoutingKey())
				}
				bound = append(bound, m)
			}
		}
		if len(bound) > 0 {
			x.Set("exchangeBindings", bound)
		}
	}
	bd.Set("bindingVersion", BindingVersion)
	if x.Len() > 0 {
		bd.Set(ExtensionKey, x)
	}
	if ch.Obj.Bindings == nil {
		ch.Obj.Bindings = &asyncapi.Bindings{}
	}
	ch.Obj.Bindings.Set("amqp", bd)
}

// emitMessage renders the official message binding of a Protobuf message.
func (p *Protocol) emitMessage(m *core.Message) {
	if m.Proto == nil {
		return
	}
	if _, done := m.Obj.Bindings.Get("amqp"); done {
		return
	}
	o := messageOptions(m.Proto)
	bd := asyncapi.NewMap[any]()
	if o.GetContentEncoding() != "" {
		bd.Set("contentEncoding", o.GetContentEncoding())
	}
	bd.Set("messageType", core.FirstNonEmpty(o.GetType(), string(m.Proto.Desc.FullName())))
	bd.Set("bindingVersion", BindingVersion)
	if m.Obj.Bindings == nil {
		m.Obj.Bindings = &asyncapi.Bindings{}
	}
	m.Obj.Bindings.Set("amqp", bd)
}

func renderBinding(b *amqpv1.Binding) *asyncapi.Map[any] {
	m := asyncapi.NewMap[any]()
	m.Set("exchange", b.GetExchange())
	if b.GetRoutingKey() != "" {
		m.Set("routingKey", b.GetRoutingKey())
	}
	if b.GetMatch() != amqpv1.Match_MATCH_UNSPECIFIED || len(b.GetHeaders()) > 0 {
		args := jsonArguments(b.GetHeaders())
		match := "all"
		if b.GetMatch() == amqpv1.Match_MATCH_ANY {
			match = "any"
		}
		out := asyncapi.NewMap[any]()
		out.Set("x-match", match)
		for _, k := range args.Keys() {
			v, _ := args.Get(k)
			out.Set(k, v)
		}
		m.Set("arguments", out)
	}
	return m
}

// jsonArguments decodes declaration arguments, sorted by name.
func jsonArguments(in map[string]string) *asyncapi.Map[any] {
	out := asyncapi.NewMap[any]()
	for _, k := range core.SortedKeys(in) {
		v, _ := asyncapi.DecodeJSON([]byte(in[k])) // validated by collect
		out.Set(k, v)
	}
	return out
}

// queueArguments renders the x- arguments of a queue declaration.
func queueArguments(q *amqpv1.Queue) *asyncapi.Map[any] {
	out := asyncapi.NewMap[any]()
	if t := q.GetType(); t != amqpv1.QueueType_QUEUE_TYPE_UNSPECIFIED {
		out.Set("x-queue-type", queueTypeName(t))
	}
	ms := func(key, value string) {
		if value != "" {
			v, _ := duration(key, value) // validated by collect
			out.Set(key, v)
		}
	}
	ms("x-message-ttl", q.GetMessageTtl())
	ms("x-expires", q.GetExpires())
	if q.GetMaxLength() > 0 {
		out.Set("x-max-length", q.GetMaxLength())
	}
	if q.GetMaxLengthBytes() > 0 {
		out.Set("x-max-length-bytes", q.GetMaxLengthBytes())
	}
	if o := q.GetOverflow(); o != amqpv1.Overflow_OVERFLOW_UNSPECIFIED {
		out.Set("x-overflow", strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(o.String(), "OVERFLOW_")), "_", "-"))
	}
	if q.GetDeadLetterExchange() != "" {
		out.Set("x-dead-letter-exchange", q.GetDeadLetterExchange())
	}
	if q.GetDeadLetterRoutingKey() != "" {
		out.Set("x-dead-letter-routing-key", q.GetDeadLetterRoutingKey())
	}
	if q.GetMaxPriority() > 0 {
		out.Set("x-max-priority", q.GetMaxPriority())
	}
	if q.GetSingleActiveConsumer() {
		out.Set("x-single-active-consumer", true)
	}
	if q.GetDeliveryLimit() > 0 {
		out.Set("x-delivery-limit", q.GetDeliveryLimit())
	}
	if q.GetMaxAge() != "" {
		// RabbitMQ takes stream retention as a string such as "7D" or "24h";
		// whole seconds are always accepted.
		v, _ := duration("max_age", q.GetMaxAge())
		out.Set("x-max-age", fmt.Sprintf("%ds", v/1000))
	}
	args := jsonArguments(q.GetArguments())
	for _, k := range args.Keys() {
		v, _ := args.Get(k)
		out.Set(k, v)
	}
	return out
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
