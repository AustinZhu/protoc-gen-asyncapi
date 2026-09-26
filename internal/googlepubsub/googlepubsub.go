// Package googlepubsub documents Google Cloud Pub/Sub APIs described by
// Protobuf services: topics, subscriptions (pull, push, BigQuery and Cloud
// Storage), publishers and subscribers.
package googlepubsub

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
	pubsubv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/googlepubsub/asyncapi/v1"
)

// BindingKey is the key of the official Google Pub/Sub bindings.
const BindingKey = "googlepubsub"

// BindingVersion is the version of the official bindings.
const BindingVersion = "0.2.0"

// ExtensionKey is the key of the x- bindings carrying what the official
// bindings do not define (or reserve, for servers and operations).
const ExtensionKey = "x-googlepubsub"

const emptyFullName = "google.protobuf.Empty"

// Protocol implements core.Protocol for Google Cloud Pub/Sub.
type Protocol struct {
	// Per document state, reset by Begin.
	b        *core.Builder
	settings []*fileSettings
	project  string
	topics   map[string]*topic
	subs     map[string]*subscription
	chans    map[*core.Channel]*topic
	order    []*core.Channel
}

type fileSettings struct {
	file *protogen.File
	doc  *pubsubv1.Document
}

// New returns the Google Pub/Sub protocol.
func New() core.Protocol { return &Protocol{} }

func (p *Protocol) Name() string { return "googlepubsub" }

func (p *Protocol) Options() []core.Option { return nil }

func documentOptions(f *protogen.File) *pubsubv1.Document {
	return core.Extension[*pubsubv1.Document](f.Desc.Options(), pubsubv1.E_Document)
}

func serviceOptions(s *protogen.Service) *pubsubv1.Service {
	return core.Extension[*pubsubv1.Service](s.Desc.Options(), pubsubv1.E_Service)
}

func operationOptions(m *protogen.Method) *pubsubv1.Operation {
	return core.Extension[*pubsubv1.Operation](m.Desc.Options(), pubsubv1.E_Operation)
}

func messageOptions(m *protogen.Message) *pubsubv1.Message {
	if m == nil {
		return nil
	}
	return core.Extension[*pubsubv1.Message](m.Desc.Options(), pubsubv1.E_Message)
}

// Claims documents services with Pub/Sub annotations.
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

// HasContent reports messages with their own topic, or Pub/Sub document
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
	p.project = ""
	p.topics = map[string]*topic{}
	p.subs = map[string]*subscription{}
	p.chans = map[*core.Channel]*topic{}
	p.order = nil
	for _, f := range b.Imports() {
		d := documentOptions(f)
		if d == nil {
			continue
		}
		p.settings = append(p.settings, &fileSettings{file: f, doc: d})
		if d.GetProject() != "" {
			if p.project != "" && p.project != d.GetProject() {
				return fmt.Errorf("%s: project %q differs from project %q of another file of the document; set the project of topics and subscriptions instead", f.Desc.Path(), d.GetProject(), p.project)
			}
			p.project = d.GetProject()
		}
	}
	if err := p.serverDetails(); err != nil {
		return err
	}
	if err := p.collect(); err != nil {
		return err
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
		for _, s := range set.doc.GetSubscriptions() {
			if _, err := p.topicChannel(set.file.Desc, s.GetTopic(), core.ChannelSpec{}); err != nil {
				return err
			}
		}
	}
	return nil
}

// serverDetails adds the x-googlepubsub server bindings.
func (p *Protocol) serverDetails() error {
	for _, set := range p.settings {
		for _, srv := range set.doc.GetServers() {
			out, ok := p.b.Doc.Servers.Get(srv.GetName())
			if !ok {
				return fmt.Errorf("%s: googlepubsub server %q is not declared in the servers of the asyncapi.v3.document file option", set.file.Desc.Path(), srv.GetName())
			}
			x := asyncapi.NewMap[any]()
			if project := core.FirstNonEmpty(srv.GetProject(), p.project); project != "" {
				x.Set("project", project)
			}
			if srv.GetEmulator() {
				x.Set("emulator", true)
			}
			if out.Bindings == nil {
				out.Bindings = &asyncapi.Bindings{}
			}
			out.Bindings.Set(ExtensionKey, x)
		}
	}
	return nil
}

var idPartReplacer = strings.NewReplacer("{", "", "}", "")

// topicChannel returns the channel of a topic.
func (p *Protocol) topicChannel(d protoreflect.Descriptor, name string, spec core.ChannelSpec) (*core.Channel, error) {
	params, err := parseID("topic", name)
	if err != nil {
		return nil, core.Errorf(d, "%v", err)
	}
	t := p.topic(name)
	if t.decl.GetDescription() != "" {
		spec.Meta = append(spec.Meta, &asyncapiv3.Channel{Description: t.decl.GetDescription()})
	}
	spec.DeclaredBy, spec.Address, spec.Params = d, name, params
	spec.DefaultID = strings.Trim(unsafeIDRE.ReplaceAllString(idPartReplacer.Replace(name), "_"), "._")
	spec.Key = "googlepubsub\x00" + name
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
			p.emitMessage(m, p.chans[ch])
		}
	}
	return nil
}

// emitChannel renders the topic bindings: the official binding when the
// topic has a schema (the binding requires one), and x-googlepubsub for
// the rest.
func (p *Protocol) emitChannel(ch *core.Channel) {
	t := p.chans[ch]
	d := t.decl
	project := core.FirstNonEmpty(d.GetProject(), p.project)
	official := d.GetSchema() != nil
	topicProps := asyncapi.NewMap[any]()
	if len(d.GetLabels()) > 0 {
		topicProps.Set("labels", labels(d.GetLabels()))
	}
	if r := d.GetMessageRetentionDuration(); r != "" {
		v, _ := duration("", r) // validated by collect
		topicProps.Set("messageRetentionDuration", seconds(v))
	}
	if regions := d.GetAllowedPersistenceRegions(); len(regions) > 0 {
		sp := asyncapi.NewMap[any]()
		sp.Set("allowedPersistenceRegions", regions)
		topicProps.Set("messageStoragePolicy", sp)
	}
	if ch.Obj.Bindings == nil {
		ch.Obj.Bindings = &asyncapi.Bindings{}
	}
	if official {
		bd := asyncapi.NewMap[any]()
		for _, k := range topicProps.Keys() {
			v, _ := topicProps.Get(k)
			bd.Set(k, v)
		}
		s := d.GetSchema()
		ss := asyncapi.NewMap[any]()
		encoding := "json"
		if s.GetEncoding() == pubsubv1.Encoding_ENCODING_BINARY {
			encoding = "binary"
		}
		ss.Set("encoding", encoding)
		if s.GetFirstRevisionId() != "" {
			ss.Set("firstRevisionId", s.GetFirstRevisionId())
		}
		if s.GetLastRevisionId() != "" {
			ss.Set("lastRevisionId", s.GetLastRevisionId())
		}
		ss.Set("name", fullName(project, "schemas", s.GetName()))
		bd.Set("schemaSettings", ss)
		bd.Set("bindingVersion", BindingVersion)
		ch.Obj.Bindings.Set(BindingKey, bd)
	}

	x := asyncapi.NewMap[any]()
	x.Set("topic", fullName(project, "topics", t.name))
	if !official {
		for _, k := range topicProps.Keys() {
			v, _ := topicProps.Get(k)
			x.Set(k, v)
		}
	}
	if d.GetEnforceInTransit() {
		x.Set("enforceInTransit", true)
	}
	if d.GetKmsKeyName() != "" {
		x.Set("kmsKeyName", d.GetKmsKeyName())
	}
	if len(t.subs) > 0 {
		var subs []any
		for _, s := range t.subs {
			subs = append(subs, p.renderSubscription(s))
		}
		x.Set("subscriptions", subs)
	}
	ch.Obj.Bindings.Set(ExtensionKey, x)
}

// emitMessage renders the official message binding when the message has
// Pub/Sub properties.
func (p *Protocol) emitMessage(m *core.Message, t *topic) {
	if m.Proto == nil {
		return
	}
	if _, done := m.Obj.Bindings.Get(BindingKey); done {
		return
	}
	o := messageOptions(m.Proto)
	schema := o.GetSchema()
	if schema == "" && t.decl.GetSchema() != nil {
		schema = t.decl.GetSchema().GetName()
	}
	if len(o.GetAttributes()) == 0 && o.GetOrderingKey() == "" && schema == "" {
		return
	}
	bd := asyncapi.NewMap[any]()
	if len(o.GetAttributes()) > 0 {
		bd.Set("attributes", labels(o.GetAttributes()))
	}
	if o.GetOrderingKey() != "" {
		bd.Set("orderingKey", o.GetOrderingKey())
	}
	if schema != "" {
		s := asyncapi.NewMap[any]()
		s.Set("name", fullName(core.FirstNonEmpty(t.decl.GetProject(), p.project), "schemas", schema))
		bd.Set("schema", s)
	}
	bd.Set("bindingVersion", BindingVersion)
	if m.Obj.Bindings == nil {
		m.Obj.Bindings = &asyncapi.Bindings{}
	}
	m.Obj.Bindings.Set(BindingKey, bd)
	for _, k := range core.SortedKeys(o.GetAttributes()) {
		m.AddHeader(k, &asyncapi.Schema{Type: "string", Const: o.GetAttributes()[k]}, true)
	}
}

// ---------------------------------------------------------------------------
// Services and operations
// ---------------------------------------------------------------------------

func (p *Protocol) AddService(b *core.Builder, s *protogen.Service) error {
	for _, name := range core.ServiceOptions(s).GetServers() {
		if _, ok := b.Servers[name]; !ok {
			return core.Errorf(s.Desc, "unknown server %q", name)
		}
	}
	opt := serviceOptions(s)
	for _, m := range s.Methods {
		if core.OperationOptions(m).GetSkip() {
			continue
		}
		if err := p.addMethod(s, opt, m); err != nil {
			return err
		}
	}
	return nil
}

// resolvePattern returns the pattern of an rpc: the annotated one, or the
// one its shape implies.
func resolvePattern(m *protogen.Method, pat pubsubv1.Pattern) (pubsubv1.Pattern, core.Shape, error) {
	shapes := map[pubsubv1.Pattern]core.Shape{
		pubsubv1.Pattern_PATTERN_REQUEST_REPLY: core.ShapeRequestReply,
		pubsubv1.Pattern_PATTERN_PUBLISH:       core.ShapePublish,
		pubsubv1.Pattern_PATTERN_SUBSCRIBE:     core.ShapeSubscribe,
		pubsubv1.Pattern_PATTERN_PROCESS:       core.ShapeProcess,
	}
	if pat != pubsubv1.Pattern_PATTERN_UNSPECIFIED {
		return pat, shapes[pat], nil
	}
	shape, err := core.InferShape(m, "(googlepubsub.asyncapi.v1.operation)")
	if err != nil {
		return pat, 0, err
	}
	for p, s := range shapes {
		if s == shape {
			pat = p
		}
	}
	return pat, shape, nil
}

func joinID(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.Trim(p, "."); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, ".")
}

func (p *Protocol) addMethod(s *protogen.Service, svcOpt *pubsubv1.Service, m *protogen.Method) error {
	b := p.b
	fail := func(format string, args ...any) error { return core.Errorf(m.Desc, format, args...) }
	opt := operationOptions(m)
	pattern, shape, err := resolvePattern(m, opt.GetPattern())
	if err != nil {
		return err
	}
	if shape == core.ShapeProcess {
		if err := core.CheckProcess(m); err != nil {
			return err
		}
	} else if opt.GetOutput() != "" {
		return core.Errorf(m.Desc, "output is only valid for PROCESS operations")
	}
	payload, response := core.Payloads(m, shape)
	if pattern == pubsubv1.Pattern_PATTERN_REQUEST_REPLY {
		if opt.GetReplyTopic() == "" {
			return fail("Pub/Sub has no built-in replies: REQUEST_REPLY operations must set reply_topic")
		}
	}
	msgOpt := messageOptions(payload)

	var name string
	switch {
	case opt.GetTopic() != "":
		name = joinID(svcOpt.GetTopicPrefix(), opt.GetTopic())
	case msgOpt.GetTopic() != "":
		name = msgOpt.GetTopic()
	default:
		prefix := svcOpt.GetTopicPrefix()
		if prefix == "" {
			prefix = joinID(string(s.Desc.ParentFile().Package()), core.SnakeCase(string(s.Desc.Name())))
		}
		name = joinID(prefix, core.SnakeCase(string(m.Desc.Name())))
	}

	serviceReceives := pattern != pubsubv1.Pattern_PATTERN_PUBLISH
	action := asyncapi.ActionSend
	if serviceReceives == (b.Params.Perspective == "server") {
		action = asyncapi.ActionReceive
	}

	ch, err := p.topicChannel(m.Desc, name, core.ChannelSpec{
		Meta:    metas(core.OperationOptions(m).GetChannel(), core.MessageOptions(payload).GetChannel()),
		Servers: core.ChannelServers(s, m),
	})
	if err != nil {
		return err
	}
	msg, err := b.Message(payload)
	if err != nil {
		return err
	}
	ch.AddMessage(msg)

	// The subscription of the consuming side: the service's default only
	// applies to the service's own consumers.
	subName := opt.GetSubscription()
	if subName == "" && serviceReceives {
		subName = svcOpt.GetSubscription()
	}
	var sub *subscription
	if subName != "" {
		if sub, err = p.subscription(subName, name); err != nil {
			return fail("%v", err)
		}
	}

	inOpt := opt
	if shape == core.ShapeProcess {
		inOpt = proto.Clone(opt).(*pubsubv1.Operation)
		inOpt.Publish = nil
	}
	x, err := p.operationBinding(inOpt, action, sub, msg)
	if err != nil {
		return fail("%v", err)
	}
	var bindings *asyncapi.Bindings
	if x.Len() > 0 {
		bindings = asyncapi.NewBindings(ExtensionKey, x)
	}
	op, err := b.AddOperation(core.OpSpec{
		Service:   s,
		Method:    m,
		DefaultID: string(s.Desc.Name()) + "." + string(m.Desc.Name()),
		Action:    action,
		Channel:   ch,
		Payload:   msg,
		Bindings:  bindings,
	})
	if err != nil {
		return err
	}

	if shape == core.ShapeProcess {
		if err := p.addOutput(s, svcOpt, m, opt, name, action, response); err != nil {
			return err
		}
	}
	if pattern != pubsubv1.Pattern_PATTERN_REQUEST_REPLY {
		if opt.GetReplyTopic() != "" || len(opt.GetReplyMessages()) > 0 {
			return fail("reply_topic and reply_messages are only valid for REQUEST_REPLY operations")
		}
		return nil
	}
	reply, err := p.topicChannel(m.Desc, joinID(svcOpt.GetTopicPrefix(), opt.GetReplyTopic()), core.ChannelSpec{})
	if err != nil {
		return err
	}
	resp, err := b.Message(response)
	if err != nil {
		return err
	}
	b.SetReply(op, reply, resp)
	for _, n := range opt.GetReplyMessages() {
		n = strings.TrimPrefix(n, ".")
		pm := b.FindMessage(protoreflect.FullName(n))
		if pm == nil {
			return fail("reply message %q not found (use the full Protobuf name, e.g. \"acme.v1.Error\")", n)
		}
		mi, err := b.Message(pm)
		if err != nil {
			return err
		}
		b.AddReplyMessage(op, reply, mi)
	}
	for _, mi := range append([]*core.Message{msg}, reply.Messages...) {
		mi.AddHeader("correlation_id", &asyncapi.Schema{Type: "string", Description: "Attribute correlating a reply with its request."}, true)
		if mi.Obj.CorrelationID == nil {
			mi.Obj.CorrelationID = &asyncapi.CorrelationID{
				Location:    "$message.header#/correlation_id",
				Description: "The correlation_id attribute, copied from the request to its reply.",
			}
		}
	}
	return nil
}

// addOutput adds the operation publishing a processor's output: the
// opposite action of its input, on the output topic, with the publish
// settings.
func (p *Protocol) addOutput(s *protogen.Service, svcOpt *pubsubv1.Service, m *protogen.Method, opt *pubsubv1.Operation, input string, inAction asyncapi.Action, output *protogen.Message) error {
	name := joinID(input, "output")
	if opt.GetOutput() != "" {
		name = joinID(svcOpt.GetTopicPrefix(), opt.GetOutput())
	}
	ch, err := p.topicChannel(m.Desc, name, core.ChannelSpec{Servers: core.ChannelServers(s, m)})
	if err != nil {
		return err
	}
	msg, err := p.b.Message(output)
	if err != nil {
		return err
	}
	ch.AddMessage(msg)
	action := asyncapi.ActionReceive
	if inAction == asyncapi.ActionReceive {
		action = asyncapi.ActionSend
	}
	outOpt := proto.Clone(opt).(*pubsubv1.Operation)
	outOpt.Consume, outOpt.Subscription = nil, ""
	x, err := p.operationBinding(outOpt, action, nil, msg)
	if err != nil {
		return core.Errorf(m.Desc, "output: %v", err)
	}
	var bindings *asyncapi.Bindings
	if x.Len() > 0 {
		bindings = asyncapi.NewBindings(ExtensionKey, x)
	}
	_, err = p.b.AddOperation(core.OpSpec{
		Service:   s,
		Method:    m,
		DefaultID: string(s.Desc.Name()) + "." + string(m.Desc.Name()),
		IDSuffix:  ".output",
		Action:    action,
		Channel:   ch,
		Payload:   msg,
		Bindings:  bindings,
	})
	return err
}

// subscription returns the subscription consumers of a topic read from,
// registering undeclared ones.
func (p *Protocol) subscription(name, topicName string) (*subscription, error) {
	if _, err := parseID("subscription", name); err != nil {
		return nil, err
	}
	s, ok := p.subs[name]
	if !ok {
		s = &subscription{name: name, topic: topicName}
		p.subs[name] = s
		t := p.topic(topicName)
		t.subs = append(t.subs, s)
		return s, nil
	}
	if s.topic != topicName {
		return nil, fmt.Errorf("subscription %q is attached to topic %q, not %q", name, s.topic, topicName)
	}
	if s.export() {
		return nil, fmt.Errorf("subscription %q exports to %s and has no subscribers", name, s.delivery())
	}
	return s, nil
}

// operationBinding validates the publisher and subscriber settings of an rpc
// and renders the x-googlepubsub operation binding of the documented side.
func (p *Protocol) operationBinding(opt *pubsubv1.Operation, action asyncapi.Action, sub *subscription, msg *core.Message) (*asyncapi.Map[any], error) {
	pub, con := opt.GetPublish(), opt.GetConsume()
	if key := pub.GetOrderingKey(); key != "" {
		if strings.ContainsAny(paramRE.ReplaceAllString(key, ""), "{}") {
			return nil, fmt.Errorf("publish.ordering_key %q has unbalanced braces", key)
		}
	}
	for _, k := range core.SortedKeys(pub.GetAttributes()) {
		if strings.HasPrefix(strings.ToLower(k), "goog") || len(k) > 256 || k == "" {
			return nil, fmt.Errorf("publish attribute %q: names are 1 to 256 bytes and must not start with \"goog\"", k)
		}
	}
	if len(pub.GetAttributes()) > 100 {
		return nil, fmt.Errorf("messages have at most 100 attributes")
	}
	if pub.GetBatchMaxMessages() < 0 || pub.GetBatchMaxBytes() < 0 || con.GetMaxOutstandingMessages() < 0 || con.GetMaxOutstandingBytes() < 0 {
		return nil, fmt.Errorf("batching and flow control limits must be >= 0")
	}
	latency, err := duration("publish.batch_max_latency", pub.GetBatchMaxLatency())
	if err != nil {
		return nil, err
	}
	extension, err := duration("consume.max_extension", con.GetMaxExtension())
	if err != nil {
		return nil, err
	}
	if con != nil && sub != nil && sub.delivery() == "push" {
		return nil, fmt.Errorf("consume settings do not apply to push subscription %q", sub.name)
	}

	// Attributes publishers set describe the messages on both sides.
	for _, k := range core.SortedKeys(pub.GetAttributes()) {
		msg.AddHeader(k, &asyncapi.Schema{Type: "string", Const: pub.GetAttributes()[k]}, true)
	}

	x := asyncapi.NewMap[any]()
	if pub.GetOrderingKey() != "" {
		x.Set("orderingKey", pub.GetOrderingKey())
	}
	if action == asyncapi.ActionSend {
		if len(pub.GetAttributes()) > 0 {
			x.Set("attributes", labels(pub.GetAttributes()))
		}
		batch := asyncapi.NewMap[any]()
		if pub.GetBatchMaxMessages() > 0 {
			batch.Set("maxMessages", pub.GetBatchMaxMessages())
		}
		if pub.GetBatchMaxBytes() > 0 {
			batch.Set("maxBytes", pub.GetBatchMaxBytes())
		}
		if pub.GetBatchMaxLatency() != "" {
			batch.Set("maxLatency", fmt.Sprintf("%gs", latency.Seconds()))
		}
		if batch.Len() > 0 {
			x.Set("batching", batch)
		}
		if pub.GetCompression() {
			x.Set("compression", true)
		}
		return x, nil
	}
	if sub != nil {
		d := sub.decl
		x.Set("subscription", fullName(core.FirstNonEmpty(d.GetProject(), p.project), "subscriptions", sub.name))
		x.Set("delivery", sub.delivery())
		if d.GetEnableMessageOrdering() {
			x.Set("ordered", true)
		}
		if d.GetEnableExactlyOnceDelivery() {
			x.Set("exactlyOnce", true)
		}
	}
	flow := asyncapi.NewMap[any]()
	if con.GetMaxOutstandingMessages() > 0 {
		flow.Set("maxOutstandingMessages", con.GetMaxOutstandingMessages())
	}
	if con.GetMaxOutstandingBytes() > 0 {
		flow.Set("maxOutstandingBytes", con.GetMaxOutstandingBytes())
	}
	if flow.Len() > 0 {
		x.Set("flowControl", flow)
	}
	if con.GetMaxExtension() != "" {
		x.Set("maxExtension", seconds(extension.Round(time.Second)))
	}
	if con.GetSynchronousPull() {
		x.Set("synchronousPull", true)
	}
	return x, nil
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
