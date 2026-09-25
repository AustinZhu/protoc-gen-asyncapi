// Package nats documents NATS (core NATS, JetStream, key-value stores and
// NATS micro services) APIs described by Protobuf services.
package nats

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	natsv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/nats/asyncapi/v1"
)

const emptyFullName = "google.protobuf.Empty"

// NATSBindingVersion is the version of the NATS operation binding.
const NATSBindingVersion = "0.1.0"

// Protocol implements core.Protocol for NATS.
type Protocol struct {
	includeAll bool

	// Per document state, reset by Begin.
	b        *core.Builder
	settings []*fileSettings
	chans    map[*core.Channel]*channelData
	ops      map[*core.Op]*natsv1.Operation
	js       *jetStream
}

type fileSettings struct {
	file *protogen.File
	doc  *natsv1.Document
}

// channelData holds NATS specifics of a channel.
type channelData struct {
	subject  *subject // nil for dynamic (inbox) channels
	stream   string
	explicit bool // stream set explicitly (not matched)
	kvBucket string
}

// New returns the NATS protocol.
func New() core.Protocol { return &Protocol{} }

func (p *Protocol) Name() string { return "nats" }

func (p *Protocol) Options() []core.Option {
	return []core.Option{
		{Name: "include_all", Usage: "true|false: document NATS services without nats annotations", Set: core.BoolOption("include_all", &p.includeAll)},
	}
}

func documentOptions(f *protogen.File) *natsv1.Document {
	return core.Extension[*natsv1.Document](f.Desc.Options(), natsv1.E_Document)
}

func serviceOptions(s *protogen.Service) *natsv1.Service {
	return core.Extension[*natsv1.Service](s.Desc.Options(), natsv1.E_Service)
}

func operationOptions(m *protogen.Method) *natsv1.Operation {
	return core.Extension[*natsv1.Operation](m.Desc.Options(), natsv1.E_Operation)
}

func messageOptions(m *protogen.Message) *natsv1.Message {
	if m == nil {
		return nil
	}
	return core.Extension[*natsv1.Message](m.Desc.Options(), natsv1.E_Message)
}

// Claims documents services with NATS annotations, services of files with
// NATS or AsyncAPI document options, and every service with include_all.
func (p *Protocol) Claims(b *core.Builder, s *protogen.Service) bool {
	f := b.Plugin.FilesByPath[s.Desc.ParentFile().Path()]
	if p.includeAll || serviceOptions(s) != nil || documentOptions(f) != nil || core.DocumentOptions(f) != nil {
		return true
	}
	for _, m := range s.Methods {
		if operationOptions(m) != nil {
			return true
		}
	}
	return false
}

// HasContent reports messages with their own subject, or NATS document
// options in the documented files.
func (p *Protocol) HasContent(b *core.Builder) bool {
	for _, f := range b.Files {
		if documentOptions(f) != nil {
			return true
		}
		found := false
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if o := messageOptions(m); o.GetSubject() != "" || o.GetKvBucket() != "" {
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
	p.chans = map[*core.Channel]*channelData{}
	p.ops = map[*core.Op]*natsv1.Operation{}
	p.js = newJetStream(p)
	for _, f := range b.Imports() {
		if d := documentOptions(f); d != nil {
			p.settings = append(p.settings, &fileSettings{file: f, doc: d})
		}
	}
	if err := p.serverDetails(); err != nil {
		return err
	}
	return p.js.collect()
}

// serverDetails adds the x-nats server extension and the NATS
// authentication mechanism of security schemes.
func (p *Protocol) serverDetails() error {
	b := p.b
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, srv := range set.doc.GetServers() {
			out, ok := b.Doc.Servers.Get(srv.GetName())
			if !ok {
				return fmt.Errorf("%s: nats server %q is not declared in the servers of the asyncapi.v3.document file option", where, srv.GetName())
			}
			x := asyncapi.NewMap[any]()
			if srv.GetAccount() != "" {
				x.Set("account", srv.GetAccount())
			}
			if srv.GetJetstreamDomain() != "" {
				x.Set("jetstreamDomain", srv.GetJetstreamDomain())
			}
			if srv.GetJetstreamApiPrefix() != "" {
				x.Set("jetstreamApiPrefix", srv.GetJetstreamApiPrefix())
			}
			if srv.GetTls() {
				x.Set("tls", true)
			}
			if x.Len() > 0 {
				if out.Extensions == nil {
					out.Extensions = asyncapi.Extensions{}
				}
				out.Extensions["x-nats"] = x
			}
		}
		for _, a := range set.doc.GetAuth() {
			var sc *asyncapi.SecurityScheme
			if c := b.Doc.Components; c != nil {
				sc, _ = c.SecuritySchemes.Get(a.GetScheme())
			}
			if sc == nil {
				return fmt.Errorf("%s: nats auth for %q: the security scheme is not declared in the asyncapi.v3.document file option", where, a.GetScheme())
			}
			typ, kind, err := authType(a.GetType())
			if err != nil {
				return fmt.Errorf("%s: nats auth for %q: %w", where, a.GetScheme(), err)
			}
			if sc.Type == "" {
				sc.Type = typ
				if typ == asyncapi.SecurityAPIKey && sc.In == "" {
					sc.In = "user"
				}
			}
			if sc.Extensions == nil {
				sc.Extensions = asyncapi.Extensions{}
			}
			sc.Extensions["x-nats-auth"] = kind
		}
	}
	return nil
}

// authType maps a NATS authentication mechanism to the AsyncAPI security
// scheme type and the x-nats-auth value.
func authType(t natsv1.AuthType) (string, string, error) {
	switch t {
	case natsv1.AuthType_AUTH_TYPE_USER_PASSWORD:
		return asyncapi.SecurityUserPassword, "user_password", nil
	case natsv1.AuthType_AUTH_TYPE_TOKEN:
		return asyncapi.SecurityAPIKey, "token", nil
	case natsv1.AuthType_AUTH_TYPE_NKEY:
		return asyncapi.SecurityAsymmetricEncryption, "nkey", nil
	case natsv1.AuthType_AUTH_TYPE_JWT:
		return asyncapi.SecurityAsymmetricEncryption, "jwt", nil
	case natsv1.AuthType_AUTH_TYPE_TLS:
		return asyncapi.SecurityX509, "tls", nil
	case natsv1.AuthType_AUTH_TYPE_AUTH_CALLOUT:
		return asyncapi.SecurityUserPassword, "auth_callout", nil
	}
	return "", "", fmt.Errorf("type is required")
}

func (p *Protocol) Finish(b *core.Builder) error {
	// Messages with their own subject get a channel even without operation.
	for _, f := range b.Files {
		var standalone []*protogen.Message
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if o := messageOptions(m); o.GetSubject() != "" || o.GetKvBucket() != "" {
				standalone = append(standalone, m)
			}
		})
		for _, m := range standalone {
			if _, err := p.messageChannel(m); err != nil {
				return err
			}
		}
	}
	if err := p.js.bind(); err != nil {
		return err
	}
	p.js.emit()
	return nil
}

// ---------------------------------------------------------------------------
// Services and operations
// ---------------------------------------------------------------------------

func (p *Protocol) AddService(b *core.Builder, s *protogen.Service) error {
	opt := serviceOptions(s)
	if m := opt.GetMicro(); m != nil && !paramNameRE.MatchString(m.GetName()) {
		return core.Errorf(s.Desc, "micro.name %q is invalid (allowed: letters, digits, '_' and '-')", m.GetName())
	}
	for _, name := range core.ServiceOptions(s).GetServers() {
		if _, ok := b.Servers[name]; !ok {
			return core.Errorf(s.Desc, "unknown server %q", name)
		}
	}
	for _, m := range s.Methods {
		if core.OperationOptions(m).GetSkip() {
			continue
		}
		if err := p.addMethod(s, opt, m); err != nil {
			return err
		}
	}
	if m := opt.GetMicro(); m != nil && !m.GetNoDiscovery() {
		if err := p.addMicroDiscovery(s, m); err != nil {
			return err
		}
	}
	return nil
}

// resolvePattern applies the inference rules documented on
// nats.asyncapi.v1.Pattern.
func resolvePattern(m *protogen.Method, pat natsv1.Pattern) (natsv1.Pattern, error) {
	in, out := m.Desc.IsStreamingClient(), m.Desc.IsStreamingServer()
	if pat == natsv1.Pattern_PATTERN_UNSPECIFIED {
		switch {
		case in && out:
			return pat, core.Errorf(m.Desc, "cannot infer the NATS pattern of a bidirectional streaming method; set (nats.asyncapi.v1.operation).pattern")
		case out:
			return natsv1.Pattern_PATTERN_PUBLISH, nil
		case in:
			return natsv1.Pattern_PATTERN_SUBSCRIBE, nil
		case m.Output.Desc.FullName() == emptyFullName:
			return natsv1.Pattern_PATTERN_SUBSCRIBE, nil
		default:
			return natsv1.Pattern_PATTERN_REQUEST_REPLY, nil
		}
	}
	return pat, nil
}

func (p *Protocol) addMethod(s *protogen.Service, svcOpt *natsv1.Service, m *protogen.Method) error {
	b := p.b
	opt := operationOptions(m)
	pattern, err := resolvePattern(m, opt.GetPattern())
	if err != nil {
		return err
	}

	// Pick the payload.
	var payload, response *protogen.Message
	switch pattern {
	case natsv1.Pattern_PATTERN_REQUEST_REPLY:
		payload, response = m.Input, m.Output
	case natsv1.Pattern_PATTERN_SUBSCRIBE:
		payload = m.Input
	case natsv1.Pattern_PATTERN_PUBLISH:
		payload = m.Input
		if m.Desc.IsStreamingServer() || m.Input.Desc.FullName() == emptyFullName {
			payload = m.Output
		}
	}

	// The service is the receiver of requests and subscriptions and the
	// sender of publications; the client perspective flips this.
	serviceReceives := pattern != natsv1.Pattern_PATTERN_PUBLISH
	action := asyncapi.ActionSend
	if serviceReceives == (b.Params.Perspective == "server") {
		action = asyncapi.ActionReceive
	}

	ch, err := p.methodChannel(s, svcOpt, m, opt, payload)
	if err != nil {
		return err
	}
	msg, err := b.Message(payload)
	if err != nil {
		return err
	}
	ch.AddMessage(msg)

	// Queue groups only apply to the receiving side.
	var bindings *asyncapi.Bindings
	if action == asyncapi.ActionReceive {
		queue := opt.GetQueueGroup()
		if queue == "" && serviceReceives {
			queue = svcOpt.GetQueueGroup()
			if queue == "" && svcOpt.GetMicro() != nil {
				queue = "q"
			}
		}
		if queue != "" {
			if len(queue) > 255 || strings.ContainsAny(queue, " \t") {
				return core.Errorf(m.Desc, "invalid queue group %q", queue)
			}
			bindings = queueBinding(queue)
		}
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
	p.ops[op] = opt

	if pattern == natsv1.Pattern_PATTERN_REQUEST_REPLY {
		if err := p.addReply(op, ch, m, opt, svcOpt, response); err != nil {
			return err
		}
	} else if opt.GetReplySubject() != "" || len(opt.GetReplyMessages()) > 0 {
		return core.Errorf(m.Desc, "reply_subject and reply_messages are only valid for REQUEST_REPLY operations")
	}

	if mo := svcOpt.GetMicro(); mo != nil && pattern == natsv1.Pattern_PATTERN_REQUEST_REPLY {
		x := asyncapi.NewMap[any]()
		x.Set("service", mo.GetName())
		x.Set("endpoint", string(m.Desc.Name()))
		if mo.GetGroup() != "" {
			x.Set("group", mo.GetGroup())
		}
		op.Obj.Extensions["x-nats-micro"] = x
	}
	return nil
}

func queueBinding(queue string) *asyncapi.Bindings {
	nb := asyncapi.NewMap[any]()
	nb.Set("queue", queue)
	nb.Set("bindingVersion", NATSBindingVersion)
	return asyncapi.NewBindings("nats", nb)
}

// queueOf returns the NATS queue of an operation binding.
func queueOf(b *asyncapi.Bindings) string {
	v, ok := b.Get("nats")
	if !ok {
		return ""
	}
	m, _ := v.(*asyncapi.Map[any])
	q, _ := m.Get("queue")
	s, _ := q.(string)
	return s
}

// methodChannel resolves the channel (subject) of a method.
func (p *Protocol) methodChannel(s *protogen.Service, svcOpt *natsv1.Service, m *protogen.Method, opt *natsv1.Operation, payload *protogen.Message) (*core.Channel, error) {
	msgOpt := messageOptions(payload)
	prefix := svcOpt.GetSubjectPrefix()
	if prefix == "" && svcOpt.GetMicro() != nil {
		prefix = svcOpt.GetMicro().GetGroup()
	}

	bucket := core.FirstNonEmpty(opt.GetKvBucket(), msgOpt.GetKvBucket())
	var address string
	switch {
	case bucket != "":
		key := opt.GetSubject()
		if key == "" && msgOpt.GetKvBucket() == bucket {
			key = msgOpt.GetSubject()
		}
		address = kvSubject(bucket, key)
	case opt.GetSubject() != "":
		address = joinSubject(prefix, opt.GetSubject())
	case msgOpt.GetSubject() != "":
		address = msgOpt.GetSubject()
	default:
		if prefix == "" {
			prefix = joinSubject(string(s.Desc.ParentFile().Package()), core.SnakeCase(string(s.Desc.Name())))
		}
		address = joinSubject(prefix, core.SnakeCase(string(m.Desc.Name())))
	}

	ch, err := p.channel(m.Desc, address, bucket, core.ChannelSpec{
		Meta:    metas(core.OperationOptions(m).GetChannel(), core.MessageOptions(payload).GetChannel()),
		Servers: core.ChannelServers(s, m),
	})
	if err != nil {
		return nil, err
	}
	cd := p.chans[ch]
	stream := core.FirstNonEmpty(opt.GetStream(), svcOpt.GetStream())
	if stream != "" && bucket == "" {
		if cd.stream != "" && cd.stream != stream {
			return nil, core.Errorf(m.Desc, "subject %q is bound to stream %q and %q", address, cd.stream, stream)
		}
		cd.stream, cd.explicit = stream, true
	}
	return ch, nil
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

// messageChannel declares the channel of a message with its own subject.
func (p *Protocol) messageChannel(m *protogen.Message) (*core.Channel, error) {
	o := messageOptions(m)
	address := o.GetSubject()
	if o.GetKvBucket() != "" {
		address = kvSubject(o.GetKvBucket(), o.GetSubject())
	}
	meta := core.MessageOptions(m).GetChannel()
	ch, err := p.channel(m.Desc, address, o.GetKvBucket(), core.ChannelSpec{Meta: metas(meta), Servers: meta.GetServers()})
	if err != nil {
		return nil, err
	}
	msg, err := p.b.Message(m)
	if err != nil {
		return nil, err
	}
	ch.AddMessage(msg)
	return ch, nil
}

func kvSubject(bucket, key string) string {
	if key == "" {
		key = ">"
	}
	return "$KV." + bucket + "." + key
}

// channel parses a subject and returns its channel.
func (p *Protocol) channel(d protoreflect.Descriptor, address, bucket string, spec core.ChannelSpec) (*core.Channel, error) {
	sub, err := parseSubject(address)
	if err != nil {
		return nil, core.Errorf(d, "%v", err)
	}
	spec.DeclaredBy, spec.Address, spec.Params, spec.DefaultID = d, address, sub.params, channelID(address)
	ch, err := p.b.Channel(spec)
	if err != nil {
		return nil, err
	}
	if p.chans[ch] == nil {
		cd := &channelData{subject: sub}
		if bucket != "" {
			if !paramNameRE.MatchString(bucket) {
				return nil, core.Errorf(d, "invalid key-value bucket name %q", bucket)
			}
			cd.kvBucket, cd.stream, cd.explicit = bucket, "KV_"+bucket, true
		}
		p.chans[ch] = cd
	}
	return ch, nil
}

func (p *Protocol) addReply(op *core.Op, ch *core.Channel, m *protogen.Method, opt *natsv1.Operation, svcOpt *natsv1.Service, response *protogen.Message) error {
	b := p.b
	var reply *core.Channel
	if rs := opt.GetReplySubject(); rs != "" {
		var err error
		reply, err = p.channel(m.Desc, rs, "", core.ChannelSpec{})
		if err != nil {
			return err
		}
	} else {
		reply = b.ReplyChannel(ch.ID+".reply", fmt.Sprintf(
			"Replies to requests sent on `%s`. NATS delivers them to the reply subject (usually a unique `_INBOX` subject) chosen by the requester.",
			*ch.Obj.Address))
	}
	msg, err := b.Message(response)
	if err != nil {
		return err
	}
	b.SetReply(op, reply, msg)
	for _, name := range opt.GetReplyMessages() {
		name = strings.TrimPrefix(name, ".")
		pm := b.FindMessage(protoreflect.FullName(name))
		if pm == nil {
			return core.Errorf(m.Desc, "reply message %q not found (use the full Protobuf name, e.g. \"acme.v1.Error\")", name)
		}
		mi, err := b.Message(pm)
		if err != nil {
			return err
		}
		b.AddReplyMessage(op, reply, mi)
	}
	if svcOpt.GetMicro() != nil {
		b.AddReplyMessage(op, reply, p.microErrorMessage())
	}
	return nil
}
