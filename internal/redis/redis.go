// Package redis documents Redis messaging APIs (Pub/Sub, sharded Pub/Sub,
// Streams, Lists used as queues and keyspace notifications) described by
// Protobuf services.
package redis

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	redisv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/redis/asyncapi/v1"
)

// BindingKey is the key of the Redis bindings. AsyncAPI reserves the
// "redis" bindings for future use (they must stay empty), so the Redis
// details go in an x- binding.
const BindingKey = "x-redis"

// BindingVersion is the version of the x-redis binding objects.
const BindingVersion = "0.1.0"

const emptyFullName = "google.protobuf.Empty"

// Channel kinds, as rendered in the x-redis channel binding.
const (
	kindPubSub  = "pubsub"
	kindSharded = "sharded-pubsub"
	kindStream  = "stream"
	kindList    = "list"
	kindSpace   = "keyspace"
	kindEvent   = "keyevent"
)

// DefaultEntryField is the stream entry field holding the payload.
const DefaultEntryField = "payload"

// Protocol implements core.Protocol for Redis.
type Protocol struct {
	// Per document state, reset by Begin.
	b        *core.Builder
	settings []*fileSettings
	chans    map[*core.Channel]*channelData
	order    []*core.Channel
}

type fileSettings struct {
	file *protogen.File
	doc  *redisv1.Document
}

// channelData holds the Redis specifics of a channel.
type channelData struct {
	kind   string
	addr   *address
	groups []group
	// Stream entry layout, once a stream operation or message sets it.
	entrySet bool
	field    string
	flatten  bool
	// database of keyspace notification channels.
	database int32
}

type group struct{ name, startID string }

// New returns the Redis protocol.
func New() core.Protocol { return &Protocol{} }

func (p *Protocol) Name() string { return "redis" }

func (p *Protocol) Options() []core.Option { return nil }

func documentOptions(f *protogen.File) *redisv1.Document {
	return core.Extension[*redisv1.Document](f.Desc.Options(), redisv1.E_Document)
}

func serviceOptions(s *protogen.Service) *redisv1.Service {
	return core.Extension[*redisv1.Service](s.Desc.Options(), redisv1.E_Service)
}

func operationOptions(m *protogen.Method) *redisv1.Operation {
	return core.Extension[*redisv1.Operation](m.Desc.Options(), redisv1.E_Operation)
}

func messageOptions(m *protogen.Message) *redisv1.Message {
	if m == nil {
		return nil
	}
	return core.Extension[*redisv1.Message](m.Desc.Options(), redisv1.E_Message)
}

// Claims documents services with Redis annotations.
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

// HasContent reports messages with their own channel, or Redis document
// options in the documented files.
func (p *Protocol) HasContent(b *core.Builder) bool {
	for _, f := range b.Files {
		if documentOptions(f) != nil {
			return true
		}
		found := false
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if messageOptions(m).GetChannel() != "" {
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
	p.order = nil
	for _, f := range b.Imports() {
		if d := documentOptions(f); d != nil {
			p.settings = append(p.settings, &fileSettings{file: f, doc: d})
		}
	}
	return p.serverDetails()
}

// keyspaceFlags are the characters of the notify-keyspace-events setting.
const keyspaceFlags = "KEg$lshzxetmdnA"

// serverDetails adds the x-redis server bindings and the Redis
// authentication mechanism of security schemes.
func (p *Protocol) serverDetails() error {
	b := p.b
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, srv := range set.doc.GetServers() {
			out, ok := b.Doc.Servers.Get(srv.GetName())
			if !ok {
				return fmt.Errorf("%s: redis server %q is not declared in the servers of the asyncapi.v3.document file option", where, srv.GetName())
			}
			fail := func(format string, args ...any) error {
				return fmt.Errorf("%s: redis server %q: %s", where, srv.GetName(), fmt.Sprintf(format, args...))
			}
			x := asyncapi.NewMap[any]()
			mode := srv.GetMode()
			switch {
			case srv.GetDatabase() < 0:
				return fail("database must be >= 0")
			case srv.GetDatabase() != 0 && mode == redisv1.Mode_MODE_CLUSTER:
				return fail("Redis Cluster only has database 0")
			case srv.GetSentinelMaster() != "" && mode != redisv1.Mode_MODE_SENTINEL:
				return fail("sentinel_master requires mode MODE_SENTINEL")
			case srv.GetSentinelMaster() == "" && mode == redisv1.Mode_MODE_SENTINEL:
				return fail("mode MODE_SENTINEL requires sentinel_master")
			case srv.GetResp() != 0 && srv.GetResp() != 2 && srv.GetResp() != 3:
				return fail("resp must be 2 or 3")
			}
			if ev := srv.GetNotifyKeyspaceEvents(); strings.Trim(ev, keyspaceFlags) != "" {
				return fail("notify_keyspace_events %q has unknown flags (allowed: %s)", ev, keyspaceFlags)
			}
			if mode != redisv1.Mode_MODE_UNSPECIFIED {
				x.Set("mode", strings.ToLower(strings.TrimPrefix(mode.String(), "MODE_")))
			}
			if srv.GetSentinelMaster() != "" {
				x.Set("sentinelMaster", srv.GetSentinelMaster())
			}
			if srv.GetDatabase() != 0 {
				x.Set("database", srv.GetDatabase())
			}
			if srv.GetTls() {
				x.Set("tls", true)
			}
			if srv.GetResp() != 0 {
				x.Set("resp", srv.GetResp())
			}
			if srv.GetNotifyKeyspaceEvents() != "" {
				x.Set("notifyKeyspaceEvents", srv.GetNotifyKeyspaceEvents())
			}
			x.Set("bindingVersion", BindingVersion)
			if out.Bindings == nil {
				out.Bindings = &asyncapi.Bindings{}
			}
			out.Bindings.Set(BindingKey, x)
		}
		for _, a := range set.doc.GetAuth() {
			var sc *asyncapi.SecurityScheme
			if c := b.Doc.Components; c != nil {
				sc, _ = c.SecuritySchemes.Get(a.GetScheme())
			}
			if sc == nil {
				return fmt.Errorf("%s: redis auth for %q: the security scheme is not declared in the asyncapi.v3.document file option", where, a.GetScheme())
			}
			typ, kind := "", ""
			switch a.GetType() {
			case redisv1.AuthType_AUTH_TYPE_PASSWORD:
				typ, kind = asyncapi.SecurityUserPassword, "password"
			case redisv1.AuthType_AUTH_TYPE_ACL:
				typ, kind = asyncapi.SecurityUserPassword, "acl"
			case redisv1.AuthType_AUTH_TYPE_TLS:
				typ, kind = asyncapi.SecurityX509, "tls"
			default:
				return fmt.Errorf("%s: redis auth for %q: type is required", where, a.GetScheme())
			}
			if sc.Type == "" {
				sc.Type = typ
			}
			if sc.Extensions == nil {
				sc.Extensions = asyncapi.Extensions{}
			}
			sc.Extensions["x-redis-auth"] = kind
		}
	}
	return nil
}

func (p *Protocol) Finish(b *core.Builder) error {
	// Messages with their own channel get one even without operation.
	for _, f := range b.Files {
		var standalone []*protogen.Message
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if messageOptions(m).GetChannel() != "" {
				standalone = append(standalone, m)
			}
		})
		for _, m := range standalone {
			o := messageOptions(m)
			kind := transportKind(o.GetTransport())
			meta := core.MessageOptions(m).GetChannel()
			ch, err := p.channel(m.Desc, o.GetChannel(), kind, core.ChannelSpec{Meta: metas(meta), Servers: meta.GetServers()})
			if err != nil {
				return err
			}
			if kind == kindStream {
				if err := p.setEntry(m.Desc, ch, "", false); err != nil {
					return err
				}
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
	}
	return nil
}

// emitChannel renders the x-redis channel binding.
func (p *Protocol) emitChannel(ch *core.Channel) {
	cd := p.chans[ch]
	x := asyncapi.NewMap[any]()
	x.Set("type", cd.kind)
	switch cd.kind {
	case kindStream:
		if cd.flatten {
			x.Set("entry", "flattened")
		} else {
			x.Set("entryField", core.FirstNonEmpty(cd.field, DefaultEntryField))
		}
		if len(cd.groups) > 0 {
			var groups []any
			for _, g := range cd.groups {
				m := asyncapi.NewMap[any]()
				m.Set("name", g.name)
				m.Set("startId", core.FirstNonEmpty(g.startID, "$"))
				groups = append(groups, m)
			}
			x.Set("consumerGroups", groups)
		}
	case kindSpace, kindEvent:
		x.Set("database", cd.database)
	}
	x.Set("bindingVersion", BindingVersion)
	if ch.Obj.Bindings == nil {
		ch.Obj.Bindings = &asyncapi.Bindings{}
	}
	ch.Obj.Bindings.Set(BindingKey, x)
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

func transportKind(t redisv1.Transport) string {
	switch t {
	case redisv1.Transport_TRANSPORT_SHARDED_PUBSUB:
		return kindSharded
	case redisv1.Transport_TRANSPORT_STREAM:
		return kindStream
	case redisv1.Transport_TRANSPORT_LIST:
		return kindList
	}
	return kindPubSub
}

// channel parses a channel name or key and returns its channel.
func (p *Protocol) channel(d protoreflect.Descriptor, name, kind string, spec core.ChannelSpec) (*core.Channel, error) {
	addr, err := parseAddress(name)
	if err != nil {
		return nil, core.Errorf(d, "%v", err)
	}
	if spec.DefaultID == "" {
		spec.DefaultID = channelID(name)
	}
	spec.DeclaredBy, spec.Address, spec.Params = d, name, addr.params
	ch, err := p.b.Channel(spec)
	if err != nil {
		return nil, err
	}
	cd := p.chans[ch]
	if cd == nil {
		cd = &channelData{kind: kind, addr: addr}
		p.chans[ch] = cd
		p.order = append(p.order, ch)
	} else if cd.kind != kind {
		return nil, core.Errorf(d, "channel %q is used both as %s and as %s", name, cd.kind, kind)
	}
	return ch, nil
}

// setEntry records the stream entry layout of a channel.
func (p *Protocol) setEntry(d protoreflect.Descriptor, ch *core.Channel, field string, flatten bool) error {
	cd := p.chans[ch]
	if field == "" && !flatten {
		field = DefaultEntryField
	}
	if cd.entrySet && (cd.field != field || cd.flatten != flatten) {
		return core.Errorf(d, "stream %q already stores entries %s", *ch.Obj.Address, describeEntry(cd.field, cd.flatten))
	}
	cd.entrySet, cd.field, cd.flatten = true, field, flatten
	return nil
}

func describeEntry(field string, flatten bool) string {
	if flatten {
		return "flattened"
	}
	return fmt.Sprintf("in field %q", core.FirstNonEmpty(field, DefaultEntryField))
}
