package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
	redisv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/redis/asyncapi/v1"
)

func (p *Protocol) AddService(b *core.Builder, s *protogen.Service) error {
	opt := serviceOptions(s)
	if prefix := opt.GetKeyPrefix(); prefix != "" {
		if _, err := parseAddress(prefix); err != nil {
			return core.Errorf(s.Desc, "key_prefix: %v", err)
		}
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
	for i, k := range opt.GetKeyspace() {
		if err := p.addKeyspace(s, opt, k, i); err != nil {
			return err
		}
	}
	return nil
}

// resolvePattern applies the inference rules documented on
// redis.asyncapi.v1.Pattern.
func resolvePattern(m *protogen.Method, pat redisv1.Pattern) (redisv1.Pattern, error) {
	if pat != redisv1.Pattern_PATTERN_UNSPECIFIED {
		return pat, nil
	}
	in, out := m.Desc.IsStreamingClient(), m.Desc.IsStreamingServer()
	switch {
	case in && out:
		return pat, core.Errorf(m.Desc, "cannot infer the Redis pattern of a bidirectional streaming rpc; set (redis.asyncapi.v1.operation).pattern")
	case out:
		return redisv1.Pattern_PATTERN_PUBLISH, nil
	case in, m.Output.Desc.FullName() == emptyFullName:
		return redisv1.Pattern_PATTERN_SUBSCRIBE, nil
	}
	return redisv1.Pattern_PATTERN_REQUEST_REPLY, nil
}

// transport resolves the transport of an rpc and its settings.
type transport struct {
	kind   string
	stream *redisv1.Stream
	list   *redisv1.List
}

func resolveTransport(opt *redisv1.Operation, svc *redisv1.Service, msg *redisv1.Message) transport {
	switch t := opt.GetTransport().(type) {
	case *redisv1.Operation_Pubsub:
		if t.Pubsub.GetSharded() {
			return transport{kind: kindSharded}
		}
		return transport{kind: kindPubSub}
	case *redisv1.Operation_Stream:
		return transport{kind: kindStream, stream: t.Stream}
	case *redisv1.Operation_List:
		return transport{kind: kindList, list: t.List}
	}
	if svc.GetTransport() != redisv1.Transport_TRANSPORT_UNSPECIFIED {
		return transport{kind: transportKind(svc.GetTransport())}
	}
	return transport{kind: transportKind(msg.GetTransport())}
}

func (p *Protocol) addMethod(s *protogen.Service, svcOpt *redisv1.Service, m *protogen.Method) error {
	b := p.b
	opt := operationOptions(m)
	pattern, err := resolvePattern(m, opt.GetPattern())
	if err != nil {
		return err
	}

	var payload, response *protogen.Message
	switch pattern {
	case redisv1.Pattern_PATTERN_REQUEST_REPLY:
		payload, response = m.Input, m.Output
	case redisv1.Pattern_PATTERN_SUBSCRIBE:
		payload = m.Input
	case redisv1.Pattern_PATTERN_PUBLISH:
		payload = m.Input
		if m.Desc.IsStreamingServer() || m.Input.Desc.FullName() == emptyFullName {
			payload = m.Output
		}
	}
	msgOpt := messageOptions(payload)
	tr := resolveTransport(opt, svcOpt, msgOpt)

	// The service consumes requests and subscriptions and produces
	// publications; the client perspective flips this.
	serviceReceives := pattern != redisv1.Pattern_PATTERN_PUBLISH
	action := asyncapi.ActionSend
	if serviceReceives == (b.Params.Perspective == "server") {
		action = asyncapi.ActionReceive
	}

	prefix := svcOpt.GetKeyPrefix()
	var name string
	switch {
	case opt.GetChannel() != "":
		name = joinKey(prefix, opt.GetChannel())
	case msgOpt.GetChannel() != "":
		name = msgOpt.GetChannel()
	default:
		if prefix == "" {
			prefix = joinKey(strings.ReplaceAll(string(s.Desc.ParentFile().Package()), ".", ":"), core.SnakeCase(string(s.Desc.Name())))
		}
		name = joinKey(prefix, core.SnakeCase(string(m.Desc.Name())))
	}
	ch, err := p.channel(m.Desc, name, tr.kind, core.ChannelSpec{
		Meta:    metas(core.OperationOptions(m).GetChannel(), core.MessageOptions(payload).GetChannel()),
		Servers: core.ChannelServers(s, m),
	})
	if err != nil {
		return err
	}
	cd := p.chans[ch]
	if cd.addr.glob && !(tr.kind == kindPubSub && pattern == redisv1.Pattern_PATTERN_SUBSCRIBE) {
		return core.Errorf(m.Desc, "channel %q: glob patterns are only valid for Pub/Sub subscriptions", name)
	}
	msg, err := b.Message(payload)
	if err != nil {
		return err
	}
	ch.AddMessage(msg)

	x, err := p.operationBinding(m, svcOpt, tr, cd, ch, action, msg)
	if err != nil {
		return err
	}
	op, err := b.AddOperation(core.OpSpec{
		Service:   s,
		Method:    m,
		DefaultID: string(s.Desc.Name()) + "." + string(m.Desc.Name()),
		Action:    action,
		Channel:   ch,
		Payload:   msg,
		Bindings:  asyncapi.NewBindings(BindingKey, x),
	})
	if err != nil {
		return err
	}

	if pattern == redisv1.Pattern_PATTERN_REQUEST_REPLY {
		return p.addReply(op, ch, m, opt, svcOpt, tr, response)
	}
	if opt.GetReplyChannel() != "" || len(opt.GetReplyMessages()) > 0 {
		return core.Errorf(m.Desc, "reply_channel and reply_messages are only valid for REQUEST_REPLY operations")
	}
	return nil
}

// operationBinding validates the transport settings of an rpc and renders
// the x-redis operation binding of the documented side.
func (p *Protocol) operationBinding(m *protogen.Method, svcOpt *redisv1.Service, tr transport, cd *channelData, ch *core.Channel, action asyncapi.Action, msg *core.Message) (*asyncapi.Map[any], error) {
	fail := func(format string, args ...any) error { return core.Errorf(m.Desc, format, args...) }
	send := action == asyncapi.ActionSend
	x := asyncapi.NewMap[any]()
	switch tr.kind {
	case kindPubSub:
		switch {
		case send:
			x.Set("command", "PUBLISH")
		case cd.addr.glob || len(cd.addr.params) > 0:
			x.Set("command", "PSUBSCRIBE")
			x.Set("pattern", cd.addr.pattern())
		default:
			x.Set("command", "SUBSCRIBE")
		}
	case kindSharded:
		if send {
			x.Set("command", "SPUBLISH")
		} else {
			x.Set("command", "SSUBSCRIBE")
		}
	case kindStream:
		if err := p.streamBinding(m, svcOpt, tr.stream, cd, ch, send, msg, x); err != nil {
			return nil, err
		}
	case kindList:
		l := tr.list
		push, pop := l.GetPush(), l.GetPop()
		if push == redisv1.End_END_UNSPECIFIED {
			push = redisv1.End_END_LEFT
		}
		if pop == redisv1.End_END_UNSPECIFIED {
			pop = redisv1.End_END_RIGHT
			if push == redisv1.End_END_RIGHT {
				pop = redisv1.End_END_LEFT
			}
		}
		if err := checkBlock("list.block", l.GetBlock()); err != nil {
			return nil, fail("%v", err)
		}
		if l.GetMaxLen() < 0 {
			return nil, fail("list.max_len must be >= 0")
		}
		var processing string
		if pl := l.GetProcessingList(); pl != "" {
			processing = joinKey(svcOpt.GetKeyPrefix(), pl)
			if _, err := parseAddress(processing); err != nil {
				return nil, fail("list.processing_list: %v", err)
			}
		}
		if send {
			x.Set("command", end(push)+"PUSH")
			if l.GetMaxLen() > 0 {
				x.Set("maxLen", l.GetMaxLen())
			}
		} else {
			if processing != "" {
				x.Set("command", "BLMOVE")
				x.Set("from", endName(pop))
				x.Set("processingList", processing)
			} else {
				x.Set("command", "B"+end(pop)+"POP")
			}
			if l.GetBlock() != "" {
				x.Set("block", l.GetBlock())
			}
		}
	}
	x.Set("bindingVersion", BindingVersion)
	return x, nil
}

func (p *Protocol) streamBinding(m *protogen.Method, svcOpt *redisv1.Service, st *redisv1.Stream, cd *channelData, ch *core.Channel, send bool, msg *core.Message, x *asyncapi.Map[any]) error {
	fail := func(format string, args ...any) error { return core.Errorf(m.Desc, format, args...) }
	if st.GetFlatten() && st.GetField() != "" {
		return fail("stream.field and stream.flatten are mutually exclusive")
	}
	if err := p.setEntry(m.Desc, ch, st.GetField(), st.GetFlatten()); err != nil {
		return err
	}
	grp := core.FirstNonEmpty(st.GetGroup(), svcOpt.GetConsumerGroup())
	if grp == "" {
		for _, set := range []struct {
			on   bool
			name string
		}{
			{st.GetConsumer() != "", "consumer"}, {st.GetNoAck(), "no_ack"}, {st.GetClaimMinIdle() != "", "claim_min_idle"},
			{st.GetMaxDeliveries() != 0, "max_deliveries"}, {st.GetDeadLetter() != "", "dead_letter"},
		} {
			if set.on {
				return fail("stream.%s requires a consumer group", set.name)
			}
		}
	}
	if id := st.GetStartId(); id != "" && !streamIDRE.MatchString(id) {
		return fail("stream.start_id %q is not \"$\", \"0\" or a stream ID such as \"1700000000000-0\"", id)
	}
	if st.GetCount() < 0 || st.GetMaxDeliveries() < 0 {
		return fail("stream.count and stream.max_deliveries must be >= 0")
	}
	if st.GetDeadLetter() != "" && st.GetMaxDeliveries() == 0 {
		return fail("stream.dead_letter requires max_deliveries")
	}
	if err := checkBlock("stream.block", st.GetBlock()); err != nil {
		return fail("%v", err)
	}
	if err := checkDuration("stream.claim_min_idle", st.GetClaimMinIdle()); err != nil {
		return fail("%v", err)
	}
	trim := st.GetTrim()
	if trim.GetMaxLen() < 0 || trim.GetLimit() < 0 {
		return fail("stream.trim.max_len and stream.trim.limit must be >= 0")
	}
	if id := trim.GetMinId(); id != "" && !streamIDRE.MatchString(id) {
		return fail("stream.trim.min_id %q is not a stream ID such as \"1700000000000-0\"", id)
	}
	if trim != nil && trim.GetStrategy() == nil {
		return fail("stream.trim must set max_len or min_id")
	}
	if trim.GetExact() && trim.GetLimit() > 0 {
		return fail("stream.trim.limit only applies to approximate trimming")
	}
	if grp != "" {
		if err := p.addGroup(m.Desc, cd, grp, st.GetStartId()); err != nil {
			return err
		}
	}
	if dl := st.GetDeadLetter(); dl != "" {
		dch, err := p.channel(m.Desc, joinKey(svcOpt.GetKeyPrefix(), dl), kindStream, core.ChannelSpec{Meta: []*asyncapiv3.Channel{{
			Description: fmt.Sprintf("Dead letters of `%s`: entries delivered more than %d times.", *ch.Obj.Address, st.GetMaxDeliveries()),
		}}})
		if err != nil {
			return err
		}
		if err := p.setEntry(m.Desc, dch, st.GetField(), st.GetFlatten()); err != nil {
			return err
		}
		dch.AddMessage(msg)
	}

	if send {
		x.Set("command", "XADD")
		if trim != nil {
			t := asyncapi.NewMap[any]()
			if trim.GetMinId() != "" {
				t.Set("strategy", "MINID")
				t.Set("threshold", trim.GetMinId())
			} else {
				t.Set("strategy", "MAXLEN")
				t.Set("threshold", trim.GetMaxLen())
			}
			t.Set("exact", trim.GetExact())
			if trim.GetLimit() > 0 {
				t.Set("limit", trim.GetLimit())
			}
			x.Set("trim", t)
		}
		if st.GetNoMkstream() {
			x.Set("noMkStream", true)
		}
		return nil
	}
	if grp == "" {
		x.Set("command", "XREAD")
		x.Set("startId", core.FirstNonEmpty(st.GetStartId(), "$"))
	} else {
		x.Set("command", "XREADGROUP")
		x.Set("group", grp)
		if st.GetConsumer() != "" {
			x.Set("consumer", st.GetConsumer())
		}
		if st.GetNoAck() {
			x.Set("noAck", true)
		}
	}
	if st.GetCount() > 0 {
		x.Set("count", st.GetCount())
	}
	if st.GetBlock() != "" {
		x.Set("block", st.GetBlock())
	}
	if st.GetClaimMinIdle() != "" {
		x.Set("claimMinIdle", st.GetClaimMinIdle())
	}
	if st.GetMaxDeliveries() > 0 {
		x.Set("maxDeliveries", st.GetMaxDeliveries())
	}
	if st.GetDeadLetter() != "" {
		x.Set("deadLetter", joinKey(svcOpt.GetKeyPrefix(), st.GetDeadLetter()))
	}
	return nil
}

func (p *Protocol) addGroup(d protoreflect.Descriptor, cd *channelData, name, startID string) error {
	if strings.ContainsAny(name, " \t\r\n") {
		return core.Errorf(d, "invalid consumer group %q", name)
	}
	for i, g := range cd.groups {
		if g.name == name {
			if startID != "" && g.startID != "" && g.startID != startID {
				return core.Errorf(d, "consumer group %q is created at %q and %q", name, g.startID, startID)
			}
			if g.startID == "" {
				cd.groups[i].startID = startID
			}
			return nil
		}
	}
	cd.groups = append(cd.groups, group{name, startID})
	return nil
}

func end(e redisv1.End) string {
	if e == redisv1.End_END_RIGHT {
		return "R"
	}
	return "L"
}

func endName(e redisv1.End) string {
	if e == redisv1.End_END_RIGHT {
		return "RIGHT"
	}
	return "LEFT"
}

func checkDuration(field, value string) error {
	if value == "" {
		return nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s: %q is not a duration like \"500ms\" or \"5s\"", field, value)
	}
	if d <= 0 {
		return fmt.Errorf("%s must be positive, got %q", field, value)
	}
	return nil
}

// checkBlock validates a blocking timeout, where "0" blocks forever.
func checkBlock(field, value string) error {
	if value == "0" {
		return nil
	}
	return checkDuration(field, value)
}

func (p *Protocol) addReply(op *core.Op, ch *core.Channel, m *protogen.Method, opt *redisv1.Operation, svcOpt *redisv1.Service, tr transport, response *protogen.Message) error {
	b := p.b
	var reply *core.Channel
	if rc := opt.GetReplyChannel(); rc != "" {
		var err error
		reply, err = p.channel(m.Desc, joinKey(svcOpt.GetKeyPrefix(), rc), tr.kind, core.ChannelSpec{})
		if err != nil {
			return err
		}
		if p.chans[reply].addr.glob {
			return core.Errorf(m.Desc, "reply_channel %q must not be a glob pattern", rc)
		}
		if tr.kind == kindStream {
			if err := p.setEntry(m.Desc, reply, tr.stream.GetField(), tr.stream.GetFlatten()); err != nil {
				return err
			}
		}
	} else {
		how := map[string]string{
			kindPubSub:  "published to the reply channel",
			kindSharded: "published (SPUBLISH) to the reply channel",
			kindStream:  "added to the reply stream",
			kindList:    "pushed to the reply list",
		}[tr.kind]
		reply = b.ReplyChannel(ch.ID+".reply", fmt.Sprintf("Replies to requests sent on `%s`, %s chosen by the requester.", *ch.Obj.Address, how))
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
	return nil
}

// addKeyspace documents keyspace notifications consumed by a service. They
// describe the service itself, so the client perspective omits them.
func (p *Protocol) addKeyspace(s *protogen.Service, svcOpt *redisv1.Service, k *redisv1.Keyspace, i int) error {
	b := p.b
	fail := func(format string, args ...any) error {
		return core.Errorf(s.Desc, "keyspace[%d]: %s", i, fmt.Sprintf(format, args...))
	}
	if k.GetDatabase() < 0 {
		return fail("database must be >= 0")
	}
	for _, ev := range k.GetEvents() {
		if !eventRE.MatchString(ev) {
			return fail("event %q is not an event name such as \"expired\" or \"xadd\"", ev)
		}
	}
	if k.GetName() != "" && !asyncapi.KeyPattern.MatchString(k.GetName()) {
		return fail("name %q is invalid (allowed: letters, digits, '.', '_' and '-')", k.GetName())
	}
	var key string
	if k.GetKey() != "" {
		key = joinKey(svcOpt.GetKeyPrefix(), k.GetKey())
	}
	db := strconv.Itoa(int(k.GetDatabase()))
	if b.Params.Perspective == "client" {
		return nil
	}

	// base names the notification's payload message.
	base := k.GetName()
	if base == "" {
		if k.GetKind() == redisv1.KeyspaceKind_KEYSPACE_KIND_KEYEVENT {
			base = "keyevent." + strings.Join(k.GetEvents(), "_")
		} else {
			base = "keyspace." + channelID(key)
		}
	}

	type notification struct{ channel, id, name, summary string }
	var list []notification
	var payload *core.Message
	if k.GetKind() == redisv1.KeyspaceKind_KEYSPACE_KIND_KEYEVENT {
		if len(k.GetEvents()) == 0 {
			return fail("KEYEVENT notifications require events")
		}
		for _, ev := range k.GetEvents() {
			name := core.FirstNonEmpty(k.GetName(), "keyevent")
			if k.GetName() == "" || len(k.GetEvents()) > 1 {
				name += "." + ev
			}
			list = append(list, notification{
				channel: "__keyevent@" + db + "__:" + ev,
				id:      "keyevent." + db + "." + channelID(ev),
				name:    name,
				summary: fmt.Sprintf("Receives the keys touched by `%s` events.", ev),
			})
		}
		desc := "Name of the key the event concerns."
		if key != "" {
			desc = fmt.Sprintf("Name of the key the event concerns, matching `%s`.", key)
		}
		payload = b.SyntheticMessage("redis."+string(s.Desc.Name())+"."+base,
			&asyncapi.Message{Name: "KeyeventNotification", Title: "Keyevent notification", ContentType: "text/plain"},
			&asyncapi.Schema{Type: "string", Description: desc})
	} else {
		if key == "" {
			return fail("KEYSPACE notifications require a key")
		}
		list = append(list, notification{
			channel: "__keyspace@" + db + "__:" + key,
			id:      "keyspace." + db + "." + channelID(key),
			name:    core.FirstNonEmpty(k.GetName(), "keyspace."+channelID(key)),
			summary: fmt.Sprintf("Receives the events touching `%s`.", key),
		})
		schema := &asyncapi.Schema{Type: "string", Description: "Name of the event, e.g. the command that touched the key or \"expired\"."}
		for _, ev := range k.GetEvents() {
			schema.Enum = append(schema.Enum, ev)
		}
		payload = b.SyntheticMessage("redis."+string(s.Desc.Name())+"."+base,
			&asyncapi.Message{Name: "KeyspaceNotification", Title: "Keyspace notification", ContentType: "text/plain"}, schema)
	}

	kind := kindSpace
	if k.GetKind() == redisv1.KeyspaceKind_KEYSPACE_KIND_KEYEVENT {
		kind = kindEvent
	}
	for _, n := range list {
		ch, err := p.channel(s.Desc, n.channel, kind, core.ChannelSpec{DefaultID: n.id})
		if err != nil {
			return err
		}
		cd := p.chans[ch]
		cd.database = k.GetDatabase()
		ch.AddMessage(payload)
		id := string(s.Desc.Name()) + "." + n.name
		if b.OperationIDTaken(id) {
			return fail("operation id %q is already used; set a name", id)
		}
		x := asyncapi.NewMap[any]()
		if cd.addr.glob || len(cd.addr.params) > 0 {
			x.Set("command", "PSUBSCRIBE")
			x.Set("pattern", cd.addr.pattern())
		} else {
			x.Set("command", "SUBSCRIBE")
		}
		if kind == kindSpace && len(k.GetEvents()) > 0 {
			x.Set("events", k.GetEvents())
		}
		x.Set("bindingVersion", BindingVersion)
		summary, description := n.summary, k.GetDescription()
		b.AddSyntheticOperation(id, &asyncapi.Operation{
			Action:      asyncapi.ActionReceive,
			Channel:     ch.Ref(),
			Summary:     summary,
			Description: description,
			Tags:        b.ServiceTags(s),
			Messages:    []*asyncapi.Reference{ch.MessageRef(payload)},
			Bindings:    asyncapi.NewBindings(BindingKey, x),
		}, ch, payload)
	}
	return nil
}
