package amqp

import (
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	amqpv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/amqp/asyncapi/v1"
)

func (p *Protocol) AddService(b *core.Builder, s *protogen.Service) error {
	opt := serviceOptions(s)
	for _, name := range core.ServiceOptions(s).GetServers() {
		if _, ok := b.Servers[name]; !ok {
			return core.Errorf(s.Desc, "unknown server %q", name)
		}
	}
	if x := opt.GetExchange(); x != "" && !p.topo.known(x) {
		return core.Errorf(s.Desc, "exchange %q is not declared in (amqp.asyncapi.v1.document).exchanges", x)
	}
	if opt.GetPrefetch() < 0 {
		return core.Errorf(s.Desc, "prefetch must be >= 0")
	}
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
func resolvePattern(m *protogen.Method, pat amqpv1.Pattern) (amqpv1.Pattern, core.Shape, error) {
	shapes := map[amqpv1.Pattern]core.Shape{
		amqpv1.Pattern_PATTERN_REQUEST_REPLY: core.ShapeRequestReply,
		amqpv1.Pattern_PATTERN_PUBLISH:       core.ShapePublish,
		amqpv1.Pattern_PATTERN_SUBSCRIBE:     core.ShapeSubscribe,
		amqpv1.Pattern_PATTERN_PROCESS:       core.ShapeProcess,
	}
	if pat != amqpv1.Pattern_PATTERN_UNSPECIFIED {
		return pat, shapes[pat], nil
	}
	shape, err := core.InferShape(m, "(amqp.asyncapi.v1.operation)")
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

// joinKey joins non-empty routing key segments with '.'.
func joinKey(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.Trim(p, "."); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, ".")
}

func (p *Protocol) addMethod(s *protogen.Service, svcOpt *amqpv1.Service, m *protogen.Method) error {
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
	msgOpt := messageOptions(payload)

	// Exchange, routing key and queue.
	exName := core.FirstNonEmpty(opt.GetExchange(), svcOpt.GetExchange())
	if exName == "" && opt.GetRoutingKey() == "" {
		exName = msgOpt.GetExchange()
	}
	if exName != "" && !p.topo.known(exName) {
		return fail("exchange %q is not declared in (amqp.asyncapi.v1.document).exchanges", exName)
	}
	ex := p.topo.exchange(exName)
	if ex.decl.GetInternal() {
		return fail("exchange %q is internal: publishers cannot publish to it", exName)
	}
	queueName := core.FirstNonEmpty(opt.GetQueue(), svcOpt.GetQueue())
	var key string
	switch {
	case opt.GetRoutingKey() != "":
		key = joinKey(svcOpt.GetRoutingKeyPrefix(), opt.GetRoutingKey())
	case msgOpt.GetRoutingKey() != "":
		key = msgOpt.GetRoutingKey()
	case exName == "" && queueName != "":
		key = queueName
	case ex.typeKnow && (ex.typ == amqpv1.ExchangeType_EXCHANGE_TYPE_FANOUT || ex.typ == amqpv1.ExchangeType_EXCHANGE_TYPE_HEADERS):
		// These exchanges ignore the routing key.
		key = ""
	default:
		prefix := svcOpt.GetRoutingKeyPrefix()
		if prefix == "" {
			prefix = joinKey(string(s.Desc.ParentFile().Package()), core.SnakeCase(string(s.Desc.Name())))
		}
		key = joinKey(prefix, core.SnakeCase(string(m.Desc.Name())))
	}
	params, err := parseKey(key)
	if err != nil {
		return fail("%v", err)
	}
	wildcard := strings.ContainsAny(key, "*#")
	switch {
	case exName == "" && (len(params) > 0 || wildcard):
		return fail("routing key %q: the default exchange delivers to the queue named after the routing key, so it cannot have parameters or wildcards", key)
	case exName == "" && queueName == "":
		queueName = key
	case exName == "" && queueName != key:
		return fail("the default exchange delivers to the queue named after the routing key: routing key %q does not reach queue %q", key, queueName)
	case wildcard && pattern != amqpv1.Pattern_PATTERN_SUBSCRIBE && pattern != amqpv1.Pattern_PATTERN_PROCESS:
		return fail("routing key %q: wildcards are binding patterns, only valid for SUBSCRIBE and PROCESS operations", key)
	case wildcard && !ex.topic():
		return fail("routing key %q: wildcards need a topic exchange, %q is %s", key, exName, typeName(ex.typ))
	}

	serviceReceives := pattern != amqpv1.Pattern_PATTERN_PUBLISH
	action := asyncapi.ActionSend
	if serviceReceives == (b.Params.Perspective == "server") {
		action = asyncapi.ActionReceive
	}

	routing, err := p.routingChannel(m.Desc, exName, key, core.ChannelSpec{
		Meta:    metas(core.OperationOptions(m).GetChannel(), core.MessageOptions(payload).GetChannel()),
		Servers: core.ChannelServers(s, m),
	})
	if err != nil {
		return err
	}
	var queueCh *core.Channel
	var q *queue
	if queueName != "" {
		if queueCh, err = p.queueChannel(m.Desc, queueName); err != nil {
			return err
		}
		q = p.topo.queue(queueName)
		if exName != "" && (q.decl == nil || len(q.decl.GetBindings()) == 0) {
			q.bind(&amqpv1.Binding{Exchange: exName, RoutingKey: bindingKey(key, ex.topic())})
		}
	}

	msg, err := b.Message(payload)
	if err != nil {
		return err
	}
	routing.AddMessage(msg)
	if queueCh != nil {
		queueCh.AddMessage(msg)
	}
	ch := routing
	if action == asyncapi.ActionReceive && queueCh != nil {
		ch = queueCh
	}

	inOpt := opt
	if shape == core.ShapeProcess {
		inOpt = proto.Clone(opt).(*amqpv1.Operation)
		inOpt.Publish = nil
	}
	bd, err := p.operationBinding(m, svcOpt, inOpt, ex, q, action, msg)
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
		Bindings:  asyncapi.NewBindings("amqp", bd),
	})
	if err != nil {
		return err
	}

	if shape == core.ShapeProcess {
		if err := p.addOutput(s, svcOpt, m, opt, ex, key, action, response); err != nil {
			return err
		}
	}
	if pattern == amqpv1.Pattern_PATTERN_REQUEST_REPLY {
		return p.addReply(op, m, opt, msg, response)
	}
	if opt.GetReplyQueue() != "" || len(opt.GetReplyMessages()) > 0 {
		return fail("reply_queue and reply_messages are only valid for REQUEST_REPLY operations")
	}
	return nil
}

// bind adds a binding unless the queue already has it.
func (q *queue) bind(bd *amqpv1.Binding) {
	for _, b := range q.bindings {
		if b.GetExchange() == bd.GetExchange() && b.GetRoutingKey() == bd.GetRoutingKey() {
			return
		}
	}
	q.bindings = append(q.bindings, bd)
}

var streamOffsetRE = regexp.MustCompile(`^(first|last|next|[0-9]+)$`)

// operationBinding validates the publish and consume settings of an rpc
// and renders the official operation binding of the documented side.
func (p *Protocol) operationBinding(m *protogen.Method, svcOpt *amqpv1.Service, opt *amqpv1.Operation, ex *exchange, q *queue, action asyncapi.Action, msg *core.Message) (*asyncapi.Map[any], error) {
	fail := func(format string, args ...any) error { return core.Errorf(m.Desc, format, args...) }
	pub, con := opt.GetPublish(), opt.GetConsume()
	var qdecl *amqpv1.Queue
	if q != nil {
		qdecl = q.decl
	}

	// Publish settings.
	if pr := pub.GetPriority(); pr < 0 || pr > 255 {
		return nil, fail("publish.priority must be between 0 and 255")
	}
	if max := q.maxPriority(); pub.GetPriority() > 0 && max > 0 && pub.GetPriority() > max {
		return nil, fail("publish.priority %d exceeds the max_priority %d of queue %q", pub.GetPriority(), max, q.name)
	}
	expiration, err := duration("publish.expiration", pub.GetExpiration())
	if err != nil {
		return nil, fail("%v", err)
	}
	delay, err := duration("publish.delay", pub.GetDelay())
	if err != nil {
		return nil, fail("%v", err)
	}
	if pub.GetDelay() != "" && !ex.decl.GetDelayed() {
		return nil, fail("publish.delay needs a delayed-message exchange, %q is not one", ex.name)
	}
	for _, k := range append(append([]string(nil), pub.GetCc()...), pub.GetBcc()...) {
		if _, err := parseKey(k); err != nil || k == "" {
			return nil, fail("publish cc and bcc must be routing keys, got %q", k)
		}
	}

	// Consume settings.
	prefetch := con.GetPrefetch()
	if prefetch == 0 && p.b.Params.Perspective == "server" {
		// The service's default configures its own consumers, not its
		// clients'.
		prefetch = svcOpt.GetPrefetch()
	}
	if prefetch < 0 {
		return nil, fail("consume.prefetch must be >= 0")
	}
	if off := con.GetStreamOffset(); off != "" {
		if qdecl.GetType() != amqpv1.QueueType_QUEUE_TYPE_STREAM {
			return nil, fail("consume.stream_offset needs a stream queue")
		}
		if !streamOffsetRE.MatchString(off) {
			if _, err := time.Parse(time.RFC3339, off); err != nil {
				if _, err := time.ParseDuration(off); err != nil {
					return nil, fail("consume.stream_offset %q is not first, last, next, an offset, an RFC 3339 timestamp or a duration", off)
				}
			}
		}
	}
	if qdecl.GetType() == amqpv1.QueueType_QUEUE_TYPE_STREAM && action == asyncapi.ActionReceive && prefetch == 0 {
		return nil, fail("stream queue %q: consumers must set a prefetch", q.name)
	}

	// Message properties set by publishers describe the messages on both
	// sides; publishing flags only apply to publishers, consuming settings
	// only to consumers.
	send := action == asyncapi.ActionSend
	bd := asyncapi.NewMap[any]()
	x := asyncapi.NewMap[any]()
	if pub.GetExpiration() != "" {
		bd.Set("expiration", expiration)
	}
	if pub.GetUserId() != "" {
		bd.Set("userId", pub.GetUserId())
	}
	if len(pub.GetCc()) > 0 {
		bd.Set("cc", pub.GetCc())
	}
	if pub.GetPriority() > 0 {
		bd.Set("priority", pub.GetPriority())
	}
	if dm := pub.GetDeliveryMode(); dm != amqpv1.DeliveryMode_DELIVERY_MODE_UNSPECIFIED {
		bd.Set("deliveryMode", int(dm))
	}
	if send && pub.GetMandatory() {
		bd.Set("mandatory", true)
	}
	if send && len(pub.GetBcc()) > 0 {
		bd.Set("bcc", pub.GetBcc())
	}
	if pub.GetTimestamp() {
		bd.Set("timestamp", true)
	}
	if !send {
		bd.Set("ack", !con.GetAutoAck())
	}
	if pub.GetAppId() != "" {
		x.Set("appId", pub.GetAppId())
	}
	if send && pub.GetConfirm() {
		x.Set("publisherConfirms", true)
	}
	if pub.GetDelay() != "" {
		x.Set("delay", delay)
		msg.AddHeader("x-delay", &asyncapi.Schema{Type: "integer", Minimum: 0, Description: "Delay before the delayed-message exchange routes the message, in milliseconds."}, false)
	}
	if !send {
		if prefetch > 0 && !con.GetAutoAck() {
			x.Set("prefetch", prefetch)
		}
		if con.GetExclusive() {
			x.Set("exclusive", true)
		}
		if con.GetConsumerTag() != "" {
			x.Set("consumerTag", con.GetConsumerTag())
		}
		if con.GetStreamOffset() != "" {
			x.Set("streamOffset", con.GetStreamOffset())
		}
		if con.GetPriority() != 0 {
			x.Set("priority", con.GetPriority())
		}
	}
	bd.Set("bindingVersion", BindingVersion)
	if x.Len() > 0 {
		bd.Set(ExtensionKey, x)
	}
	return bd, nil
}

func (q *queue) maxPriority() int32 {
	if q == nil {
		return 0
	}
	return q.decl.GetMaxPriority()
}

// addReply makes an rpc a request/reply operation: replies go to the queue
// named by the request's reply_to property, correlated by correlation_id.
func (p *Protocol) addReply(op *core.Op, m *protogen.Method, opt *amqpv1.Operation, request *core.Message, response *protogen.Message) error {
	b := p.b
	name := core.FirstNonEmpty(opt.GetReplyQueue(), directReplyTo)
	reply, err := p.queueChannel(m.Desc, name)
	if err != nil {
		return err
	}
	if name == directReplyTo && reply.Obj.Description == "" {
		reply.Obj.Description = "RabbitMQ direct reply-to: replies are delivered to the requester's connection without a reply queue."
	}
	msg, err := b.Message(response)
	if err != nil {
		return err
	}
	b.SetReply(op, reply, msg)
	if op.Obj.Reply.Address == nil {
		op.Obj.Reply.Address = &asyncapi.OperationReplyAddress{
			Location:    "$message.header#/reply_to",
			Description: "The reply_to property of the request names the queue replies are sent to, through the default exchange.",
		}
	}
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
	for _, mi := range append([]*core.Message{request}, messagesOf(reply)...) {
		if mi.Obj.CorrelationID == nil {
			mi.Obj.CorrelationID = &asyncapi.CorrelationID{
				Location:    "$message.header#/correlation_id",
				Description: "The correlation_id property, copied from the request to its reply.",
			}
		}
	}
	return nil
}

func messagesOf(ch *core.Channel) []*core.Message { return ch.Messages }

// addOutput adds the operation publishing a processor's output: the
// opposite action of its input, with the output routing key on the input's
// exchange.
func (p *Protocol) addOutput(s *protogen.Service, svcOpt *amqpv1.Service, m *protogen.Method, opt *amqpv1.Operation, ex *exchange, input string, inAction asyncapi.Action, output *protogen.Message) error {
	fail := func(format string, args ...any) error { return core.Errorf(m.Desc, format, args...) }
	if ex.typeKnow && (ex.typ == amqpv1.ExchangeType_EXCHANGE_TYPE_FANOUT || ex.typ == amqpv1.ExchangeType_EXCHANGE_TYPE_HEADERS) {
		return fail("exchange %q ignores routing keys: the output of a processor would reach its input; use a direct or topic exchange", ex.name)
	}
	key := joinKey(input, "output")
	if opt.GetOutput() != "" {
		key = joinKey(svcOpt.GetRoutingKeyPrefix(), opt.GetOutput())
	}
	params, err := parseKey(key)
	if err != nil {
		return fail("%v", err)
	}
	switch {
	case strings.ContainsAny(key, "*#"):
		return fail("output routing key %q must not contain wildcards; set output", key)
	case ex.name == "" && len(params) > 0:
		return fail("output routing key %q: the default exchange delivers to the queue named after the routing key, so it cannot have parameters", key)
	}
	routing, err := p.routingChannel(m.Desc, ex.name, key, core.ChannelSpec{Servers: core.ChannelServers(s, m)})
	if err != nil {
		return err
	}
	var queueCh *core.Channel
	if ex.name == "" {
		if queueCh, err = p.queueChannel(m.Desc, key); err != nil {
			return err
		}
	}
	msg, err := p.b.Message(output)
	if err != nil {
		return err
	}
	routing.AddMessage(msg)
	if queueCh != nil {
		queueCh.AddMessage(msg)
	}
	action := asyncapi.ActionReceive
	if inAction == asyncapi.ActionReceive {
		action = asyncapi.ActionSend
	}
	ch := routing
	if action == asyncapi.ActionReceive && queueCh != nil {
		ch = queueCh
	}
	var q *queue
	if queueCh != nil {
		q = p.topo.queue(key)
	}
	outOpt := proto.Clone(opt).(*amqpv1.Operation)
	outOpt.Consume = nil
	// The service's prefetch configures its input's consumers; the
	// output's consumers are the service's callers.
	bd, err := p.operationBinding(m, &amqpv1.Service{}, outOpt, ex, q, action, msg)
	if err != nil {
		return err
	}
	_, err = p.b.AddOperation(core.OpSpec{
		Service:   s,
		Method:    m,
		DefaultID: string(s.Desc.Name()) + "." + string(m.Desc.Name()),
		IDSuffix:  ".output",
		Action:    action,
		Channel:   ch,
		Payload:   msg,
		Bindings:  asyncapi.NewBindings("amqp", bd),
	})
	return err
}
