package mqtt

import (
	"fmt"
	"math"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	mqttv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/mqtt/asyncapi/v1"
)

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

func resolvePattern(m *protogen.Method, pat mqttv1.Pattern) (mqttv1.Pattern, error) {
	if pat != mqttv1.Pattern_PATTERN_UNSPECIFIED {
		return pat, nil
	}
	in, out := m.Desc.IsStreamingClient(), m.Desc.IsStreamingServer()
	switch {
	case in && out:
		return pat, core.Errorf(m.Desc, "cannot infer the MQTT pattern of a bidirectional streaming rpc; set (mqtt.asyncapi.v1.operation).pattern")
	case out:
		return mqttv1.Pattern_PATTERN_PUBLISH, nil
	case in, m.Output.Desc.FullName() == emptyFullName:
		return mqttv1.Pattern_PATTERN_SUBSCRIBE, nil
	}
	return mqttv1.Pattern_PATTERN_REQUEST_REPLY, nil
}

func (p *Protocol) addMethod(s *protogen.Service, svcOpt *mqttv1.Service, m *protogen.Method) error {
	b := p.b
	fail := func(format string, args ...any) error { return core.Errorf(m.Desc, format, args...) }
	opt := operationOptions(m)
	pattern, err := resolvePattern(m, opt.GetPattern())
	if err != nil {
		return err
	}
	var payload, response *protogen.Message
	switch pattern {
	case mqttv1.Pattern_PATTERN_REQUEST_REPLY:
		payload, response = m.Input, m.Output
		if err := p.requireV5(true, "request/response (response topic and correlation data)"); err != nil {
			return fail("%v", err)
		}
	case mqttv1.Pattern_PATTERN_SUBSCRIBE:
		payload = m.Input
	case mqttv1.Pattern_PATTERN_PUBLISH:
		payload = m.Input
		if m.Desc.IsStreamingServer() || m.Input.Desc.FullName() == emptyFullName {
			payload = m.Output
		}
	}
	msgOpt := messageOptions(payload)

	var name string
	switch {
	case opt.GetTopic() != "":
		name = joinTopic(svcOpt.GetTopicPrefix(), opt.GetTopic())
	case msgOpt.GetTopic() != "":
		name = msgOpt.GetTopic()
	default:
		prefix := svcOpt.GetTopicPrefix()
		if prefix == "" {
			prefix = joinTopic(strings.ReplaceAll(string(s.Desc.ParentFile().Package()), ".", "/"), core.SnakeCase(string(s.Desc.Name())))
		}
		name = joinTopic(prefix, core.SnakeCase(string(m.Desc.Name())))
	}

	serviceReceives := pattern != mqttv1.Pattern_PATTERN_PUBLISH
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
	t := p.chans[ch]
	if pattern != mqttv1.Pattern_PATTERN_SUBSCRIBE {
		if err := t.publishable(); err != nil {
			return fail("%v", err)
		}
	}
	msg, err := b.Message(payload)
	if err != nil {
		return err
	}
	ch.AddMessage(msg)
	if strings.HasPrefix(name, "$") && action == asyncapi.ActionSend {
		// Only the broker publishes to "$" topics such as $SYS: callers of
		// a subscriber to them have nothing to document.
		return nil
	}

	// The service's default group applies to its own subscribers only.
	group := opt.GetSharedGroup()
	if group == "" && serviceReceives && b.Params.Perspective == "server" {
		group = svcOpt.GetSharedGroup()
	}
	q := opt.GetQos()
	if q == mqttv1.Qos_QOS_UNSPECIFIED {
		q = svcOpt.GetQos()
	}
	bd, err := p.operationBinding(opt, action, t, q, group)
	if err != nil {
		return fail("%v", err)
	}
	op, err := b.AddOperation(core.OpSpec{
		Service:   s,
		Method:    m,
		DefaultID: string(s.Desc.Name()) + "." + string(m.Desc.Name()),
		Action:    action,
		Channel:   ch,
		Payload:   msg,
		Bindings:  asyncapi.NewBindings(BindingKey, bd),
	})
	if err != nil {
		return err
	}

	if pattern != mqttv1.Pattern_PATTERN_REQUEST_REPLY {
		if opt.GetReplyTopic() != "" || len(opt.GetReplyMessages()) > 0 {
			return fail("reply_topic and reply_messages are only valid for REQUEST_REPLY operations")
		}
		return nil
	}
	var reply *core.Channel
	responseTopic := ""
	if rt := opt.GetReplyTopic(); rt != "" {
		responseTopic = joinTopic(svcOpt.GetTopicPrefix(), rt)
		if reply, err = p.topicChannel(m.Desc, responseTopic, core.ChannelSpec{}); err != nil {
			return err
		}
		if err := p.chans[reply].publishable(); err != nil {
			return fail("reply_topic: %v", err)
		}
	} else {
		reply = b.ReplyChannel(ch.ID+".reply", fmt.Sprintf("Responses to requests published on `%s`, published to the Response Topic chosen by each requester.", name))
	}
	resp, err := b.Message(response)
	if err != nil {
		return err
	}
	b.SetReply(op, reply, resp)
	if op.Obj.Reply.Address == nil && responseTopic == "" {
		op.Obj.Reply.Address = &asyncapi.OperationReplyAddress{
			Location:    "$message.header#/responseTopic",
			Description: "The Response Topic property of the request.",
		}
	}
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
	rt := responseTopic
	p.responses[msg] = &rt
	for _, mi := range append([]*core.Message{msg}, reply.Messages...) {
		if mi.Obj.CorrelationID == nil {
			mi.Obj.CorrelationID = &asyncapi.CorrelationID{
				Location:    "$message.header#/correlationData",
				Description: "The Correlation Data property, copied from the request to its response.",
			}
		}
	}
	return nil
}

// operationBinding validates the publish and subscription settings of an
// rpc and renders the official operation binding of the documented side.
func (p *Protocol) operationBinding(opt *mqttv1.Operation, action asyncapi.Action, t *topic, q mqttv1.Qos, group string) (*asyncapi.Map[any], error) {
	for _, f := range []struct {
		used    bool
		feature string
	}{
		{opt.GetMessageExpiry() != "", "message_expiry"},
		{group != "", "shared subscriptions"},
		{opt.GetRetainHandling() != mqttv1.RetainHandling_RETAIN_HANDLING_UNSPECIFIED, "retain_handling"},
		{opt.GetNoLocal(), "no_local"},
		{opt.GetRetainAsPublished(), "retain_as_published"},
	} {
		if err := p.requireV5(f.used, f.feature); err != nil {
			return nil, err
		}
	}
	expiry, err := seconds("message_expiry", opt.GetMessageExpiry(), math.MaxUint32)
	if err != nil {
		return nil, err
	}
	if strings.ContainsAny(group, "/+#") {
		return nil, fmt.Errorf("shared_group %q must not contain '/', '+' or '#'", group)
	}
	if group != "" && opt.GetNoLocal() {
		return nil, fmt.Errorf("no_local is a protocol error on shared subscriptions")
	}

	bd := asyncapi.NewMap[any]()
	if q != mqttv1.Qos_QOS_UNSPECIFIED {
		bd.Set("qos", qos(q))
	}
	if action == asyncapi.ActionSend {
		if opt.GetRetain() {
			bd.Set("retain", true)
		}
		if opt.GetMessageExpiry() != "" {
			bd.Set("messageExpiryInterval", expiry)
		}
		bd.Set("bindingVersion", BindingVersion)
		return bd, nil
	}
	bd.Set("bindingVersion", BindingVersion)
	x := asyncapi.NewMap[any]()
	filter := t.filter()
	if group != "" {
		filter = "$share/" + group + "/" + filter
		x.Set("sharedGroup", group)
	}
	x.Set("topicFilter", filter)
	switch opt.GetRetainHandling() {
	case mqttv1.RetainHandling_RETAIN_HANDLING_SEND_AT_SUBSCRIBE:
		x.Set("retainHandling", 0)
	case mqttv1.RetainHandling_RETAIN_HANDLING_SEND_IF_NEW:
		x.Set("retainHandling", 1)
	case mqttv1.RetainHandling_RETAIN_HANDLING_DO_NOT_SEND:
		x.Set("retainHandling", 2)
	}
	if opt.GetNoLocal() {
		x.Set("noLocal", true)
	}
	if opt.GetRetainAsPublished() {
		x.Set("retainAsPublished", true)
	}
	bd.Set(ExtensionKey, x)
	return bd, nil
}
