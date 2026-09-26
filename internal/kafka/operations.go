package kafka

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	kafkav1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/kafka/asyncapi/v1"
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

func resolvePattern(m *protogen.Method, pat kafkav1.Pattern) (kafkav1.Pattern, error) {
	if pat != kafkav1.Pattern_PATTERN_UNSPECIFIED {
		return pat, nil
	}
	in, out := m.Desc.IsStreamingClient(), m.Desc.IsStreamingServer()
	switch {
	case in && out:
		return pat, core.Errorf(m.Desc, "cannot infer the Kafka pattern of a bidirectional streaming rpc; set (kafka.asyncapi.v1.operation).pattern")
	case out:
		return kafkav1.Pattern_PATTERN_PUBLISH, nil
	case in, m.Output.Desc.FullName() == emptyFullName:
		return kafkav1.Pattern_PATTERN_SUBSCRIBE, nil
	}
	return kafkav1.Pattern_PATTERN_REQUEST_REPLY, nil
}

func joinTopic(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.Trim(p, "."); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, ".")
}

func (p *Protocol) addMethod(s *protogen.Service, svcOpt *kafkav1.Service, m *protogen.Method) error {
	b := p.b
	fail := func(format string, args ...any) error { return core.Errorf(m.Desc, format, args...) }
	opt := operationOptions(m)
	pattern, err := resolvePattern(m, opt.GetPattern())
	if err != nil {
		return err
	}
	var payload, response *protogen.Message
	switch pattern {
	case kafkav1.Pattern_PATTERN_REQUEST_REPLY:
		payload, response = m.Input, m.Output
		if opt.GetReplyTopic() == "" {
			return fail("Kafka has no built-in replies: REQUEST_REPLY operations must set reply_topic")
		}
	case kafkav1.Pattern_PATTERN_SUBSCRIBE:
		payload = m.Input
	case kafkav1.Pattern_PATTERN_PUBLISH:
		payload = m.Input
		if m.Desc.IsStreamingServer() || m.Input.Desc.FullName() == emptyFullName {
			payload = m.Output
		}
	}
	msgOpt := messageOptions(payload)

	if t := opt.GetTopic(); t != "" {
		if _, err := parseTopic(t); err != nil {
			return fail("%v", err)
		}
	}
	var topic string
	switch {
	case opt.GetTopic() != "":
		topic = joinTopic(svcOpt.GetTopicPrefix(), opt.GetTopic())
	case msgOpt.GetTopic() != "":
		topic = msgOpt.GetTopic()
	default:
		prefix := svcOpt.GetTopicPrefix()
		if prefix == "" {
			prefix = joinTopic(string(s.Desc.ParentFile().Package()), core.SnakeCase(string(s.Desc.Name())))
		}
		topic = joinTopic(prefix, core.SnakeCase(string(m.Desc.Name())))
	}
	if compacted(p.topics[topic]) && msgOpt.GetKey() == nil {
		return fail("topic %q is compacted: its records need keys; set (kafka.asyncapi.v1.message).key on %s", topic, payload.Desc.FullName())
	}

	serviceReceives := pattern != kafkav1.Pattern_PATTERN_PUBLISH
	action := asyncapi.ActionSend
	if serviceReceives == (b.Params.Perspective == "server") {
		action = asyncapi.ActionReceive
	}

	ch, err := p.topicChannel(m.Desc, topic, core.ChannelSpec{
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

	// The service's defaults configure its own clients, not its callers'.
	server := b.Params.Perspective == "server"
	// consumerGroup is the group of whoever consumes: the service's default
	// applies when the service does. It is validated either way, and
	// rendered on the documented side only.
	consumerGroup := opt.GetGroupId()
	if consumerGroup == "" && serviceReceives {
		consumerGroup = svcOpt.GetGroupId()
	}
	group := opt.GetGroupId()
	if group == "" && server && serviceReceives {
		group = svcOpt.GetGroupId()
	}
	clientID := opt.GetClientId()
	if clientID == "" && server {
		clientID = svcOpt.GetClientId()
	}
	bd, err := operationBinding(opt, action, consumerGroup, group, clientID)
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

	if pattern != kafkav1.Pattern_PATTERN_REQUEST_REPLY {
		if opt.GetReplyTopic() != "" || len(opt.GetReplyMessages()) > 0 || opt.GetCorrelationHeader() != "" {
			return fail("reply_topic, reply_messages and correlation_header are only valid for REQUEST_REPLY operations")
		}
		return nil
	}
	replyTopic := joinTopic(svcOpt.GetTopicPrefix(), opt.GetReplyTopic())
	if compacted(p.topics[replyTopic]) && messageOptions(response).GetKey() == nil {
		return fail("reply topic %q is compacted: its records need keys; set (kafka.asyncapi.v1.message).key on %s", replyTopic, response.Desc.FullName())
	}
	reply, err := p.topicChannel(m.Desc, replyTopic, core.ChannelSpec{})
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
	header := core.FirstNonEmpty(opt.GetCorrelationHeader(), "correlation_id")
	for _, mi := range append([]*core.Message{msg}, reply.Messages...) {
		mi.AddHeader(header, &asyncapi.Schema{Type: "string", Description: "Record header correlating a reply with its request."}, true)
		if mi.Obj.CorrelationID == nil {
			mi.Obj.CorrelationID = &asyncapi.CorrelationID{
				Location:    "$message.header#/" + core.JSONPointerEscape(header),
				Description: fmt.Sprintf("The %s header, copied from the request to its reply.", header),
			}
		}
	}
	return nil
}

// constant is the schema of a known string value, as the official binding
// describes group and client IDs.
func constant(v string) *asyncapi.Schema {
	return &asyncapi.Schema{Type: "string", Enum: []any{v}}
}

// operationBinding validates the producer and consumer settings of an rpc
// and renders the official operation binding of the documented side, with
// the settings it does not define under x-kafka.
func operationBinding(opt *kafkav1.Operation, action asyncapi.Action, consumerGroup, group, clientID string) (*asyncapi.Map[any], error) {
	pr, con := opt.GetProduce(), opt.GetConsume()
	acks := pr.GetAcks()
	switch {
	case (pr.GetIdempotent() || pr.GetTransactionalId() != "") && (acks == kafkav1.Acks_ACKS_NONE || acks == kafkav1.Acks_ACKS_LEADER):
		return nil, fmt.Errorf("idempotent and transactional producers need acks=all")
	case pr.GetCompression() == kafkav1.Compression_COMPRESSION_PRODUCER:
		return nil, fmt.Errorf("produce.compression: PRODUCER only applies to topics")
	case pr.GetBatchSize() < 0 || con.GetMaxPollRecords() < 0:
		return nil, fmt.Errorf("batch_size and max_poll_records must be >= 0")
	case con.GetAutoCommit() && consumerGroup == "":
		return nil, fmt.Errorf("consume.auto_commit needs a consumer group (group_id)")
	case con.GetGroupInstanceId() != "" && consumerGroup == "":
		return nil, fmt.Errorf("consume.group_instance_id needs a consumer group (group_id)")
	case con.GetNextGenRebalance() && con.GetAssignmentStrategy() != kafkav1.AssignmentStrategy_ASSIGNMENT_STRATEGY_UNSPECIFIED:
		return nil, fmt.Errorf("consume.assignment_strategy does not apply to the KIP-848 group protocol, which assigns partitions on the broker")
	}
	durations := map[string]int64{}
	for _, d := range [][2]string{
		{"produce.linger", pr.GetLinger()}, {"produce.delivery_timeout", pr.GetDeliveryTimeout()},
		{"consume.session_timeout", con.GetSessionTimeout()}, {"consume.max_poll_interval", con.GetMaxPollInterval()},
	} {
		ms, err := millis(d[0], d[1])
		if err != nil {
			return nil, err
		}
		durations[d[0]] = ms
	}

	bd := asyncapi.NewMap[any]()
	x := asyncapi.NewMap[any]()
	if action == asyncapi.ActionReceive {
		if group != "" {
			bd.Set("groupId", constant(group))
		}
		if clientID != "" {
			bd.Set("clientId", constant(clientID))
		}
		if r := con.GetAutoOffsetReset(); r != kafkav1.OffsetReset_OFFSET_RESET_UNSPECIFIED {
			x.Set("autoOffsetReset", lower(r, "OFFSET_RESET_"))
		}
		if con.GetReadCommitted() {
			x.Set("isolationLevel", "read_committed")
		}
		if con.GetAutoCommit() {
			x.Set("enableAutoCommit", true)
		}
		if con.GetMaxPollRecords() > 0 {
			x.Set("maxPollRecords", con.GetMaxPollRecords())
		}
		if con.GetSessionTimeout() != "" {
			x.Set("sessionTimeoutMs", durations["consume.session_timeout"])
		}
		if con.GetMaxPollInterval() != "" {
			x.Set("maxPollIntervalMs", durations["consume.max_poll_interval"])
		}
		if a := con.GetAssignmentStrategy(); a != kafkav1.AssignmentStrategy_ASSIGNMENT_STRATEGY_UNSPECIFIED {
			x.Set("partitionAssignmentStrategy", lower(a, "ASSIGNMENT_STRATEGY_"))
		}
		if con.GetGroupInstanceId() != "" {
			x.Set("groupInstanceId", con.GetGroupInstanceId())
		}
		if con.GetNextGenRebalance() {
			x.Set("groupProtocol", "consumer")
		}
	} else {
		if clientID != "" {
			bd.Set("clientId", constant(clientID))
		}
		switch acks {
		case kafkav1.Acks_ACKS_NONE:
			x.Set("acks", "0")
		case kafkav1.Acks_ACKS_LEADER:
			x.Set("acks", "1")
		case kafkav1.Acks_ACKS_ALL:
			x.Set("acks", "all")
		}
		if pr.GetIdempotent() || pr.GetTransactionalId() != "" {
			x.Set("enableIdempotence", true)
		}
		if pr.GetTransactionalId() != "" {
			x.Set("transactionalId", pr.GetTransactionalId())
		}
		if c := pr.GetCompression(); c != kafkav1.Compression_COMPRESSION_UNSPECIFIED {
			x.Set("compressionType", lower(c, "COMPRESSION_"))
		}
		if pr.GetLinger() != "" {
			x.Set("lingerMs", durations["produce.linger"])
		}
		if pr.GetBatchSize() > 0 {
			x.Set("batchSize", pr.GetBatchSize())
		}
		if pr.GetDeliveryTimeout() != "" {
			x.Set("deliveryTimeoutMs", durations["produce.delivery_timeout"])
		}
		if pr.GetPartitioner() != "" {
			x.Set("partitioner", pr.GetPartitioner())
		}
	}
	bd.Set("bindingVersion", BindingVersion)
	if x.Len() > 0 {
		bd.Set(ExtensionKey, x)
	}
	return bd, nil
}
