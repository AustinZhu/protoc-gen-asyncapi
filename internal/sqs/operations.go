package sqs

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	sqsv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/sqs/asyncapi/v1"
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

// resolvePattern returns the pattern of an rpc: the annotated one, or the
// one its shape implies.
func resolvePattern(m *protogen.Method, pat sqsv1.Pattern) (sqsv1.Pattern, core.Shape, error) {
	shapes := map[sqsv1.Pattern]core.Shape{
		sqsv1.Pattern_PATTERN_REQUEST_REPLY: core.ShapeRequestReply,
		sqsv1.Pattern_PATTERN_PUBLISH:       core.ShapePublish,
		sqsv1.Pattern_PATTERN_SUBSCRIBE:     core.ShapeSubscribe,
		sqsv1.Pattern_PATTERN_PROCESS:       core.ShapeProcess,
	}
	if pat != sqsv1.Pattern_PATTERN_UNSPECIFIED {
		return pat, shapes[pat], nil
	}
	shape, err := core.InferShape(m, "(sqs.asyncapi.v1.operation)")
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

// joinName joins queue name segments with '-', keeping a ".fifo" suffix
// last.
func joinName(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.Trim(p, "-"); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "-")
}

func (p *Protocol) addMethod(s *protogen.Service, svcOpt *sqsv1.Service, m *protogen.Method) error {
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
	if pattern == sqsv1.Pattern_PATTERN_REQUEST_REPLY {
		if opt.GetReplyQueue() == "" {
			return fail("SQS has no built-in replies: REQUEST_REPLY operations must set reply_queue")
		}
	}

	var name string
	switch {
	case opt.GetQueue() != "":
		name = joinName(svcOpt.GetQueuePrefix(), opt.GetQueue())
	case messageOptions(payload).GetQueue() != "":
		name = messageOptions(payload).GetQueue()
	default:
		prefix := svcOpt.GetQueuePrefix()
		if prefix == "" {
			prefix = joinName(strings.ReplaceAll(string(s.Desc.ParentFile().Package()), ".", "-"), core.SnakeCase(string(s.Desc.Name())))
		}
		name = joinName(prefix, core.SnakeCase(string(m.Desc.Name())))
	}

	serviceReceives := pattern != sqsv1.Pattern_PATTERN_PUBLISH
	action := asyncapi.ActionSend
	if serviceReceives == (b.Params.Perspective == "server") {
		action = asyncapi.ActionReceive
	}

	ch, err := p.queueChannel(m.Desc, name, core.ChannelSpec{
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

	inOpt := opt
	if shape == core.ShapeProcess {
		inOpt = proto.Clone(opt).(*sqsv1.Operation)
		inOpt.Send = nil
	}
	bd, err := p.operationBinding(inOpt, action, name, shape != core.ShapeProcess)
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

	if shape == core.ShapeProcess {
		if err := p.addOutput(s, svcOpt, m, opt, name, action, response); err != nil {
			return err
		}
	}
	if pattern != sqsv1.Pattern_PATTERN_REQUEST_REPLY {
		if opt.GetReplyQueue() != "" || len(opt.GetReplyMessages()) > 0 {
			return fail("reply_queue and reply_messages are only valid for REQUEST_REPLY operations")
		}
		return nil
	}
	reply, err := p.queueChannel(m.Desc, joinName(svcOpt.GetQueuePrefix(), opt.GetReplyQueue()), core.ChannelSpec{})
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
		mi.AddHeader("correlation_id", &asyncapi.Schema{Type: "string", Description: "Message attribute correlating a reply with its request."}, true)
		if mi.Obj.CorrelationID == nil {
			mi.Obj.CorrelationID = &asyncapi.CorrelationID{
				Location:    "$message.header#/correlation_id",
				Description: "The correlation_id message attribute, copied from the request to its reply.",
			}
		}
	}
	return nil
}

// checkTemplate validates a message group or deduplication ID template.
func checkTemplate(field, v string) error {
	if strings.ContainsAny(paramRE.ReplaceAllString(v, ""), "{}") {
		return fmt.Errorf("%s %q has unbalanced braces", field, v)
	}
	if len(v) > 128 {
		return fmt.Errorf("%s is longer than 128 characters", field)
	}
	return nil
}

// operationBinding validates the send and receive settings of an rpc and
// renders the official operation binding of the documented side. The send
// settings of the queue are only checked when its messages are sent by the
// rpc's parties (not for a processor's input).
func (p *Protocol) operationBinding(opt *sqsv1.Operation, action asyncapi.Action, name string, sent bool) (*asyncapi.Map[any], error) {
	send, recv := opt.GetSend(), opt.GetReceive()
	fifo := isFIFO(name)
	q := p.queues[name]
	switch {
	case fifo && sent && send.GetMessageGroupId() == "":
		return nil, fmt.Errorf("queue %q is FIFO: set send.message_group_id", name)
	case fifo && sent && send.GetDeduplicationId() == "" && !q.GetContentBasedDeduplication():
		return nil, fmt.Errorf("queue %q is FIFO without content-based deduplication: set send.deduplication_id", name)
	case !fifo && (send.GetMessageGroupId() != "" || send.GetDeduplicationId() != ""):
		return nil, fmt.Errorf("message_group_id and deduplication_id only apply to FIFO queues (names ending with \".fifo\")")
	case fifo && send.GetDelay() != "":
		return nil, fmt.Errorf("FIFO queues have no per-message delay; set the queue's delivery_delay instead")
	case recv.GetMaxMessages() < 0 || recv.GetMaxMessages() > 10:
		return nil, fmt.Errorf("receive.max_messages must be between 1 and 10")
	}
	for _, t := range [][2]string{{"send.message_group_id", send.GetMessageGroupId()}, {"send.deduplication_id", send.GetDeduplicationId()}} {
		if err := checkTemplate(t[0], t[1]); err != nil {
			return nil, err
		}
	}
	delay, err := between("send.delay", send.GetDelay(), 0, 15*time.Minute)
	if err != nil {
		return nil, err
	}
	wait, err := between("receive.wait_time", recv.GetWaitTime(), 0, 20*time.Second)
	if err != nil {
		return nil, err
	}
	visibility, err := between("receive.visibility_timeout", recv.GetVisibilityTimeout(), 0, 12*time.Hour)
	if err != nil {
		return nil, err
	}

	queue := asyncapi.NewMap[any]()
	queue.Set("name", name)
	bd := asyncapi.NewMap[any]()
	bd.Set("queues", []any{queue})
	bd.Set("bindingVersion", BindingVersion)
	x := asyncapi.NewMap[any]()
	if action == asyncapi.ActionSend {
		if send.GetMessageGroupId() != "" {
			x.Set("messageGroupId", send.GetMessageGroupId())
		}
		if send.GetDeduplicationId() != "" {
			x.Set("messageDeduplicationId", send.GetDeduplicationId())
		}
		if send.GetDelay() != "" {
			x.Set("delaySeconds", secs(delay))
		}
		if send.GetBatch() {
			x.Set("batch", true)
		}
	} else {
		if recv.GetMaxMessages() > 0 {
			x.Set("maxNumberOfMessages", recv.GetMaxMessages())
		}
		if recv.GetWaitTime() != "" {
			x.Set("waitTimeSeconds", secs(wait))
		}
		if recv.GetVisibilityTimeout() != "" {
			x.Set("visibilityTimeout", secs(visibility))
		}
		if recv.GetBatchDelete() {
			x.Set("batchDelete", true)
		}
	}
	if x.Len() > 0 {
		bd.Set(ExtensionKey, x)
	}
	return bd, nil
}

// addOutput adds the operation sending a processor's output: the opposite
// action of its input, on the output queue, with the send settings.
func (p *Protocol) addOutput(s *protogen.Service, svcOpt *sqsv1.Service, m *protogen.Method, opt *sqsv1.Operation, input string, inAction asyncapi.Action, output *protogen.Message) error {
	name := joinName(strings.TrimSuffix(input, ".fifo"), "output")
	if isFIFO(input) {
		name += ".fifo"
	}
	if opt.GetOutput() != "" {
		name = joinName(svcOpt.GetQueuePrefix(), opt.GetOutput())
	}
	ch, err := p.queueChannel(m.Desc, name, core.ChannelSpec{Servers: core.ChannelServers(s, m)})
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
	outOpt := proto.Clone(opt).(*sqsv1.Operation)
	outOpt.Receive = nil
	bd, err := p.operationBinding(outOpt, action, name, true)
	if err != nil {
		return core.Errorf(m.Desc, "output: %v", err)
	}
	_, err = p.b.AddOperation(core.OpSpec{
		Service:   s,
		Method:    m,
		DefaultID: string(s.Desc.Name()) + "." + string(m.Desc.Name()),
		IDSuffix:  ".output",
		Action:    action,
		Channel:   ch,
		Payload:   msg,
		Bindings:  asyncapi.NewBindings(BindingKey, bd),
	})
	return err
}
