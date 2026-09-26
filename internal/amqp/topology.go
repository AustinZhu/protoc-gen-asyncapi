package amqp

import (
	"fmt"
	"strings"
	"time"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	amqpv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/amqp/asyncapi/v1"
)

// builtinExchanges are the exchanges every RabbitMQ virtual host has.
var builtinExchanges = map[string]amqpv1.ExchangeType{
	"amq.direct":  amqpv1.ExchangeType_EXCHANGE_TYPE_DIRECT,
	"amq.fanout":  amqpv1.ExchangeType_EXCHANGE_TYPE_FANOUT,
	"amq.topic":   amqpv1.ExchangeType_EXCHANGE_TYPE_TOPIC,
	"amq.headers": amqpv1.ExchangeType_EXCHANGE_TYPE_HEADERS,
	"amq.match":   amqpv1.ExchangeType_EXCHANGE_TYPE_HEADERS,
}

// directReplyTo is RabbitMQ's direct reply-to pseudo-queue.
const directReplyTo = "amq.rabbitmq.reply-to"

// exchange is a declared, built-in or implicitly used exchange.
type exchange struct {
	name     string
	decl     *amqpv1.Exchange // nil when not declared
	typ      amqpv1.ExchangeType
	typeKnow bool
}

// queue is a declared or implicitly used queue.
type queue struct {
	name     string
	decl     *amqpv1.Queue // nil when not declared
	bindings []*amqpv1.Binding
}

type topology struct {
	exchanges map[string]*exchange
	queues    map[string]*queue
	xbindings []*amqpv1.ExchangeBinding
}

func newTopology() *topology {
	t := &topology{exchanges: map[string]*exchange{}, queues: map[string]*queue{}}
	for name, typ := range builtinExchanges {
		t.exchanges[name] = &exchange{name: name, typ: typ, typeKnow: true}
	}
	return t
}

// exchange returns an exchange, registering undeclared ones on first use.
func (t *topology) exchange(name string) *exchange {
	if x, ok := t.exchanges[name]; ok {
		return x
	}
	x := &exchange{name: name}
	t.exchanges[name] = x
	return x
}

// queue returns a queue, registering undeclared ones on first use.
func (t *topology) queue(name string) *queue {
	if q, ok := t.queues[name]; ok {
		return q
	}
	q := &queue{name: name}
	t.queues[name] = q
	return q
}

func (x *exchange) topic() bool {
	return x.typeKnow && x.typ == amqpv1.ExchangeType_EXCHANGE_TYPE_TOPIC
}

func (x *exchange) headers() bool {
	return x.typeKnow && x.typ == amqpv1.ExchangeType_EXCHANGE_TYPE_HEADERS
}

// checkName validates an exchange or queue name.
func checkName(kind, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%s name is required", kind)
	case len(name) > 255:
		return fmt.Errorf("%s name %q is longer than 255 bytes", kind, name)
	case strings.HasPrefix(name, "amq."):
		return fmt.Errorf("%s name %q uses the reserved amq. prefix", kind, name)
	case strings.ContainsAny(name, " \t\r\n"):
		return fmt.Errorf("%s name %q must not contain whitespace", kind, name)
	}
	return nil
}

func duration(field, value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration like \"30s\" or \"24h\"", field, value)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %q", field, value)
	}
	return d.Milliseconds(), nil
}

// collect declares the exchanges, queues and bindings of every document
// option, reporting every problem.
func (p *Protocol) collect() error {
	t := p.topo
	var errs []string
	fail := func(where, format string, args ...any) {
		errs = append(errs, where+": "+fmt.Sprintf(format, args...))
	}
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, x := range set.doc.GetExchanges() {
			w := fmt.Sprintf("%s: exchange %q", where, x.GetName())
			if err := checkName("exchange", x.GetName()); err != nil {
				fail(where, "%v", err)
				continue
			}
			if prev, ok := t.exchanges[x.GetName()]; ok && prev.decl != nil {
				fail(w, "is declared twice")
				continue
			}
			typ := x.GetType()
			if typ == amqpv1.ExchangeType_EXCHANGE_TYPE_UNSPECIFIED {
				typ = amqpv1.ExchangeType_EXCHANGE_TYPE_DIRECT
			}
			if err := checkArguments(x.GetArguments()); err != nil {
				fail(w, "arguments: %v", err)
			}
			t.exchanges[x.GetName()] = &exchange{name: x.GetName(), decl: x, typ: typ, typeKnow: true}
		}
		for _, q := range set.doc.GetQueues() {
			w := fmt.Sprintf("%s: queue %q", where, q.GetName())
			if err := checkName("queue", q.GetName()); err != nil {
				fail(where, "%v", err)
				continue
			}
			if prev, ok := t.queues[q.GetName()]; ok && prev.decl != nil {
				fail(w, "is declared twice")
				continue
			}
			if err := checkQueue(q); err != nil {
				fail(w, "%v", err)
			}
			t.queues[q.GetName()] = &queue{name: q.GetName(), decl: q, bindings: q.GetBindings()}
		}
		t.xbindings = append(t.xbindings, set.doc.GetExchangeBindings()...)
	}
	// References are checked once everything is declared.
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, x := range set.doc.GetExchanges() {
			if ae := x.GetAlternateExchange(); ae != "" && !t.known(ae) {
				fail(fmt.Sprintf("%s: exchange %q", where, x.GetName()), "alternate exchange %q is not declared", ae)
			}
		}
		for _, q := range set.doc.GetQueues() {
			w := fmt.Sprintf("%s: queue %q", where, q.GetName())
			if dlx := q.GetDeadLetterExchange(); dlx != "" && !t.known(dlx) {
				fail(w, "dead letter exchange %q is not declared", dlx)
			}
			for i, bd := range q.GetBindings() {
				if err := t.checkBinding(bd); err != nil {
					fail(w, "bindings[%d]: %v", i, err)
				}
			}
		}
		for i, xb := range set.doc.GetExchangeBindings() {
			for _, name := range []string{xb.GetSource(), xb.GetDestination()} {
				if !t.known(name) {
					fail(where, "exchange_bindings[%d]: exchange %q is not declared", i, name)
				}
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "\n"))
	}
	return nil
}

// known reports a declared or built-in exchange.
func (t *topology) known(name string) bool {
	x, ok := t.exchanges[name]
	return ok && (x.decl != nil || x.typeKnow)
}

func (t *topology) checkBinding(bd *amqpv1.Binding) error {
	if !t.known(bd.GetExchange()) {
		return fmt.Errorf("exchange %q is not declared", bd.GetExchange())
	}
	x := t.exchanges[bd.GetExchange()]
	if strings.ContainsAny(bd.GetRoutingKey(), "*#") && !x.topic() {
		return fmt.Errorf("routing key %q: wildcards need a topic exchange, %q is %s", bd.GetRoutingKey(), x.name, typeName(x.typ))
	}
	if (bd.GetMatch() != amqpv1.Match_MATCH_UNSPECIFIED || len(bd.GetHeaders()) > 0) && !x.headers() {
		return fmt.Errorf("match and headers need a headers exchange, %q is %s", x.name, typeName(x.typ))
	}
	return checkArguments(bd.GetHeaders())
}

func checkArguments(args map[string]string) error {
	for _, k := range core.SortedKeys(args) {
		if _, err := asyncapi.DecodeJSON([]byte(args[k])); err != nil {
			return fmt.Errorf("%q is not a JSON value (strings must be quoted): %v", k, err)
		}
	}
	return nil
}

func checkQueue(q *amqpv1.Queue) error {
	typ := q.GetType()
	switch {
	case q.GetMaxPriority() < 0 || q.GetMaxPriority() > 255:
		return fmt.Errorf("max_priority must be between 1 and 255")
	case q.GetMaxLength() < 0 || q.GetMaxLengthBytes() < 0 || q.GetDeliveryLimit() < 0:
		return fmt.Errorf("max_length, max_length_bytes and delivery_limit must be >= 0")
	case q.GetDeadLetterRoutingKey() != "" && q.GetDeadLetterExchange() == "":
		return fmt.Errorf("dead_letter_routing_key requires dead_letter_exchange")
	case q.GetMaxAge() != "" && typ != amqpv1.QueueType_QUEUE_TYPE_STREAM:
		return fmt.Errorf("max_age only applies to stream queues")
	case q.GetDeliveryLimit() != 0 && typ != amqpv1.QueueType_QUEUE_TYPE_QUORUM:
		return fmt.Errorf("delivery_limit only applies to quorum queues")
	case q.GetMaxPriority() != 0 && typ != amqpv1.QueueType_QUEUE_TYPE_UNSPECIFIED && typ != amqpv1.QueueType_QUEUE_TYPE_CLASSIC:
		return fmt.Errorf("max_priority only applies to classic queues")
	case (q.GetExclusive() || q.GetAutoDelete()) && (typ == amqpv1.QueueType_QUEUE_TYPE_QUORUM || typ == amqpv1.QueueType_QUEUE_TYPE_STREAM):
		return fmt.Errorf("%s queues cannot be exclusive or auto-delete", queueTypeName(typ))
	case typ == amqpv1.QueueType_QUEUE_TYPE_STREAM && q.GetOverflow() == amqpv1.Overflow_OVERFLOW_REJECT_PUBLISH_DLX:
		return fmt.Errorf("stream queues do not dead-letter")
	case typ == amqpv1.QueueType_QUEUE_TYPE_STREAM && q.GetDeadLetterExchange() != "":
		return fmt.Errorf("stream queues do not dead-letter")
	}
	for _, d := range [][2]string{{"message_ttl", q.GetMessageTtl()}, {"expires", q.GetExpires()}, {"max_age", q.GetMaxAge()}} {
		if _, err := duration(d[0], d[1]); err != nil {
			return err
		}
	}
	return checkArguments(q.GetArguments())
}

func typeName(t amqpv1.ExchangeType) string {
	if t == amqpv1.ExchangeType_EXCHANGE_TYPE_UNSPECIFIED {
		return "of unknown type"
	}
	return "a " + strings.ToLower(strings.TrimPrefix(t.String(), "EXCHANGE_TYPE_")) + " exchange"
}

func queueTypeName(t amqpv1.QueueType) string {
	if t == amqpv1.QueueType_QUEUE_TYPE_UNSPECIFIED {
		return "classic"
	}
	return strings.ToLower(strings.TrimPrefix(t.String(), "QUEUE_TYPE_"))
}
