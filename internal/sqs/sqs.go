// Package sqs documents Amazon SQS APIs described by Protobuf services:
// standard and FIFO queues, dead-letter queues, senders and receivers.
package sqs

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
	sqsv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/sqs/asyncapi/v1"
)

// BindingKey is the key of the official SQS bindings.
const BindingKey = "sqs"

// BindingVersion is the version of the official SQS bindings.
const BindingVersion = "0.2.0"

// ExtensionKey holds SQS details the official bindings do not define (or
// reserve, for servers).
const ExtensionKey = "x-sqs"

const emptyFullName = "google.protobuf.Empty"

var (
	paramRE     = regexp.MustCompile(`\{([^{}]*)\}`)
	paramNameRE = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)
	queueRE     = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	unsafeIDRE  = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
)

// Protocol implements core.Protocol for Amazon SQS.
type Protocol struct {
	// Per document state, reset by Begin.
	b         *core.Builder
	settings  []*fileSettings
	region    string
	accountID string
	queues    map[string]*sqsv1.Queue
	chans     map[*core.Channel]string // queue name
	order     []*core.Channel
}

type fileSettings struct {
	file *protogen.File
	doc  *sqsv1.Document
}

// New returns the SQS protocol.
func New() core.Protocol { return &Protocol{} }

func (p *Protocol) Name() string { return "sqs" }

func (p *Protocol) Options() []core.Option { return nil }

func documentOptions(f *protogen.File) *sqsv1.Document {
	return core.Extension[*sqsv1.Document](f.Desc.Options(), sqsv1.E_Document)
}

func serviceOptions(s *protogen.Service) *sqsv1.Service {
	return core.Extension[*sqsv1.Service](s.Desc.Options(), sqsv1.E_Service)
}

func operationOptions(m *protogen.Method) *sqsv1.Operation {
	return core.Extension[*sqsv1.Operation](m.Desc.Options(), sqsv1.E_Operation)
}

func messageOptions(m *protogen.Message) *sqsv1.Message {
	if m == nil {
		return nil
	}
	return core.Extension[*sqsv1.Message](m.Desc.Options(), sqsv1.E_Message)
}

// Claims documents services with SQS annotations.
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

// HasContent reports messages with their own queue, or SQS document
// options in the documented files.
func (p *Protocol) HasContent(b *core.Builder) bool {
	for _, f := range b.Files {
		if documentOptions(f) != nil {
			return true
		}
		found := false
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if messageOptions(m).GetQueue() != "" {
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
	p.region, p.accountID = "", ""
	p.queues = map[string]*sqsv1.Queue{}
	p.chans = map[*core.Channel]string{}
	p.order = nil
	for _, f := range b.Imports() {
		d := documentOptions(f)
		if d == nil {
			continue
		}
		p.settings = append(p.settings, &fileSettings{file: f, doc: d})
		for _, v := range []struct {
			dst  *string
			val  string
			what string
		}{{&p.region, d.GetRegion(), "region"}, {&p.accountID, d.GetAccountId(), "account_id"}} {
			if v.val == "" {
				continue
			}
			if *v.dst != "" && *v.dst != v.val {
				return fmt.Errorf("%s: %s %q differs from %s %q of another file of the document", f.Desc.Path(), v.what, v.val, v.what, *v.dst)
			}
			*v.dst = v.val
		}
	}
	if err := p.serverDetails(); err != nil {
		return err
	}
	var errs []string
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, q := range set.doc.GetQueues() {
			if _, err := parseQueue(q.GetName()); err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", where, err))
				continue
			}
			if _, dup := p.queues[q.GetName()]; dup {
				errs = append(errs, fmt.Sprintf("%s: queue %q is declared twice", where, q.GetName()))
				continue
			}
			p.queues[q.GetName()] = q
		}
	}
	for _, set := range p.settings {
		for _, q := range set.doc.GetQueues() {
			if err := p.checkQueue(q); err != nil {
				errs = append(errs, fmt.Sprintf("%s: queue %q: %v", set.file.Desc.Path(), q.GetName(), err))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "\n"))
	}
	// Declared queues are documented even when no rpc uses them.
	for _, set := range p.settings {
		if !b.OwnFile(set.file) {
			continue
		}
		for _, q := range set.doc.GetQueues() {
			if _, err := p.queueChannel(set.file.Desc, q.GetName(), core.ChannelSpec{}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Protocol) serverDetails() error {
	for _, set := range p.settings {
		for _, srv := range set.doc.GetServers() {
			out, ok := p.b.Doc.Servers.Get(srv.GetName())
			if !ok {
				return fmt.Errorf("%s: sqs server %q is not declared in the servers of the asyncapi.v3.document file option", set.file.Desc.Path(), srv.GetName())
			}
			x := asyncapi.NewMap[any]()
			if r := core.FirstNonEmpty(srv.GetRegion(), p.region); r != "" {
				x.Set("region", r)
			}
			if a := core.FirstNonEmpty(srv.GetAccountId(), p.accountID); a != "" {
				x.Set("accountId", a)
			}
			if x.Len() == 0 {
				continue
			}
			if out.Bindings == nil {
				out.Bindings = &asyncapi.Bindings{}
			}
			out.Bindings.Set(ExtensionKey, x)
		}
	}
	return nil
}

// queueName is a parsed queue name template.
type queueName struct {
	raw    string
	params []string
	fifo   bool
}

// parseQueue validates a queue name template.
func parseQueue(name string) (*queueName, error) {
	q := &queueName{raw: name, fifo: strings.HasSuffix(name, ".fifo")}
	seen := map[string]bool{}
	for _, m := range paramRE.FindAllStringSubmatch(name, -1) {
		if !paramNameRE.MatchString(m[1]) {
			return nil, fmt.Errorf("queue %q: invalid parameter name %q (allowed: letters, digits, '_' and '-')", name, m[1])
		}
		if seen[m[1]] {
			return nil, fmt.Errorf("queue %q: parameter %q is used twice", name, m[1])
		}
		seen[m[1]] = true
		q.params = append(q.params, m[1])
	}
	plain := paramRE.ReplaceAllString(name, "p")
	base := strings.TrimSuffix(plain, ".fifo")
	switch {
	case strings.ContainsAny(plain, "{}"):
		return nil, fmt.Errorf("queue %q has unbalanced braces", name)
	case !queueRE.MatchString(base) || len(plain) > 80:
		return nil, fmt.Errorf("queue %q must be up to 80 letters, digits, '-' and '_', with a \".fifo\" suffix for FIFO queues", name)
	}
	return q, nil
}

func isFIFO(name string) bool { return strings.HasSuffix(name, ".fifo") }

// between parses a duration and checks its range.
func between(field, value string, min, max time.Duration) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration like \"30s\" or \"96h\"", field, value)
	}
	if d < min || d > max {
		return 0, fmt.Errorf("%s must be between %s and %s, got %q", field, min, max, value)
	}
	return d, nil
}

func secs(d time.Duration) int64 { return int64(d / time.Second) }

const day = 24 * time.Hour

func (p *Protocol) checkQueue(q *sqsv1.Queue) error {
	fifo := isFIFO(q.GetName())
	switch {
	case !fifo && (q.GetContentBasedDeduplication() || q.GetDeduplicationScope() != sqsv1.DeduplicationScope_DEDUPLICATION_SCOPE_UNSPECIFIED ||
		q.GetFifoThroughputLimit() != sqsv1.FifoThroughputLimit_FIFO_THROUGHPUT_LIMIT_UNSPECIFIED):
		return fmt.Errorf("content_based_deduplication, deduplication_scope and fifo_throughput_limit only apply to FIFO queues (names ending with \".fifo\")")
	case q.GetFifoThroughputLimit() == sqsv1.FifoThroughputLimit_FIFO_THROUGHPUT_LIMIT_PER_MESSAGE_GROUP_ID &&
		q.GetDeduplicationScope() != sqsv1.DeduplicationScope_DEDUPLICATION_SCOPE_MESSAGE_GROUP:
		return fmt.Errorf("fifo_throughput_limit PER_MESSAGE_GROUP_ID requires deduplication_scope MESSAGE_GROUP")
	case q.GetMaximumMessageSize() != 0 && (q.GetMaximumMessageSize() < 1024 || q.GetMaximumMessageSize() > 1<<20):
		return fmt.Errorf("maximum_message_size must be between 1 KiB and 1 MiB")
	case q.GetMaxReceiveCount() != 0 && q.GetDeadLetterQueue() == "":
		return fmt.Errorf("max_receive_count requires dead_letter_queue")
	case q.GetMaxReceiveCount() < 0 || q.GetMaxReceiveCount() > 1000:
		return fmt.Errorf("max_receive_count must be between 1 and 1000")
	case q.GetDeadLetterQueue() == q.GetName():
		return fmt.Errorf("a queue cannot be its own dead-letter queue")
	case q.GetSqsManagedSse() && q.GetKmsKey() != "":
		return fmt.Errorf("sqs_managed_sse and kms_key are mutually exclusive")
	case q.GetKmsDataKeyReuse() != "" && q.GetKmsKey() == "":
		return fmt.Errorf("kms_data_key_reuse requires kms_key")
	}
	for _, d := range []struct {
		field, value string
		min, max     time.Duration
	}{
		{"delivery_delay", q.GetDeliveryDelay(), 0, 15 * time.Minute},
		{"visibility_timeout", q.GetVisibilityTimeout(), 0, 12 * time.Hour},
		{"receive_wait_time", q.GetReceiveWaitTime(), 0, 20 * time.Second},
		{"message_retention", q.GetMessageRetention(), time.Minute, 14 * day},
		{"kms_data_key_reuse", q.GetKmsDataKeyReuse(), time.Minute, day},
	} {
		if _, err := between(d.field, d.value, d.min, d.max); err != nil {
			return err
		}
	}
	if dlq := q.GetDeadLetterQueue(); dlq != "" {
		if _, err := parseQueue(dlq); err != nil {
			return fmt.Errorf("dead_letter_queue: %v", err)
		}
		if isFIFO(dlq) != fifo {
			return fmt.Errorf("dead_letter_queue %q must be a %s queue like the queue itself", dlq, map[bool]string{true: "FIFO", false: "standard"}[fifo])
		}
	}
	ra := q.GetRedriveAllow()
	switch {
	case ra == nil:
	case ra.GetPermission() == sqsv1.RedrivePermission_REDRIVE_PERMISSION_UNSPECIFIED:
		return fmt.Errorf("redrive_allow.permission is required")
	case ra.GetPermission() == sqsv1.RedrivePermission_REDRIVE_PERMISSION_BY_QUEUE && (len(ra.GetSourceQueues()) == 0 || len(ra.GetSourceQueues()) > 10):
		return fmt.Errorf("redrive_allow BY_QUEUE needs 1 to 10 source_queues")
	case ra.GetPermission() != sqsv1.RedrivePermission_REDRIVE_PERMISSION_BY_QUEUE && len(ra.GetSourceQueues()) > 0:
		return fmt.Errorf("redrive_allow.source_queues only apply to permission BY_QUEUE")
	}
	for i, st := range q.GetPolicy() {
		switch {
		case st.GetEffect() == sqsv1.Effect_EFFECT_UNSPECIFIED:
			return fmt.Errorf("policy[%d]: effect is required", i)
		case len(st.GetPrincipals()) == 0 || len(st.GetActions()) == 0:
			return fmt.Errorf("policy[%d]: principals and actions are required", i)
		}
		for _, a := range st.GetActions() {
			if a != "*" && !strings.HasPrefix(a, "sqs:") {
				return fmt.Errorf("policy[%d]: action %q is not an SQS action such as \"sqs:SendMessage\"", i, a)
			}
		}
	}
	return nil
}

// arn returns the ARN of a queue, when the region and account are known.
func (p *Protocol) arn(name string) string {
	if p.region == "" || p.accountID == "" {
		return ""
	}
	return "arn:aws:sqs:" + p.region + ":" + p.accountID + ":" + name
}

// queueChannel returns the channel of a queue.
func (p *Protocol) queueChannel(d protoreflect.Descriptor, name string, spec core.ChannelSpec) (*core.Channel, error) {
	qn, err := parseQueue(name)
	if err != nil {
		return nil, core.Errorf(d, "%v", err)
	}
	if q := p.queues[name]; q.GetDescription() != "" {
		spec.Meta = append(spec.Meta, &asyncapiv3.Channel{Description: q.GetDescription()})
	}
	spec.DeclaredBy, spec.Address, spec.Params = d, name, qn.params
	spec.DefaultID = strings.Trim(unsafeIDRE.ReplaceAllString(strings.NewReplacer("{", "", "}", "").Replace(name), "_"), "._")
	spec.Key = "sqs\x00" + name
	ch, err := p.b.Channel(spec)
	if err != nil {
		return nil, err
	}
	if _, ok := p.chans[ch]; !ok {
		p.chans[ch] = name
		p.order = append(p.order, ch)
	}
	return ch, nil
}

func (p *Protocol) Finish(b *core.Builder) error {
	// Messages with their own queue get a channel even without operation.
	for _, f := range b.Files {
		var list []*protogen.Message
		core.WalkMessages(f.Messages, func(m *protogen.Message) {
			if messageOptions(m).GetQueue() != "" {
				list = append(list, m)
			}
		})
		for _, m := range list {
			meta := core.MessageOptions(m).GetChannel()
			ch, err := p.queueChannel(m.Desc, messageOptions(m).GetQueue(), core.ChannelSpec{Meta: metas(meta), Servers: meta.GetServers()})
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
	}
	return nil
}

// queueObject renders a queue as the official bindings describe it.
func (p *Protocol) queueObject(name string) *asyncapi.Map[any] {
	q := p.queues[name]
	m := asyncapi.NewMap[any]()
	m.Set("name", name)
	m.Set("fifoQueue", isFIFO(name))
	switch q.GetDeduplicationScope() {
	case sqsv1.DeduplicationScope_DEDUPLICATION_SCOPE_QUEUE:
		m.Set("deduplicationScope", "queue")
	case sqsv1.DeduplicationScope_DEDUPLICATION_SCOPE_MESSAGE_GROUP:
		m.Set("deduplicationScope", "messageGroup")
	}
	switch q.GetFifoThroughputLimit() {
	case sqsv1.FifoThroughputLimit_FIFO_THROUGHPUT_LIMIT_PER_QUEUE:
		m.Set("fifoThroughputLimit", "perQueue")
	case sqsv1.FifoThroughputLimit_FIFO_THROUGHPUT_LIMIT_PER_MESSAGE_GROUP_ID:
		m.Set("fifoThroughputLimit", "perMessageGroupId")
	}
	for _, d := range []struct{ key, value string }{
		{"deliveryDelay", q.GetDeliveryDelay()}, {"visibilityTimeout", q.GetVisibilityTimeout()},
		{"receiveMessageWaitTime", q.GetReceiveWaitTime()}, {"messageRetentionPeriod", q.GetMessageRetention()},
	} {
		if d.value != "" {
			v, _ := time.ParseDuration(d.value) // validated by Begin
			m.Set(d.key, secs(v))
		}
	}
	if dlq := q.GetDeadLetterQueue(); dlq != "" {
		id := asyncapi.NewMap[any]()
		if arn := p.arn(dlq); arn != "" {
			id.Set("arn", arn)
		} else {
			id.Set("name", dlq)
		}
		rp := asyncapi.NewMap[any]()
		rp.Set("deadLetterQueue", id)
		count := q.GetMaxReceiveCount()
		if count == 0 {
			count = 10
		}
		rp.Set("maxReceiveCount", count)
		m.Set("redrivePolicy", rp)
	}
	if len(q.GetPolicy()) > 0 {
		var statements []any
		for _, st := range q.GetPolicy() {
			s := asyncapi.NewMap[any]()
			s.Set("effect", map[sqsv1.Effect]string{sqsv1.Effect_EFFECT_ALLOW: "Allow", sqsv1.Effect_EFFECT_DENY: "Deny"}[st.GetEffect()])
			s.Set("principal", oneOrMany(st.GetPrincipals()))
			s.Set("action", oneOrMany(st.GetActions()))
			statements = append(statements, s)
		}
		pol := asyncapi.NewMap[any]()
		pol.Set("statements", statements)
		m.Set("policy", pol)
	}
	if len(q.GetTags()) > 0 {
		tags := asyncapi.NewMap[any]()
		for _, k := range core.SortedKeys(q.GetTags()) {
			tags.Set(k, q.GetTags()[k])
		}
		m.Set("tags", tags)
	}
	return m
}

func oneOrMany(list []string) any {
	if len(list) == 1 {
		return list[0]
	}
	return list
}

// emitChannel renders the official channel binding of a queue, with the
// attributes it does not define under x-sqs.
func (p *Protocol) emitChannel(ch *core.Channel) {
	name := p.chans[ch]
	q := p.queues[name]
	bd := asyncapi.NewMap[any]()
	bd.Set("queue", p.queueObject(name))
	if dlq := q.GetDeadLetterQueue(); dlq != "" {
		bd.Set("deadLetterQueue", p.queueObject(dlq))
	}
	bd.Set("bindingVersion", BindingVersion)
	x := asyncapi.NewMap[any]()
	if p.region != "" && p.accountID != "" && !strings.Contains(name, "{") {
		x.Set("queueUrl", "https://sqs."+p.region+".amazonaws.com/"+p.accountID+"/"+name)
		x.Set("arn", p.arn(name))
	}
	if q.GetContentBasedDeduplication() {
		x.Set("contentBasedDeduplication", true)
	}
	if q.GetMaximumMessageSize() != 0 {
		x.Set("maximumMessageSize", q.GetMaximumMessageSize())
	}
	if q.GetSqsManagedSse() {
		x.Set("sqsManagedSseEnabled", true)
	}
	if q.GetKmsKey() != "" {
		x.Set("kmsMasterKeyId", q.GetKmsKey())
	}
	if q.GetKmsDataKeyReuse() != "" {
		v, _ := time.ParseDuration(q.GetKmsDataKeyReuse())
		x.Set("kmsDataKeyReusePeriodSeconds", secs(v))
	}
	if ra := q.GetRedriveAllow(); ra != nil {
		r := asyncapi.NewMap[any]()
		r.Set("redrivePermission", map[sqsv1.RedrivePermission]string{
			sqsv1.RedrivePermission_REDRIVE_PERMISSION_ALLOW_ALL: "allowAll",
			sqsv1.RedrivePermission_REDRIVE_PERMISSION_DENY_ALL:  "denyAll",
			sqsv1.RedrivePermission_REDRIVE_PERMISSION_BY_QUEUE:  "byQueue",
		}[ra.GetPermission()])
		if len(ra.GetSourceQueues()) > 0 {
			var sources []string
			for _, s := range ra.GetSourceQueues() {
				sources = append(sources, core.FirstNonEmpty(p.arn(s), s))
			}
			r.Set("sourceQueueArns", sources)
		}
		x.Set("redriveAllowPolicy", r)
	}
	if x.Len() > 0 {
		bd.Set(ExtensionKey, x)
	}
	if ch.Obj.Bindings == nil {
		ch.Obj.Bindings = &asyncapi.Bindings{}
	}
	ch.Obj.Bindings.Set(BindingKey, bd)
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
