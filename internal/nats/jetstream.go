package nats

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	natsv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/nats/asyncapi/v1"
)

// jetStream handles streams, key-value buckets, object stores and the
// binding of channels and operations to them.
//
// JetStream is not covered by the NATS AsyncAPI bindings, so the information
// is emitted as `x-nats-jetstream` specification extensions. Configuration
// objects use the field names and units of the JetStream JSON API
// (durations in nanoseconds), so they can be fed to `nats stream add
// --config` and friends.
type jetStream struct {
	p       *Protocol
	streams *asyncapi.Map[*streamInfo]
	kv      *asyncapi.Map[*natsv1.KeyValueBucket]
	objects *asyncapi.Map[*natsv1.ObjectStore]
	origin  map[string]string
}

type streamInfo struct {
	cfg      *natsv1.Stream
	subjects []string // explicit subjects
	derived  []string // subjects derived from bound channels
	where    string
	implicit bool // KV_/OBJ_ backing stream
}

func newJetStream(p *Protocol) *jetStream {
	return &jetStream{
		p:       p,
		streams: asyncapi.NewMap[*streamInfo](),
		kv:      asyncapi.NewMap[*natsv1.KeyValueBucket](),
		objects: asyncapi.NewMap[*natsv1.ObjectStore](),
		origin:  map[string]string{},
	}
}

func validStreamName(s string) bool {
	return s != "" && !strings.ContainsAny(s, " \t\r\n.*>/\\")
}

func (js *jetStream) collect() error {
	for _, set := range js.p.settings {
		where := set.file.Desc.Path()
		for _, s := range set.doc.GetStreams() {
			if !validStreamName(s.GetName()) {
				return fmt.Errorf("%s: invalid stream name %q (must not contain whitespace, '.', '*', '>', '/' or '\\')", where, s.GetName())
			}
			if err := js.claim("stream "+s.GetName(), where); err != nil {
				return err
			}
			for _, subj := range s.GetSubjects() {
				if _, err := parseSubject(subj); err != nil || strings.Contains(subj, "{") {
					return fmt.Errorf("%s: stream %q: invalid subject %q", where, s.GetName(), subj)
				}
			}
			if _, err := streamConfig(s, nil); err != nil {
				return fmt.Errorf("%s: stream %q: %w", where, s.GetName(), err)
			}
			js.streams.Set(s.GetName(), &streamInfo{cfg: s, subjects: s.GetSubjects(), where: where})
		}
		for _, kv := range set.doc.GetKvBuckets() {
			if !paramNameRE.MatchString(kv.GetBucket()) {
				return fmt.Errorf("%s: invalid key-value bucket name %q", where, kv.GetBucket())
			}
			if err := js.claim("key-value bucket "+kv.GetBucket(), where); err != nil {
				return err
			}
			if _, err := kvConfig(kv); err != nil {
				return fmt.Errorf("%s: key-value bucket %q: %w", where, kv.GetBucket(), err)
			}
			js.kv.Set(kv.GetBucket(), kv)
			js.streams.Set("KV_"+kv.GetBucket(), &streamInfo{subjects: []string{"$KV." + kv.GetBucket() + ".>"}, where: where, implicit: true})
		}
		for _, o := range set.doc.GetObjectStores() {
			if !paramNameRE.MatchString(o.GetBucket()) {
				return fmt.Errorf("%s: invalid object store name %q", where, o.GetBucket())
			}
			if err := js.claim("object store "+o.GetBucket(), where); err != nil {
				return err
			}
			if _, err := objectStoreConfig(o); err != nil {
				return fmt.Errorf("%s: object store %q: %w", where, o.GetBucket(), err)
			}
			js.objects.Set(o.GetBucket(), o)
			js.streams.Set("OBJ_"+o.GetBucket(), &streamInfo{
				subjects: []string{"$O." + o.GetBucket() + ".C.>", "$O." + o.GetBucket() + ".M.>"},
				where:    where, implicit: true,
			})
		}
	}
	return nil
}

func (js *jetStream) claim(what, where string) error {
	if prev, ok := js.origin[what]; ok {
		return fmt.Errorf("%s: %s is already declared in %s", where, what, prev)
	}
	js.origin[what] = where
	return nil
}

// bind resolves the stream of every channel and applies the JetStream
// specific settings of operations.
func (js *jetStream) bind() error {
	for _, ch := range js.p.b.Channels() {
		cd := js.p.chans[ch]
		if cd == nil || cd.subject == nil {
			continue
		}
		wildcard := cd.subject.wildcard()
		if cd.stream == "" {
			for _, name := range js.streams.Keys() {
				st, _ := js.streams.Get(name)
				if matchesAny(wildcard, st.subjects) {
					cd.stream = name
					break
				}
			}
		} else if st, ok := js.streams.Get(cd.stream); ok {
			switch {
			case len(st.subjects) == 0:
				if !containsString(st.derived, wildcard) {
					st.derived = append(st.derived, wildcard)
				}
			case !matchesAny(wildcard, st.subjects):
				return core.Errorf(ch.DeclaredBy, "subject %q is not captured by the subjects %q of stream %q", wildcard, st.subjects, cd.stream)
			}
		}
		if cd.stream != "" {
			x := asyncapi.NewMap[any]()
			x.Set("stream", cd.stream)
			if cd.kvBucket != "" {
				x.Set("keyValueBucket", cd.kvBucket)
			}
			if ch.Obj.Extensions == nil {
				ch.Obj.Extensions = asyncapi.Extensions{}
			}
			ch.Obj.Extensions["x-nats-jetstream"] = x
		}
	}

	for _, oi := range js.p.b.Ops() {
		if err := js.bindOperation(oi); err != nil {
			return err
		}
	}
	return nil
}

func matchesAny(subject string, filters []string) bool {
	for _, f := range filters {
		if subjectSubsetOf(subject, f) {
			return true
		}
	}
	return false
}

func (js *jetStream) bindOperation(oi *core.Op) error {
	opt, ok := js.p.ops[oi]
	if !ok {
		return nil // synthetic or other protocol's operation
	}
	m, ch, op := oi.Method, oi.Channel, oi.Obj
	cd := js.p.chans[ch]
	consumer, publish := opt.GetConsumer(), opt.GetPublish()
	if cd.stream == "" {
		if consumer != nil || publish != nil {
			return core.Errorf(m.Desc, "subject %q is not stored in a JetStream stream: declare a stream capturing it or set `stream`", *ch.Obj.Address)
		}
		return nil
	}
	var streamCfg *natsv1.Stream
	if st, ok := js.streams.Get(cd.stream); ok {
		streamCfg = st.cfg
	}

	if op.Action == asyncapi.ActionReceive && consumer != nil {
		// Queue groups only exist for push consumers (as deliver group);
		// pull consumers balance load by themselves.
		if consumer.GetKind() == natsv1.ConsumerKind_CONSUMER_KIND_PUSH {
			if consumer.GetDeliverGroup() == "" && queueOf(op.Bindings) != "" {
				consumer = proto.Clone(consumer).(*natsv1.Consumer)
				consumer.DeliverGroup = queueOf(op.Bindings)
			} else if consumer.GetDeliverGroup() != "" {
				op.Bindings = queueBinding(consumer.GetDeliverGroup())
			}
		} else {
			op.Bindings = nil
		}
		cfg, kind, err := consumerConfig(consumer, cd.subject.wildcard())
		if err != nil {
			return core.Errorf(m.Desc, "consumer: %v", err)
		}
		x := asyncapi.NewMap[any]()
		x.Set("stream", cd.stream)
		x.Set("consumerKind", kind)
		x.Set("consumer", cfg)
		if op.Extensions == nil {
			op.Extensions = asyncapi.Extensions{}
		}
		op.Extensions["x-nats-jetstream"] = x
	}
	// Headers set by JetStream publishers.
	// Messages may be shared by several operations, so these headers are
	// documented as optional.
	hdr := func(name, desc string) {
		oi.Payload.AddHeader(name, &asyncapi.Schema{Type: "string", Description: desc}, false)
	}
	requires := func(flag bool, setting string) error {
		if streamCfg != nil && !flag {
			return core.Errorf(m.Desc, "stream %q must set %s to use this publish option", cd.stream, setting)
		}
		return nil
	}
	if publish.GetMsgId() {
		hdr("Nats-Msg-Id", "Unique message ID set by JetStream publishers. JetStream discards messages with an ID already seen within the duplicate window of the stream.")
	}
	for _, e := range publish.GetExpect() {
		switch e {
		case natsv1.Expect_EXPECT_STREAM:
			hdr("Nats-Expected-Stream", "Name of the stream the message is expected to be stored in.")
		case natsv1.Expect_EXPECT_LAST_SEQUENCE:
			hdr("Nats-Expected-Last-Sequence", "Expected sequence of the last message in the stream (optimistic concurrency control).")
		case natsv1.Expect_EXPECT_LAST_SUBJECT_SEQUENCE:
			hdr("Nats-Expected-Last-Subject-Sequence", "Expected sequence of the last message on the subject (optimistic concurrency control).")
		case natsv1.Expect_EXPECT_LAST_MSG_ID:
			hdr("Nats-Expected-Last-Msg-Id", "Expected Nats-Msg-Id of the last message in the stream.")
		}
	}
	if publish.GetTtl() {
		if err := requires(streamCfg.GetAllowMsgTtl(), "allow_msg_ttl"); err != nil {
			return err
		}
		hdr("Nats-TTL", "Time to live of the message, e.g. `30s`, `1h` or `never` (NATS 2.11+).")
	}
	if publish.GetAtomicBatch() {
		if err := requires(streamCfg.GetAllowAtomic(), "allow_atomic"); err != nil {
			return err
		}
		hdr("Nats-Batch-Id", "Identifier of the atomic publish batch (NATS 2.12+).")
		hdr("Nats-Batch-Sequence", "Sequence of the message within the batch, starting at 1.")
		hdr("Nats-Batch-Commit", "Set to `1` on the last message to commit the batch.")
	}
	if publish.GetSchedule() {
		if err := requires(streamCfg.GetAllowMsgSchedules(), "allow_msg_schedules"); err != nil {
			return err
		}
		hdr("Nats-Schedule", "Schedule of the message, e.g. `@at 2030-01-01T00:00:00Z` or a cron expression (NATS 2.12+).")
		hdr("Nats-Schedule-Target", "Subject the scheduled message is published to.")
	}

	// JetStream publishes are requests answered with a publish ack.
	if op.Action == asyncapi.ActionSend && op.Reply == nil && (publish == nil || publish.Ack == nil || publish.GetAck()) {
		reply := js.p.b.ReplyChannel("jetstream.pubAck",
			"JetStream publish acknowledgements, delivered to the reply subject (inbox) of the publish request.")
		js.p.b.SetReply(oi, reply, js.pubAckMessage())
	}
	return nil
}

func (js *jetStream) pubAckMessage() *core.Message {
	props := asyncapi.NewMap[*asyncapi.Schema]()
	props.Set("stream", &asyncapi.Schema{Type: "string", Description: "Stream the message was stored in."})
	props.Set("seq", &asyncapi.Schema{Type: "integer", Minimum: 0, Description: "Sequence of the message in the stream."})
	props.Set("duplicate", &asyncapi.Schema{Type: "boolean", Description: "The message was a duplicate and was not stored again."})
	props.Set("domain", &asyncapi.Schema{Type: "string", Description: "JetStream domain."})
	props.Set("batch", &asyncapi.Schema{Type: "string", Description: "Batch identifier for atomic batch publishes."})
	props.Set("count", &asyncapi.Schema{Type: "integer", Description: "Number of messages committed by an atomic batch."})
	errProps := asyncapi.NewMap[*asyncapi.Schema]()
	errProps.Set("code", &asyncapi.Schema{Type: "integer"})
	errProps.Set("err_code", &asyncapi.Schema{Type: "integer"})
	errProps.Set("description", &asyncapi.Schema{Type: "string"})
	props.Set("error", &asyncapi.Schema{Type: "object", Description: "Set when the message was rejected.", Properties: errProps})
	return js.p.b.SyntheticMessage("nats.jetstream.PubAck", &asyncapi.Message{
		Name:  "PubAck",
		Title: "JetStream publish acknowledgement",
	}, &asyncapi.Schema{Type: "object", Properties: props})
}

// emit renders the root x-nats-jetstream extension.
func (js *jetStream) emit() {
	root := asyncapi.NewMap[any]()
	streams := asyncapi.NewMap[any]()
	for _, name := range js.streams.Keys() {
		st, _ := js.streams.Get(name)
		if st.implicit {
			continue
		}
		cfg, _ := streamConfig(st.cfg, st.derived)
		streams.Set(name, cfg)
	}
	if streams.Len() > 0 {
		root.Set("streams", streams)
	}
	if js.kv.Len() > 0 {
		kvs := asyncapi.NewMap[any]()
		for _, name := range js.kv.Keys() {
			kv, _ := js.kv.Get(name)
			cfg, _ := kvConfig(kv)
			kvs.Set(name, cfg)
		}
		root.Set("keyValueBuckets", kvs)
	}
	if js.objects.Len() > 0 {
		objs := asyncapi.NewMap[any]()
		for _, name := range js.objects.Keys() {
			o, _ := js.objects.Get(name)
			cfg, _ := objectStoreConfig(o)
			objs.Set(name, cfg)
		}
		root.Set("objectStores", objs)
	}
	if root.Len() > 0 {
		if js.p.b.Doc.Extensions == nil {
			js.p.b.Doc.Extensions = asyncapi.Extensions{}
		}
		js.p.b.Doc.Extensions["x-nats-jetstream"] = root
	}
}

// ---------------------------------------------------------------------------
// Configuration rendering
// ---------------------------------------------------------------------------

// config accumulates a JSON API configuration object, skipping zero values
// and remembering the first conversion error.
type config struct {
	m   *asyncapi.Map[any]
	err error
}

func newConfig() *config { return &config{m: asyncapi.NewMap[any]()} }

func (c *config) set(key string, v any) {
	switch x := v.(type) {
	case string:
		if x == "" {
			return
		}
	case bool:
		if !x {
			return
		}
	case int32:
		if x == 0 {
			return
		}
	case int64:
		if x == 0 {
			return
		}
	case uint64:
		if x == 0 {
			return
		}
	case []string:
		if len(x) == 0 {
			return
		}
	case map[string]string:
		if len(x) == 0 {
			return
		}
	case *asyncapi.Map[any]:
		if x == nil || x.Len() == 0 {
			return
		}
	case []any:
		if len(x) == 0 {
			return
		}
	case nil:
		return
	}
	c.m.Set(key, v)
}

func (c *config) duration(key, s string) {
	d, err := parseDuration(s)
	if err != nil {
		c.fail(key, err)
		return
	}
	c.set(key, int64(d))
}

func (c *config) size(key, s string) {
	n, err := parseSize(s)
	if err != nil {
		c.fail(key, err)
		return
	}
	c.set(key, n)
}

func (c *config) timestamp(key, s string) {
	if s == "" {
		return
	}
	if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
		c.fail(key, fmt.Errorf("invalid RFC 3339 timestamp %q", s))
		return
	}
	c.set(key, s)
}

func (c *config) fail(key string, err error) {
	if c.err == nil {
		c.err = fmt.Errorf("%s: %w", key, err)
	}
}

func enumName(v fmt.Stringer, prefix string) string {
	s := v.String()
	if strings.HasSuffix(s, "_UNSPECIFIED") {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(s, prefix))
}

func placementConfig(p *natsv1.Placement) *asyncapi.Map[any] {
	if p == nil {
		return nil
	}
	c := newConfig()
	c.set("cluster", p.GetCluster())
	c.set("tags", p.GetTags())
	c.set("preferred", p.GetPreferred())
	return c.m
}

func transformConfig(t *natsv1.SubjectTransform) *asyncapi.Map[any] {
	if t == nil {
		return nil
	}
	c := newConfig()
	c.set("src", t.GetSrc())
	c.set("dest", t.GetDest())
	return c.m
}

func republishConfig(r *natsv1.Republish) *asyncapi.Map[any] {
	if r == nil {
		return nil
	}
	c := newConfig()
	c.set("src", r.GetSrc())
	c.set("dest", r.GetDest())
	c.set("headers_only", r.GetHeadersOnly())
	return c.m
}

func sourceConfig(s *natsv1.StreamSource) (*asyncapi.Map[any], error) {
	if s == nil {
		return nil, nil
	}
	c := newConfig()
	c.set("name", s.GetName())
	c.set("opt_start_seq", s.GetOptStartSeq())
	c.timestamp("opt_start_time", s.GetOptStartTime())
	c.set("filter_subject", s.GetFilterSubject())
	var transforms []any
	for _, t := range s.GetSubjectTransforms() {
		transforms = append(transforms, transformConfig(t))
	}
	c.set("subject_transforms", transforms)
	api := s.GetExternalApiPrefix()
	if api == "" && s.GetDomain() != "" {
		api = "$JS." + s.GetDomain() + ".API"
	}
	if api != "" || s.GetExternalDeliverPrefix() != "" {
		ext := newConfig()
		ext.set("api", api)
		ext.set("deliver", s.GetExternalDeliverPrefix())
		c.set("external", ext.m)
	}
	return c.m, c.err
}

func streamConfig(s *natsv1.Stream, derived []string) (*asyncapi.Map[any], error) {
	c := newConfig()
	c.set("name", s.GetName())
	c.set("description", s.GetDescription())
	subjects := s.GetSubjects()
	if len(subjects) == 0 {
		subjects = derived
	}
	c.set("subjects", subjects)
	c.set("retention", strings.ReplaceAll(enumName(s.GetRetention(), "RETENTION_"), "_", ""))
	c.set("max_consumers", s.GetMaxConsumers())
	c.set("max_msgs", s.GetMaxMsgs())
	c.size("max_bytes", s.GetMaxBytes())
	c.duration("max_age", s.GetMaxAge())
	c.set("max_msgs_per_subject", s.GetMaxMsgsPerSubject())
	if s.GetMaxMsgSize() != "" {
		n, err := parseSize(s.GetMaxMsgSize())
		if err != nil {
			c.fail("max_msg_size", err)
		}
		c.set("max_msg_size", int32(n))
	}
	c.set("discard", enumName(s.GetDiscard(), "DISCARD_"))
	c.set("storage", enumName(s.GetStorage(), "STORAGE_"))
	c.set("num_replicas", s.GetReplicas())
	c.set("no_ack", s.GetNoAck())
	c.duration("duplicate_window", s.GetDuplicateWindow())
	c.set("placement", placementConfig(s.GetPlacement()))
	if m, err := sourceConfig(s.GetMirror()); err != nil {
		c.fail("mirror", err)
	} else {
		c.set("mirror", m)
	}
	var sources []any
	for _, src := range s.GetSources() {
		m, err := sourceConfig(src)
		if err != nil {
			c.fail("sources", err)
		}
		sources = append(sources, m)
	}
	c.set("sources", sources)
	c.set("compression", enumName(s.GetCompression(), "COMPRESSION_"))
	c.set("first_seq", s.GetFirstSeq())
	c.set("subject_transform", transformConfig(s.GetSubjectTransform()))
	c.set("republish", republishConfig(s.GetRepublish()))
	c.set("allow_direct", s.GetAllowDirect())
	c.set("mirror_direct", s.GetMirrorDirect())
	c.set("discard_new_per_subject", s.GetDiscardNewPerSubject())
	c.set("allow_rollup_hdrs", s.GetAllowRollupHdrs())
	c.set("deny_delete", s.GetDenyDelete())
	c.set("deny_purge", s.GetDenyPurge())
	c.set("allow_msg_ttl", s.GetAllowMsgTtl())
	c.duration("subject_delete_marker_ttl", s.GetSubjectDeleteMarkerTtl())
	c.set("allow_atomic", s.GetAllowAtomic())
	c.set("allow_msg_counter", s.GetAllowMsgCounter())
	c.set("allow_msg_schedules", s.GetAllowMsgSchedules())
	c.set("metadata", s.GetMetadata())
	if s.GetDiscardNewPerSubject() && s.GetDiscard() != natsv1.Discard_DISCARD_NEW {
		c.fail("discard_new_per_subject", fmt.Errorf("requires discard: DISCARD_NEW"))
	}
	if s.GetSubjectDeleteMarkerTtl() != "" && !s.GetAllowMsgTtl() {
		c.fail("subject_delete_marker_ttl", fmt.Errorf("requires allow_msg_ttl"))
	}
	return c.m, c.err
}

func consumerConfig(k *natsv1.Consumer, filter string) (*asyncapi.Map[any], string, error) {
	kind := enumName(k.GetKind(), "CONSUMER_KIND_")
	if kind == "" {
		kind = "pull"
	}
	c := newConfig()
	c.set("durable_name", k.GetDurable())
	c.set("name", k.GetName())
	c.set("description", k.GetDescription())
	c.set("deliver_policy", enumName(k.GetDeliverPolicy(), "DELIVER_POLICY_"))
	c.set("opt_start_seq", k.GetOptStartSeq())
	c.timestamp("opt_start_time", k.GetOptStartTime())
	c.set("ack_policy", enumName(k.GetAckPolicy(), "ACK_POLICY_"))
	c.duration("ack_wait", k.GetAckWait())
	c.set("max_deliver", k.GetMaxDeliver())
	var backoff []any
	for _, b := range k.GetBackoff() {
		d, err := parseDuration(b)
		if err != nil {
			c.fail("backoff", err)
		}
		backoff = append(backoff, int64(d))
	}
	c.set("backoff", backoff)
	filters := k.GetFilterSubjects()
	if len(filters) == 0 {
		filters = []string{filter}
	}
	for _, f := range filters {
		if _, err := parseSubject(f); err != nil {
			c.fail("filter_subjects", err)
		}
	}
	if len(filters) == 1 {
		c.set("filter_subject", filters[0])
	} else {
		c.set("filter_subjects", filters)
	}
	c.set("replay_policy", enumName(k.GetReplayPolicy(), "REPLAY_POLICY_"))
	c.set("rate_limit_bps", k.GetRateLimitBps())
	c.set("sample_freq", k.GetSampleFreq())
	c.set("max_waiting", k.GetMaxWaiting())
	c.set("max_ack_pending", k.GetMaxAckPending())
	c.set("headers_only", k.GetHeadersOnly())
	c.set("max_batch", k.GetMaxBatch())
	c.duration("max_expires", k.GetMaxExpires())
	c.size("max_bytes", k.GetMaxBytes())
	c.duration("inactive_threshold", k.GetInactiveThreshold())
	c.set("num_replicas", k.GetReplicas())
	c.set("mem_storage", k.GetMemStorage())
	c.set("deliver_subject", k.GetDeliverSubject())
	c.set("deliver_group", k.GetDeliverGroup())
	c.set("flow_control", k.GetFlowControl())
	c.duration("idle_heartbeat", k.GetIdleHeartbeat())
	c.set("priority_groups", k.GetPriorityGroups())
	c.set("priority_policy", enumName(k.GetPriorityPolicy(), "PRIORITY_POLICY_"))
	c.duration("priority_timeout", k.GetPriorityTimeout())
	c.timestamp("pause_until", k.GetPauseUntil())
	c.set("metadata", k.GetMetadata())

	switch {
	case k.GetKind() == natsv1.ConsumerKind_CONSUMER_KIND_ORDERED && k.GetDurable() != "":
		c.fail("durable", fmt.Errorf("ordered consumers are ephemeral and cannot be durable"))
	case k.GetKind() == natsv1.ConsumerKind_CONSUMER_KIND_PUSH && k.GetMaxWaiting() != 0:
		c.fail("max_waiting", fmt.Errorf("only valid for pull consumers"))
	case k.GetKind() != natsv1.ConsumerKind_CONSUMER_KIND_PUSH && (k.GetDeliverSubject() != "" || k.GetDeliverGroup() != "" || k.GetFlowControl()):
		c.fail("deliver_subject", fmt.Errorf("deliver_subject, deliver_group and flow_control require kind: CONSUMER_KIND_PUSH"))
	case k.GetDeliverPolicy() == natsv1.DeliverPolicy_DELIVER_POLICY_BY_START_SEQUENCE && k.GetOptStartSeq() == 0:
		c.fail("opt_start_seq", fmt.Errorf("required by DELIVER_POLICY_BY_START_SEQUENCE"))
	case k.GetDeliverPolicy() == natsv1.DeliverPolicy_DELIVER_POLICY_BY_START_TIME && k.GetOptStartTime() == "":
		c.fail("opt_start_time", fmt.Errorf("required by DELIVER_POLICY_BY_START_TIME"))
	case len(k.GetPriorityGroups()) > 0 && k.GetKind() == natsv1.ConsumerKind_CONSUMER_KIND_PUSH:
		c.fail("priority_groups", fmt.Errorf("only valid for pull consumers"))
	}
	return c.m, kind, c.err
}

func kvConfig(kv *natsv1.KeyValueBucket) (*asyncapi.Map[any], error) {
	c := newConfig()
	c.set("bucket", kv.GetBucket())
	c.set("description", kv.GetDescription())
	if kv.GetMaxValueSize() != "" {
		n, err := parseSize(kv.GetMaxValueSize())
		if err != nil {
			c.fail("max_value_size", err)
		}
		c.set("max_value_size", int32(n))
	}
	if h := kv.GetHistory(); h < 0 || h > 64 {
		c.fail("history", fmt.Errorf("must be between 1 and 64"))
	}
	c.set("history", kv.GetHistory())
	c.duration("ttl", kv.GetTtl())
	c.size("max_bytes", kv.GetMaxBytes())
	c.set("storage", enumName(kv.GetStorage(), "STORAGE_"))
	c.set("replicas", kv.GetReplicas())
	c.set("placement", placementConfig(kv.GetPlacement()))
	c.set("republish", republishConfig(kv.GetRepublish()))
	if m, err := sourceConfig(kv.GetMirror()); err != nil {
		c.fail("mirror", err)
	} else {
		c.set("mirror", m)
	}
	var sources []any
	for _, src := range kv.GetSources() {
		m, err := sourceConfig(src)
		if err != nil {
			c.fail("sources", err)
		}
		sources = append(sources, m)
	}
	c.set("sources", sources)
	c.set("compression", kv.GetCompression())
	c.duration("limit_marker_ttl", kv.GetLimitMarkerTtl())
	c.set("metadata", kv.GetMetadata())
	c.set("stream", "KV_"+kv.GetBucket())
	return c.m, c.err
}

func objectStoreConfig(o *natsv1.ObjectStore) (*asyncapi.Map[any], error) {
	c := newConfig()
	c.set("bucket", o.GetBucket())
	c.set("description", o.GetDescription())
	c.duration("ttl", o.GetTtl())
	c.size("max_bytes", o.GetMaxBytes())
	c.set("storage", enumName(o.GetStorage(), "STORAGE_"))
	c.set("replicas", o.GetReplicas())
	c.set("placement", placementConfig(o.GetPlacement()))
	c.set("compression", o.GetCompression())
	c.set("metadata", o.GetMetadata())
	c.set("stream", "OBJ_"+o.GetBucket())
	return c.m, c.err
}
