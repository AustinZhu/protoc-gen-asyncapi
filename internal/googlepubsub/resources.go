package googlepubsub

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	pubsubv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/googlepubsub/asyncapi/v1"
)

var (
	paramRE     = regexp.MustCompile(`\{([^{}]*)\}`)
	paramNameRE = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)
	// resourceIDRE is the rule for topic and subscription IDs.
	resourceIDRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9\-_.~+%]{2,254}$`)
	tableRE      = regexp.MustCompile(`^[A-Za-z0-9\-_.]+[.:][A-Za-z0-9_]+\.[A-Za-z0-9_\-$]+$`)
	unsafeIDRE   = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
	labelRE      = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
)

// topic is a declared or used topic.
type topic struct {
	name string
	decl *pubsubv1.Topic // nil when not declared
	subs []*subscription
}

// subscription is a declared or used subscription.
type subscription struct {
	name  string
	topic string
	decl  *pubsubv1.Subscription // nil when not declared
}

// export reports BigQuery and Cloud Storage subscriptions, which have no
// subscribers.
func (s *subscription) export() bool {
	return s.decl.GetBigquery() != nil || s.decl.GetCloudStorage() != nil
}

func (s *subscription) delivery() string {
	switch {
	case s.decl.GetPush() != nil:
		return "push"
	case s.decl.GetBigquery() != nil:
		return "bigquery"
	case s.decl.GetCloudStorage() != nil:
		return "cloudStorage"
	}
	return "pull"
}

// parseID validates a topic or subscription ID template and returns its
// parameters.
func parseID(kind, id string) ([]string, error) {
	var params []string
	seen := map[string]bool{}
	for _, m := range paramRE.FindAllStringSubmatch(id, -1) {
		name := m[1]
		if !paramNameRE.MatchString(name) {
			return nil, fmt.Errorf("%s %q: invalid parameter name %q (allowed: letters, digits, '_' and '-')", kind, id, name)
		}
		if seen[name] {
			return nil, fmt.Errorf("%s %q: parameter %q is used twice", kind, id, name)
		}
		seen[name] = true
		params = append(params, name)
	}
	plain := paramRE.ReplaceAllString(id, "p")
	switch {
	case strings.ContainsAny(plain, "{}"):
		return nil, fmt.Errorf("%s %q has unbalanced braces", kind, id)
	case !resourceIDRE.MatchString(plain):
		return nil, fmt.Errorf("%s %q must be 3 to 255 letters, digits and \"-_.~+%%\", starting with a letter", kind, id)
	case strings.HasPrefix(strings.ToLower(plain), "goog"):
		return nil, fmt.Errorf("%s %q must not start with \"goog\"", kind, id)
	}
	return params, nil
}

func duration(field, value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration like \"10s\" or \"168h\"", field, value)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %q", field, value)
	}
	return d, nil
}

// between parses a duration and checks its range.
func between(field, value string, min, max time.Duration) (time.Duration, error) {
	d, err := duration(field, value)
	if err != nil || value == "" {
		return d, err
	}
	if d < min || d > max {
		return 0, fmt.Errorf("%s must be between %s and %s, got %q", field, human(min), human(max), value)
	}
	return d, nil
}

func human(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	}
	return d.String()
}

// seconds renders a duration the way the Pub/Sub API does, e.g. "600s".
func seconds(d time.Duration) string { return fmt.Sprintf("%ds", int64(d/time.Second)) }

const day = 24 * time.Hour

// collect declares the topics and subscriptions of every document option,
// reporting every problem.
func (p *Protocol) collect() error {
	var errs []string
	fail := func(where, format string, args ...any) {
		errs = append(errs, where+": "+fmt.Sprintf(format, args...))
	}
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, t := range set.doc.GetTopics() {
			w := fmt.Sprintf("%s: topic %q", where, t.GetName())
			if _, err := parseID("topic", t.GetName()); err != nil {
				fail(where, "%v", err)
				continue
			}
			if prev, ok := p.topics[t.GetName()]; ok && prev.decl != nil {
				fail(w, "is declared twice")
				continue
			}
			if _, err := between("message_retention_duration", t.GetMessageRetentionDuration(), 10*time.Minute, 31*day); err != nil {
				fail(w, "%v", err)
			}
			if t.GetEnforceInTransit() && len(t.GetAllowedPersistenceRegions()) == 0 {
				fail(w, "enforce_in_transit requires allowed_persistence_regions")
			}
			if s := t.GetSchema(); s != nil && s.GetName() == "" {
				fail(w, "schema.name is required")
			}
			if err := checkLabels(t.GetLabels()); err != nil {
				fail(w, "%v", err)
			}
			p.topic(t.GetName()).decl = t
		}
	}
	for _, set := range p.settings {
		where := set.file.Desc.Path()
		for _, s := range set.doc.GetSubscriptions() {
			w := fmt.Sprintf("%s: subscription %q", where, s.GetName())
			if _, err := parseID("subscription", s.GetName()); err != nil {
				fail(where, "%v", err)
				continue
			}
			if prev, ok := p.subs[s.GetName()]; ok && prev.decl != nil {
				fail(w, "is declared twice")
				continue
			}
			if err := p.checkSubscription(s); err != nil {
				fail(w, "%v", err)
			}
			sub := &subscription{name: s.GetName(), topic: s.GetTopic(), decl: s}
			p.subs[s.GetName()] = sub
			t := p.topic(s.GetTopic())
			t.subs = append(t.subs, sub)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "\n"))
	}
	return nil
}

// topicSchema returns the schema settings of a declared topic, or nil.
func (p *Protocol) topicSchema(name string) *pubsubv1.SchemaSettings {
	if t, ok := p.topics[name]; ok {
		return t.decl.GetSchema()
	}
	return nil
}

// topic returns a topic, registering undeclared ones on first use.
func (p *Protocol) topic(name string) *topic {
	t, ok := p.topics[name]
	if !ok {
		t = &topic{name: name}
		p.topics[name] = t
	}
	return t
}

func checkLabels(labels map[string]string) error {
	for _, k := range core.SortedKeys(labels) {
		if !labelRE.MatchString(k) {
			return fmt.Errorf("label %q must be lowercase letters, digits, '_' and '-', starting with a letter", k)
		}
	}
	return nil
}

func (p *Protocol) checkSubscription(s *pubsubv1.Subscription) error {
	if s.GetTopic() == "" {
		return fmt.Errorf("topic is required")
	}
	if _, err := parseID("topic", s.GetTopic()); err != nil {
		return err
	}
	if _, err := between("ack_deadline", s.GetAckDeadline(), 10*time.Second, 600*time.Second); err != nil {
		return err
	}
	if _, err := between("message_retention_duration", s.GetMessageRetentionDuration(), 10*time.Minute, 7*day); err != nil {
		return err
	}
	if e := s.GetExpiration(); e != "" && e != "never" {
		if _, err := between("expiration", e, day, 365*day); err != nil {
			return err
		}
	}
	if len(s.GetFilter()) > 256 {
		return fmt.Errorf("filter is longer than 256 bytes")
	}
	if err := checkLabels(s.GetLabels()); err != nil {
		return err
	}
	if dl := s.GetDeadLetterPolicy(); dl != nil {
		if dl.GetTopic() == "" {
			return fmt.Errorf("dead_letter_policy.topic is required")
		}
		if dl.GetTopic() == s.GetTopic() {
			return fmt.Errorf("dead_letter_policy.topic must differ from the subscription's topic")
		}
		if n := dl.GetMaxDeliveryAttempts(); n != 0 && (n < 5 || n > 100) {
			return fmt.Errorf("dead_letter_policy.max_delivery_attempts must be between 5 and 100")
		}
	}
	if rp := s.GetRetryPolicy(); rp != nil {
		minB, err := between("retry_policy.minimum_backoff", rp.GetMinimumBackoff(), 0, 600*time.Second)
		if err != nil {
			return err
		}
		maxB, err := between("retry_policy.maximum_backoff", rp.GetMaximumBackoff(), 0, 600*time.Second)
		if err != nil {
			return err
		}
		if rp.GetMinimumBackoff() != "" && rp.GetMaximumBackoff() != "" && minB > maxB {
			return fmt.Errorf("retry_policy.minimum_backoff must not exceed maximum_backoff")
		}
	}
	if s.GetEnableExactlyOnceDelivery() && s.GetDelivery() != nil {
		return fmt.Errorf("exactly-once delivery is only available to pull subscriptions")
	}
	if push := s.GetPush(); push != nil {
		u, err := url.Parse(push.GetEndpoint())
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return fmt.Errorf("push.endpoint %q must be an https URL", push.GetEndpoint())
		}
		if push.GetAudience() != "" && push.GetServiceAccountEmail() == "" {
			return fmt.Errorf("push.audience requires service_account_email")
		}
		if push.GetWriteMetadata() && !push.GetNoWrapper() {
			return fmt.Errorf("push.write_metadata requires no_wrapper")
		}
	}
	if bq := s.GetBigquery(); bq != nil {
		if !tableRE.MatchString(bq.GetTable()) {
			return fmt.Errorf("bigquery.table %q must look like \"project.dataset.table\"", bq.GetTable())
		}
		if bq.GetUseTopicSchema() && bq.GetUseTableSchema() {
			return fmt.Errorf("bigquery.use_topic_schema and use_table_schema are mutually exclusive")
		}
		if bq.GetUseTopicSchema() && p.topicSchema(s.GetTopic()) == nil {
			return fmt.Errorf("bigquery.use_topic_schema requires topic %q to have a schema", s.GetTopic())
		}
	}
	if cs := s.GetCloudStorage(); cs != nil {
		if cs.GetBucket() == "" {
			return fmt.Errorf("cloud_storage.bucket is required")
		}
		if _, err := between("cloud_storage.max_duration", cs.GetMaxDuration(), time.Minute, 10*time.Minute); err != nil {
			return err
		}
		if b := cs.GetMaxBytes(); b != 0 && (b < 1<<10 || b > 10<<30) {
			return fmt.Errorf("cloud_storage.max_bytes must be between 1 KiB and 10 GiB")
		}
		if cs.GetMaxMessages() < 0 {
			return fmt.Errorf("cloud_storage.max_messages must be >= 0")
		}
		if cs.GetAvro().GetUseTopicSchema() && p.topicSchema(s.GetTopic()) == nil {
			return fmt.Errorf("cloud_storage.avro.use_topic_schema requires topic %q to have a schema", s.GetTopic())
		}
	}
	return nil
}

// fullName qualifies a resource ID with the project, when known.
func fullName(project, kind, id string) string {
	if project == "" || strings.HasPrefix(id, "projects/") {
		return id
	}
	return "projects/" + project + "/" + kind + "/" + id
}

// renderSubscription renders a subscription with the Pub/Sub API's field
// names.
func (p *Protocol) renderSubscription(s *subscription) *asyncapi.Map[any] {
	d := s.decl
	project := core.FirstNonEmpty(d.GetProject(), p.project)
	m := asyncapi.NewMap[any]()
	m.Set("name", fullName(project, "subscriptions", s.name))
	m.Set("delivery", s.delivery())
	if d == nil {
		return m
	}
	if d.GetAckDeadline() != "" {
		v, _ := duration("ack_deadline", d.GetAckDeadline())
		m.Set("ackDeadlineSeconds", int64(v/time.Second))
	}
	if d.GetRetainAckedMessages() {
		m.Set("retainAckedMessages", true)
	}
	if r := d.GetMessageRetentionDuration(); r != "" {
		v, _ := duration("", r) // validated by collect
		m.Set("messageRetentionDuration", seconds(v))
	}
	if len(d.GetLabels()) > 0 {
		m.Set("labels", labels(d.GetLabels()))
	}
	if d.GetEnableMessageOrdering() {
		m.Set("enableMessageOrdering", true)
	}
	if e := d.GetExpiration(); e != "" {
		ttl := "never"
		if e != "never" {
			v, _ := duration("expiration", e)
			ttl = seconds(v)
		}
		x := asyncapi.NewMap[any]()
		x.Set("ttl", ttl)
		m.Set("expirationPolicy", x)
	}
	if d.GetFilter() != "" {
		m.Set("filter", d.GetFilter())
	}
	if dl := d.GetDeadLetterPolicy(); dl != nil {
		x := asyncapi.NewMap[any]()
		x.Set("deadLetterTopic", fullName(project, "topics", dl.GetTopic()))
		attempts := dl.GetMaxDeliveryAttempts()
		if attempts == 0 {
			attempts = 5
		}
		x.Set("maxDeliveryAttempts", attempts)
		m.Set("deadLetterPolicy", x)
	}
	if rp := d.GetRetryPolicy(); rp != nil {
		x := asyncapi.NewMap[any]()
		minB, _ := duration("", core.FirstNonEmpty(rp.GetMinimumBackoff(), "10s"))
		maxB, _ := duration("", core.FirstNonEmpty(rp.GetMaximumBackoff(), "600s"))
		x.Set("minimumBackoff", seconds(minB))
		x.Set("maximumBackoff", seconds(maxB))
		m.Set("retryPolicy", x)
	}
	if d.GetEnableExactlyOnceDelivery() {
		m.Set("enableExactlyOnceDelivery", true)
	}
	if push := d.GetPush(); push != nil {
		x := asyncapi.NewMap[any]()
		x.Set("pushEndpoint", push.GetEndpoint())
		if push.GetServiceAccountEmail() != "" {
			oidc := asyncapi.NewMap[any]()
			oidc.Set("serviceAccountEmail", push.GetServiceAccountEmail())
			oidc.Set("audience", core.FirstNonEmpty(push.GetAudience(), push.GetEndpoint()))
			x.Set("oidcToken", oidc)
		}
		if push.GetNoWrapper() {
			nw := asyncapi.NewMap[any]()
			nw.Set("writeMetadata", push.GetWriteMetadata())
			x.Set("noWrapper", nw)
		}
		if len(push.GetAttributes()) > 0 {
			x.Set("attributes", labels(push.GetAttributes()))
		}
		m.Set("pushConfig", x)
	}
	if bq := d.GetBigquery(); bq != nil {
		x := asyncapi.NewMap[any]()
		x.Set("table", bq.GetTable())
		flag := func(k string, v bool) {
			if v {
				x.Set(k, true)
			}
		}
		flag("useTopicSchema", bq.GetUseTopicSchema())
		flag("useTableSchema", bq.GetUseTableSchema())
		flag("writeMetadata", bq.GetWriteMetadata())
		flag("dropUnknownFields", bq.GetDropUnknownFields())
		if bq.GetServiceAccountEmail() != "" {
			x.Set("serviceAccountEmail", bq.GetServiceAccountEmail())
		}
		m.Set("bigqueryConfig", x)
	}
	if cs := d.GetCloudStorage(); cs != nil {
		x := asyncapi.NewMap[any]()
		x.Set("bucket", cs.GetBucket())
		str := func(k, v string) {
			if v != "" {
				x.Set(k, v)
			}
		}
		str("filenamePrefix", cs.GetFilenamePrefix())
		str("filenameSuffix", cs.GetFilenameSuffix())
		str("filenameDatetimeFormat", cs.GetFilenameDatetimeFormat())
		if cs.GetMaxDuration() != "" {
			v, _ := duration("", cs.GetMaxDuration())
			x.Set("maxDuration", seconds(v))
		}
		if cs.GetMaxBytes() > 0 {
			x.Set("maxBytes", cs.GetMaxBytes())
		}
		if cs.GetMaxMessages() > 0 {
			x.Set("maxMessages", cs.GetMaxMessages())
		}
		if a := cs.GetAvro(); a != nil {
			ac := asyncapi.NewMap[any]()
			ac.Set("writeMetadata", a.GetWriteMetadata())
			ac.Set("useTopicSchema", a.GetUseTopicSchema())
			x.Set("avroConfig", ac)
		} else {
			x.Set("textConfig", asyncapi.NewMap[any]())
		}
		if cs.GetServiceAccountEmail() != "" {
			x.Set("serviceAccountEmail", cs.GetServiceAccountEmail())
		}
		m.Set("cloudStorageConfig", x)
	}
	return m
}

func labels(in map[string]string) *asyncapi.Map[any] {
	out := asyncapi.NewMap[any]()
	for _, k := range core.SortedKeys(in) {
		out.Set(k, in[k])
	}
	return out
}
