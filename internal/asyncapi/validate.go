package asyncapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Rules the official JSON Schemas enforce, and the cross-object rules of the
// specification text they cannot express, are checked by Validate so a
// generator never emits a document that AsyncAPI tooling would reject.

// KeyPattern is the pattern of component, channel, operation and server keys.
var KeyPattern = regexp.MustCompile(`^[A-Za-z0-9_.\-]+$`)

// RuntimeExpression matches the runtime expressions used by correlation IDs,
// parameter locations and reply addresses.
var RuntimeExpression = regexp.MustCompile(`^\$message\.(header|payload)#(/(([^/~])|(~[01]))*)*$`)

var addressParam = regexp.MustCompile(`\{([^{}]+)\}`)

// bindingProtocols lists the official binding keys per bindings kind for
// AsyncAPI 3.0.0. 3.1.0 adds "ros2" to server and operation bindings. Keys
// starting with "x-" are always allowed.
var bindingProtocols = map[string][]string{
	"server":    {"http", "ws", "amqp", "amqp1", "mqtt", "kafka", "anypointmq", "nats", "jms", "sns", "sqs", "stomp", "redis", "ibmmq", "solace", "googlepubsub", "pulsar"},
	"channel":   {"http", "ws", "amqp", "amqp1", "mqtt", "kafka", "anypointmq", "nats", "jms", "sns", "sqs", "stomp", "redis", "ibmmq", "solace", "googlepubsub", "pulsar"},
	"operation": {"http", "ws", "amqp", "amqp1", "mqtt", "kafka", "anypointmq", "nats", "jms", "sns", "sqs", "stomp", "redis", "ibmmq", "solace", "googlepubsub"},
	"message":   {"http", "ws", "amqp", "amqp1", "mqtt", "kafka", "anypointmq", "nats", "jms", "sns", "sqs", "stomp", "redis", "ibmmq", "solace", "googlepubsub"},
}

// BindingAllowed reports whether protocol is a valid key of a bindings object
// of the given kind ("server", "channel", "operation", "message").
func BindingAllowed(version, kind, protocol string) bool {
	if strings.HasPrefix(protocol, "x-") {
		return true
	}
	if slices.Contains(bindingProtocols[kind], protocol) {
		return true
	}
	return protocol == "ros2" && version != "3.0.0" && (kind == "server" || kind == "operation")
}

// securityFields lists, per security scheme type, the fields allowed besides
// type and description, and the ones required.
var securityFields = map[string]struct{ allowed, required []string }{
	SecurityUserPassword:         {},
	SecurityAPIKey:               {allowed: []string{"in"}, required: []string{"in"}},
	SecurityX509:                 {},
	SecuritySymmetricEncryption:  {},
	SecurityAsymmetricEncryption: {},
	SecurityHTTPAPIKey:           {allowed: []string{"name", "in"}, required: []string{"name", "in"}},
	SecurityHTTP:                 {allowed: []string{"scheme", "bearerFormat"}, required: []string{"scheme"}},
	SecurityOAuth2:               {allowed: []string{"flows", "scopes"}, required: []string{"flows"}},
	SecurityOpenIDConnect:        {allowed: []string{"openIdConnectUrl", "scopes"}, required: []string{"openIdConnectUrl"}},
	SecurityPlain:                {},
	SecurityScramSHA256:          {},
	SecurityScramSHA512:          {},
	SecurityGSSAPI:               {},
}

// Validate checks doc against AsyncAPI version rules and returns every
// problem found.
func Validate(doc *Document) error {
	v := &validator{doc: doc, version: doc.AsyncAPI}
	v.run()
	return errors.Join(v.errs...)
}

type validator struct {
	doc     *Document
	version string
	errs    []error
	tree    any // the rendered document, for resolving $refs
}

func (v *validator) errorf(format string, args ...any) {
	v.errs = append(v.errs, fmt.Errorf(format, args...))
}

func (v *validator) run() {
	d := v.doc
	if !slices.Contains(Versions, d.AsyncAPI) {
		v.errorf("asyncapi: unsupported version %q (want one of %s)", d.AsyncAPI, strings.Join(Versions, ", "))
	}
	if d.Info.Title == "" || d.Info.Version == "" {
		v.errorf("info: title and version are required")
	}
	if l := d.Info.License; l != nil && l.Name == "" {
		v.errorf("info.license: name is required")
	}

	raw, err := MarshalJSON(d)
	if err != nil {
		v.errorf("encoding: %v", err)
		return
	}
	if err := json.Unmarshal(raw, &v.tree); err != nil {
		v.errorf("encoding: %v", err)
		return
	}
	v.walk("", v.tree)

	for _, k := range d.Servers.Keys() {
		s, _ := d.Servers.Get(k)
		v.key("servers", k)
		v.server("servers."+k, s)
	}
	for _, k := range d.Channels.Keys() {
		c, _ := d.Channels.Get(k)
		v.key("channels", k)
		v.channel("channels."+k, c)
	}
	for _, k := range d.Operations.Keys() {
		o, _ := d.Operations.Get(k)
		v.key("operations", k)
		v.operation("operations."+k, o)
	}
	if c := d.Components; c != nil {
		v.components(c)
	}
}

func (v *validator) key(where, k string) {
	if !KeyPattern.MatchString(k) {
		v.errorf("%s: key %q must match %s", where, k, KeyPattern)
	}
}

// walk checks what applies to every object of the rendered document:
// specification extension keys and local references.
func (v *validator) walk(path string, node any) {
	switch n := node.(type) {
	case map[string]any:
		if ref, ok := n["$ref"].(string); ok && strings.HasPrefix(ref, "#") {
			if resolve(v.tree, ref) == nil {
				v.errorf("%s: $ref %q does not resolve", strings.TrimPrefix(path, "."), ref)
			}
		}
		for k, c := range n {
			v.walk(path+"."+k, c)
		}
	case []any:
		for i, c := range n {
			v.walk(fmt.Sprintf("%s[%d]", path, i), c)
		}
	}
}

func resolve(root any, ref string) any {
	if ref == "#" {
		return root
	}
	if !strings.HasPrefix(ref, "#/") {
		return nil
	}
	cur := root
	for _, tok := range strings.Split(ref[2:], "/") {
		tok = strings.NewReplacer("~1", "/", "~0", "~").Replace(tok)
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		if cur, ok = m[tok]; !ok {
			return nil
		}
	}
	return cur
}

func (v *validator) extensions(where string, x Extensions) {
	for k := range x {
		if !strings.HasPrefix(k, "x-") {
			v.errorf("%s: extension %q must start with \"x-\"", where, k)
		}
	}
}

func (v *validator) bindings(where, kind string, b *Bindings) {
	if b == nil || b.Ref != "" {
		return
	}
	for _, p := range b.Values.Keys() {
		if !BindingAllowed(v.version, kind, p) {
			v.errorf("%s.bindings: %q is not a %s binding of AsyncAPI %s (use an official protocol or an \"x-\" key)", where, p, kind, v.version)
		}
	}
}

func (v *validator) tags(where string, tags []*Tag) {
	seen := map[string]bool{}
	for _, t := range tags {
		if t.Ref != "" {
			// Tags must be unique after references are resolved.
			if m, ok := resolve(v.tree, t.Ref).(map[string]any); ok {
				if name, _ := m["name"].(string); name != "" {
					if seen[name] {
						v.errorf("%s.tags: duplicate tag %q (via %s)", where, name, t.Ref)
					}
					seen[name] = true
				}
			}
			continue
		}
		if t.Name == "" {
			v.errorf("%s.tags: name is required", where)
		}
		if seen[t.Name] {
			v.errorf("%s.tags: duplicate tag %q", where, t.Name)
		}
		seen[t.Name] = true
		v.docs(where+".tags."+t.Name, t.ExternalDocs)
	}
}

func (v *validator) docs(where string, d *ExternalDocs) {
	if d != nil && d.Ref == "" && d.URL == "" {
		v.errorf("%s.externalDocs: url is required", where)
	}
}

func (v *validator) server(where string, s *Server) {
	if s.Ref != "" {
		return
	}
	if s.Host == "" || s.Protocol == "" {
		v.errorf("%s: host and protocol are required", where)
	}
	v.bindings(where, "server", s.Bindings)
	v.tags(where, s.Tags)
	v.docs(where, s.ExternalDocs)
	v.extensions(where, s.Extensions)
	for _, sec := range s.Security {
		v.securityScheme(where+".security", sec)
	}
}

func (v *validator) channel(where string, c *Channel) {
	if c.Ref != "" {
		return
	}
	v.bindings(where, "channel", c.Bindings)
	v.tags(where, c.Tags)
	v.docs(where, c.ExternalDocs)
	v.extensions(where, c.Extensions)
	for _, k := range c.Messages.Keys() {
		v.key(where+".messages", k)
		m, _ := c.Messages.Get(k)
		v.message(where+".messages."+k, m)
	}
	for _, k := range c.Parameters.Keys() {
		v.key(where+".parameters", k)
		p, _ := c.Parameters.Get(k)
		v.parameter(where+".parameters."+k, p)
	}
	if c.Address != nil {
		var names []string
		for _, m := range addressParam.FindAllStringSubmatch(*c.Address, -1) {
			names = append(names, m[1])
			if _, ok := c.Parameters.Get(m[1]); !ok {
				v.errorf("%s: address parameter {%s} has no parameters entry", where, m[1])
			}
		}
		for _, k := range c.Parameters.Keys() {
			if !slices.Contains(names, k) {
				v.errorf("%s: parameter %q does not appear in address %q", where, k, *c.Address)
			}
		}
	}
}

func (v *validator) parameter(where string, p *Parameter) {
	if p.Ref != "" {
		return
	}
	if p.Location != "" && !RuntimeExpression.MatchString(p.Location) {
		v.errorf("%s: location %q is not a runtime expression", where, p.Location)
	}
	v.extensions(where, p.Extensions)
}

func (v *validator) operation(where string, o *Operation) {
	if o.Ref != "" {
		return
	}
	if o.Action != ActionSend && o.Action != ActionReceive {
		v.errorf("%s: action must be send or receive, got %q", where, o.Action)
	}
	if o.Channel == nil || !strings.HasPrefix(o.Channel.Ref, "#/channels/") {
		v.errorf("%s: channel must reference a channel of the document", where)
	} else {
		// Operation messages must be messages of the operation's channel.
		for _, m := range o.Messages {
			if !strings.HasPrefix(m.Ref, o.Channel.Ref+"/messages/") {
				v.errorf("%s: message %q is not a message of channel %q", where, m.Ref, o.Channel.Ref)
			}
		}
	}
	for _, sec := range o.Security {
		v.securityScheme(where+".security", sec)
	}
	v.bindings(where, "operation", o.Bindings)
	v.tags(where, o.Tags)
	v.docs(where, o.ExternalDocs)
	v.extensions(where, o.Extensions)
	for i, t := range o.Traits {
		v.operationTrait(fmt.Sprintf("%s.traits[%d]", where, i), t)
	}
	if r := o.Reply; r != nil {
		v.reply(where+".reply", r)
	}
}

func (v *validator) operationTrait(where string, t *OperationTrait) {
	if t.Ref != "" {
		return
	}
	v.bindings(where, "operation", t.Bindings)
	v.tags(where, t.Tags)
	v.docs(where, t.ExternalDocs)
	v.extensions(where, t.Extensions)
}

func (v *validator) reply(where string, r *OperationReply) {
	if r.Ref != "" {
		return
	}
	if a := r.Address; a != nil && a.Ref == "" && !RuntimeExpression.MatchString(a.Location) {
		v.errorf("%s.address: location %q is not a runtime expression", where, a.Location)
	}
	if r.Channel != nil {
		for _, m := range r.Messages {
			if !strings.HasPrefix(m.Ref, r.Channel.Ref+"/messages/") {
				v.errorf("%s: message %q is not a message of channel %q", where, m.Ref, r.Channel.Ref)
			}
		}
	}
	v.extensions(where, r.Extensions)
}

func (v *validator) message(where string, m *Message) {
	if m.Ref != "" {
		return
	}
	if c := m.CorrelationID; c != nil && c.Ref == "" && !RuntimeExpression.MatchString(c.Location) {
		v.errorf("%s.correlationId: location %q is not a runtime expression", where, c.Location)
	}
	v.bindings(where, "message", m.Bindings)
	v.tags(where, m.Tags)
	v.docs(where, m.ExternalDocs)
	v.extensions(where, m.Extensions)
	for i, t := range m.Traits {
		if t.Ref == "" {
			tw := fmt.Sprintf("%s.traits[%d]", where, i)
			v.bindings(tw, "message", t.Bindings)
			v.tags(tw, t.Tags)
			v.extensions(tw, t.Extensions)
		}
	}
}

func (v *validator) securityScheme(where string, s *SecurityScheme) {
	if s.Ref != "" {
		return
	}
	rules, ok := securityFields[s.Type]
	if !ok {
		v.errorf("%s: unknown security scheme type %q", where, s.Type)
		return
	}
	set := map[string]bool{
		"name": s.Name != "", "in": s.In != "", "scheme": s.Scheme != "", "bearerFormat": s.BearerFormat != "",
		"flows": s.Flows != nil, "openIdConnectUrl": s.OpenIDConnectURL != "", "scopes": len(s.Scopes) > 0,
	}
	for f, isSet := range set {
		if isSet && !slices.Contains(rules.allowed, f) {
			v.errorf("%s: %q is not allowed for security scheme type %q", where, f, s.Type)
		}
	}
	for _, f := range rules.required {
		if !set[f] {
			v.errorf("%s: %q is required for security scheme type %q", where, f, s.Type)
		}
	}
	switch s.Type {
	case SecurityAPIKey:
		if s.In != "" && s.In != "user" && s.In != "password" {
			v.errorf("%s: in must be user or password for apiKey", where)
		}
	case SecurityHTTPAPIKey:
		if s.In != "" && s.In != "query" && s.In != "header" && s.In != "cookie" {
			v.errorf("%s: in must be query, header or cookie for httpApiKey", where)
		}
	case SecurityHTTP:
		if s.BearerFormat != "" && !strings.EqualFold(s.Scheme, "bearer") {
			v.errorf("%s: bearerFormat requires the bearer scheme", where)
		}
	}
	if f := s.Flows; f != nil {
		v.flow(where+".flows.implicit", f.Implicit, true, false)
		v.flow(where+".flows.password", f.Password, false, true)
		v.flow(where+".flows.clientCredentials", f.ClientCredentials, false, true)
		v.flow(where+".flows.authorizationCode", f.AuthorizationCode, true, true)
	}
	v.extensions(where, s.Extensions)
}

// flow checks an OAuth flow: which of the authorization and token URLs it
// needs (the other is forbidden, except for authorizationCode which needs
// both), and that it lists its available scopes.
func (v *validator) flow(where string, f *OAuthFlow, auth, token bool) {
	if f == nil {
		return
	}
	if auth != (f.AuthorizationURL != "") {
		v.errorf("%s: authorizationUrl is %s", where, map[bool]string{true: "required", false: "not allowed"}[auth])
	}
	if token != (f.TokenURL != "") {
		v.errorf("%s: tokenUrl is %s", where, map[bool]string{true: "required", false: "not allowed"}[token])
	}
	if f.AvailableScopes == nil {
		v.errorf("%s: availableScopes is required", where)
	}
	v.extensions(where, f.Extensions)
}

func (v *validator) components(c *Components) {
	check := func(section string, keys []string) {
		for _, k := range keys {
			v.key("components."+section, k)
		}
	}
	check("schemas", c.Schemas.Keys())
	check("servers", c.Servers.Keys())
	check("channels", c.Channels.Keys())
	check("operations", c.Operations.Keys())
	check("messages", c.Messages.Keys())
	check("securitySchemes", c.SecuritySchemes.Keys())
	check("serverVariables", c.ServerVariables.Keys())
	check("parameters", c.Parameters.Keys())
	check("correlationIds", c.CorrelationIDs.Keys())
	check("replies", c.Replies.Keys())
	check("replyAddresses", c.ReplyAddresses.Keys())
	check("externalDocs", c.ExternalDocs.Keys())
	check("tags", c.Tags.Keys())
	check("operationTraits", c.OperationTraits.Keys())
	check("messageTraits", c.MessageTraits.Keys())
	check("serverBindings", c.ServerBindings.Keys())
	check("channelBindings", c.ChannelBindings.Keys())
	check("operationBindings", c.OperationBindings.Keys())
	check("messageBindings", c.MessageBindings.Keys())

	for _, k := range c.Servers.Keys() {
		s, _ := c.Servers.Get(k)
		v.server("components.servers."+k, s)
	}
	for _, k := range c.Channels.Keys() {
		ch, _ := c.Channels.Get(k)
		v.channel("components.channels."+k, ch)
	}
	for _, k := range c.Messages.Keys() {
		m, _ := c.Messages.Get(k)
		v.message("components.messages."+k, m)
	}
	for _, k := range c.SecuritySchemes.Keys() {
		s, _ := c.SecuritySchemes.Get(k)
		v.securityScheme("components.securitySchemes."+k, s)
	}
	for _, k := range c.Parameters.Keys() {
		p, _ := c.Parameters.Get(k)
		v.parameter("components.parameters."+k, p)
	}
	for _, k := range c.OperationTraits.Keys() {
		t, _ := c.OperationTraits.Get(k)
		v.operationTrait("components.operationTraits."+k, t)
	}
	for _, k := range c.Replies.Keys() {
		r, _ := c.Replies.Get(k)
		v.reply("components.replies."+k, r)
	}
	kinds := map[string]*Map[*Bindings]{
		"server": c.ServerBindings, "channel": c.ChannelBindings, "operation": c.OperationBindings, "message": c.MessageBindings,
	}
	for kind, m := range kinds {
		for _, k := range m.Keys() {
			b, _ := m.Get(k)
			v.bindings("components."+kind+"Bindings."+k, kind, b)
		}
	}
	v.extensions("components", c.Extensions)
}
