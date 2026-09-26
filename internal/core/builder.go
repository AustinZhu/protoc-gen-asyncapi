package core

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
)

// Protocol maps Protobuf services onto one messaging protocol (NATS,
// Temporal, ...). The Builder does everything protocol-neutral: document
// settings, info, servers, security, payload schemas, messages, channel
// parameters, tags and validation; a Protocol decides which services it
// documents and turns their rpcs into channels and operations.
type Protocol interface {
	// Name is the protocol name used for server.protocol defaults, e.g.
	// "nats".
	Name() string
	// Options are the protocol's own plugin options.
	Options() []Option
	// Claims reports whether the protocol documents a service. Each service
	// is documented by the first protocol claiming it.
	Claims(b *Builder, s *protogen.Service) bool
	// HasContent reports whether the files carry protocol content besides
	// services (e.g. messages with their own channel), so a document is
	// produced even without services.
	HasContent(b *Builder) bool
	// Begin runs once info, servers and security schemes are built.
	Begin(b *Builder) error
	// AddService documents one claimed service.
	AddService(b *Builder, s *protogen.Service) error
	// Finish runs after every service, before the document is finalised.
	Finish(b *Builder) error
}

// Builder assembles one AsyncAPI document from a set of files.
type Builder struct {
	Params *Params
	Plugin *protogen.Plugin
	// Files are the files of this document.
	Files []*protogen.File
	// Doc is the document being built.
	Doc *asyncapi.Document
	// Settings are the (asyncapi.v3.document) options of Files and of the
	// files they import, in that order.
	Settings []*FileSettings
	// Servers are the declared servers by name.
	Servers map[string]*asyncapiv3.Server
	// Protocol is the protocol documenting the current service.
	Protocol Protocol

	protocols []Protocol
	schemas   *schemaGen
	security  map[string]bool
	infoTags  *asyncapi.Map[*asyncapi.Tag]
	channels  *asyncapi.Map[*Channel]
	byAddress map[string]*Channel
	ops       []*Op
	opIDs     map[string]protoreflect.Descriptor
	messages  *asyncapi.Map[*Message]
	msgByName map[protoreflect.FullName]*Message
	claims    map[protoreflect.FullName]Protocol
}

// FileSettings pairs a file with its document options.
type FileSettings struct {
	File *protogen.File
	Doc  *asyncapiv3.Document
}

// Channel tracks a channel while the document is being built.
type Channel struct {
	ID string
	// Obj is the channel object of the document.
	Obj *asyncapi.Channel
	// Params are the parameter names of the address, in order.
	Params []string
	// Messages are the messages sent on the channel.
	Messages []*Message
	// DeclaredBy is the descriptor that created the channel, for errors.
	DeclaredBy protoreflect.Descriptor

	paramOpts map[string]*asyncapiv3.Parameter
	servers   map[string]bool
}

// AddMessage adds a message to the channel (once).
func (ch *Channel) AddMessage(m *Message) {
	for _, e := range ch.Messages {
		if e == m {
			return
		}
	}
	ch.Messages = append(ch.Messages, m)
	if ch.Obj.Messages == nil {
		ch.Obj.Messages = asyncapi.NewMap[*asyncapi.Message]()
	}
	ch.Obj.Messages.Set(m.ID, asyncapi.MessageRef(m.ID))
}

// MessageRef references a message of the channel, for operations.
func (ch *Channel) MessageRef(m *Message) *asyncapi.Reference {
	return asyncapi.Ref("#/channels/" + ch.ID + "/messages/" + m.ID)
}

// Ref references the channel.
func (ch *Channel) Ref() *asyncapi.Reference { return asyncapi.Ref("#/channels/" + ch.ID) }

// Op tracks an operation while the document is being built.
type Op struct {
	ID      string
	Obj     *asyncapi.Operation
	Channel *Channel
	Service *protogen.Service
	Method  *protogen.Method
	Payload *Message
}

// Message tracks a component message.
type Message struct {
	ID    string
	Obj   *asyncapi.Message
	Proto *protogen.Message // nil for synthetic messages

	headers  *asyncapi.Map[*asyncapi.Schema]
	required map[string]bool
	// overridden reports a payload replaced by payload_schema: it no longer
	// follows the Protobuf fields.
	overridden bool
}

// AddHeader documents a header unless already present.
func (m *Message) AddHeader(name string, s *asyncapi.Schema, required bool) {
	if _, ok := m.headers.Get(name); !ok {
		m.headers.Set(name, s)
	}
	if required {
		m.required[name] = true
	}
}

func newBuilder(p *Params, plugin *protogen.Plugin, files []*protogen.File, protocols []Protocol) *Builder {
	b := &Builder{
		Params:    p,
		Plugin:    plugin,
		Files:     files,
		Servers:   map[string]*asyncapiv3.Server{},
		protocols: protocols,
		security:  map[string]bool{},
		infoTags:  asyncapi.NewMap[*asyncapi.Tag](),
		channels:  asyncapi.NewMap[*Channel](),
		byAddress: map[string]*Channel{},
		opIDs:     map[string]protoreflect.Descriptor{},
		messages:  asyncapi.NewMap[*Message](),
		msgByName: map[protoreflect.FullName]*Message{},
		claims:    map[protoreflect.FullName]Protocol{},
	}
	b.schemas = newSchemaGen(b)
	return b
}

// build returns the document, or nil when the files describe nothing.
func (b *Builder) build() (*asyncapi.Document, error) {
	b.collectSettings()

	type claimed struct {
		s *protogen.Service
		p Protocol
	}
	var services []claimed
	active := map[Protocol]bool{}
	for _, f := range b.Files {
		for _, s := range f.Services {
			if ServiceOptions(s).GetSkip() || !b.selected(s) {
				continue
			}
			for _, p := range b.protocols {
				if p.Claims(b, s) {
					services = append(services, claimed{s, p})
					b.claims[s.Desc.FullName()] = p
					active[p] = true
					break
				}
			}
		}
	}
	for _, p := range b.protocols {
		if p.HasContent(b) {
			active[p] = true
		}
	}
	if len(services) == 0 && len(active) == 0 && !b.ownSettings() {
		return nil, nil
	}

	b.Doc = &asyncapi.Document{
		AsyncAPI:   b.Params.AsyncAPIVersion,
		Channels:   asyncapi.NewMap[*asyncapi.Channel](),
		Operations: asyncapi.NewMap[*asyncapi.Operation](),
	}
	var svcs []*protogen.Service
	for _, c := range services {
		svcs = append(svcs, c.s)
	}
	if err := b.buildInfo(svcs); err != nil {
		return nil, err
	}
	if err := b.buildServers(active); err != nil {
		return nil, err
	}
	for _, p := range b.protocols {
		b.Protocol = p
		if err := p.Begin(b); err != nil {
			return nil, err
		}
	}
	for _, c := range services {
		b.Protocol = c.p
		if err := c.p.AddService(b, c.s); err != nil {
			return nil, err
		}
	}
	for _, p := range b.protocols {
		b.Protocol = p
		if err := p.Finish(b); err != nil {
			return nil, err
		}
	}
	b.Protocol = nil
	if err := b.finish(); err != nil {
		return nil, err
	}
	return b.Doc, nil
}

// ClaimedBy returns the protocol documenting a service.
func (b *Builder) ClaimedBy(s *protogen.Service) Protocol { return b.claims[s.Desc.FullName()] }

func (b *Builder) selected(s *protogen.Service) bool {
	if len(b.Params.Services) == 0 {
		return true
	}
	for _, g := range b.Params.Services {
		if MatchService(g, string(s.Desc.FullName())) {
			return true
		}
	}
	return false
}

// WalkMessages calls fn for every (nested) message, skipping map entries.
func WalkMessages(msgs []*protogen.Message, fn func(*protogen.Message)) {
	for _, m := range msgs {
		if m.Desc.IsMapEntry() {
			continue
		}
		fn(m)
		WalkMessages(m.Messages, fn)
	}
}

// Imports returns Files followed by their transitive imports, once each.
func (b *Builder) Imports() []*protogen.File {
	seen := map[string]bool{}
	var out []*protogen.File
	queue := append([]*protogen.File(nil), b.Files...)
	for len(queue) > 0 {
		f := queue[0]
		queue = queue[1:]
		if seen[f.Desc.Path()] {
			continue
		}
		seen[f.Desc.Path()] = true
		out = append(out, f)
		imports := f.Desc.Imports()
		for i := 0; i < imports.Len(); i++ {
			if dep, ok := b.Plugin.FilesByPath[imports.Get(i).Path()]; ok {
				queue = append(queue, dep)
			}
		}
	}
	return out
}

// collectSettings gathers the document options of the input files followed
// by those of their imports, so shared files can declare servers and
// security schemes once.
func (b *Builder) collectSettings() {
	for _, f := range b.Imports() {
		if d := DocumentOptions(f); d != nil {
			b.Settings = append(b.Settings, &FileSettings{File: f, Doc: d})
		}
	}
}

// OwnFile reports whether f is one of the files of this document.
func (b *Builder) OwnFile(f *protogen.File) bool {
	for _, g := range b.Files {
		if g == f {
			return true
		}
	}
	return false
}

func (b *Builder) ownSettings() bool {
	for _, s := range b.Settings {
		if b.OwnFile(s.File) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Info, servers, security
// ---------------------------------------------------------------------------

func (b *Builder) buildInfo(services []*protogen.Service) error {
	// The file declaring the info (or the first input file) provides the
	// defaults.
	var info *asyncapiv3.Info
	first := b.Files[0]
	for _, s := range b.Settings {
		if s.Doc.GetInfo() != nil {
			info, first = s.Doc.GetInfo(), s.File
			break
		}
	}
	for _, s := range b.Settings {
		if b.Doc.ID == "" {
			b.Doc.ID = s.Doc.GetId()
		}
		if b.Doc.DefaultContentType == "" {
			b.Doc.DefaultContentType = s.Doc.GetDefaultContentType()
		}
		for _, t := range s.Doc.GetTags() {
			b.infoTags.Set(t.GetName(), ConvertTag(t))
		}
		if err := decodeExtensions(s.File.Desc.Path()+": document", s.Doc.GetExtensions(), &b.Doc.Extensions); err != nil {
			return err
		}
	}
	if b.Params.ID != "" {
		b.Doc.ID = b.Params.ID
	}
	if b.Params.ContentType != "" {
		b.Doc.DefaultContentType = b.Params.ContentType
	}
	if b.Doc.DefaultContentType == "" {
		b.Doc.DefaultContentType = "application/json"
		if b.Params.Payload == "protobuf" {
			b.Doc.DefaultContentType = "application/protobuf"
		}
	}

	i := asyncapi.Info{
		Title:          info.GetTitle(),
		Version:        info.GetVersion(),
		Description:    info.GetDescription(),
		TermsOfService: info.GetTermsOfService(),
	}
	if b.Params.Title != "" {
		i.Title = b.Params.Title
	}
	if b.Params.Description != "" {
		i.Description = b.Params.Description
	}
	if i.Title == "" {
		switch {
		case len(services) == 1:
			i.Title = string(services[0].Desc.Name())
		case first.Desc.Package() != "":
			i.Title = string(first.Desc.Package())
		default:
			i.Title = strings.TrimSuffix(first.Desc.Path(), ".proto")
		}
	}
	if b.Params.Version != "" {
		i.Version = b.Params.Version
	}
	if i.Version == "" {
		i.Version = "0.0.0"
	}
	if i.Description == "" {
		// Leading comments of the `package` statement, or of the `syntax`
		// statement when the file starts with the comment.
		locs := first.Desc.SourceLocations()
		for _, field := range []int32{2, 12} {
			if i.Description == "" {
				i.Description = commentText(protogen.Comments(locs.ByPath(protoreflect.SourcePath{field}).LeadingComments))
			}
		}
		if len(services) == 1 && i.Description == "" {
			i.Description = describe(services[0].Comments)
		}
	}
	if info.GetSummary() != "" {
		// AsyncAPI has no info summary; fold it into the description.
		i.Description = strings.TrimSpace(info.GetSummary() + "\n\n" + i.Description)
	}
	if c := info.GetContact(); c != nil {
		i.Contact = &asyncapi.Contact{Name: c.GetName(), URL: c.GetUrl(), Email: c.GetEmail()}
	}
	if l := info.GetLicense(); l != nil {
		if l.GetName() == "" {
			return fmt.Errorf("%s: info.license.name is required", first.Desc.Path())
		}
		i.License = &asyncapi.License{Name: l.GetName(), URL: l.GetUrl()}
	}
	if err := decodeExtensions(first.Desc.Path()+": info", info.GetExtensions(), &i.Extensions); err != nil {
		return err
	}
	for _, s := range b.Settings {
		if d := s.Doc.GetExternalDocs(); d != nil && i.ExternalDocs == nil {
			i.ExternalDocs = ConvertDocs(d)
		}
	}
	b.Doc.Info = i
	return nil
}

var securityTypes = map[asyncapiv3.SecuritySchemeType]string{
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_USER_PASSWORD:         asyncapi.SecurityUserPassword,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_API_KEY:               asyncapi.SecurityAPIKey,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_X509:                  asyncapi.SecurityX509,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_SYMMETRIC_ENCRYPTION:  asyncapi.SecuritySymmetricEncryption,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_ASYMMETRIC_ENCRYPTION: asyncapi.SecurityAsymmetricEncryption,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_HTTP_API_KEY:          asyncapi.SecurityHTTPAPIKey,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_HTTP:                  asyncapi.SecurityHTTP,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_OAUTH2:                asyncapi.SecurityOAuth2,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_OPEN_ID_CONNECT:       asyncapi.SecurityOpenIDConnect,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_PLAIN:                 asyncapi.SecurityPlain,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_SCRAM_SHA256:          asyncapi.SecurityScramSHA256,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_SCRAM_SHA512:          asyncapi.SecurityScramSHA512,
	asyncapiv3.SecuritySchemeType_SECURITY_SCHEME_TYPE_GSSAPI:                asyncapi.SecurityGSSAPI,
}

func convertFlow(f *asyncapiv3.OAuthFlow) *asyncapi.OAuthFlow {
	if f == nil {
		return nil
	}
	out := &asyncapi.OAuthFlow{AuthorizationURL: f.GetAuthorizationUrl(), TokenURL: f.GetTokenUrl(), RefreshURL: f.GetRefreshUrl()}
	scopes := asyncapi.NewMap[string]()
	for _, k := range sortedKeys(f.GetAvailableScopes()) {
		scopes.Set(k, f.GetAvailableScopes()[k])
	}
	out.AvailableScopes = scopes
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func (b *Builder) buildServers(active map[Protocol]bool) error {
	schemes := asyncapi.NewMap[*asyncapi.SecurityScheme]()
	origin := map[string]string{}
	for _, s := range b.Settings {
		where := s.File.Desc.Path()
		for _, sc := range s.Doc.GetSecuritySchemes() {
			name := sc.GetName()
			if !asyncapi.KeyPattern.MatchString(name) {
				return fmt.Errorf("%s: security scheme name %q is invalid (allowed: letters, digits, '.', '_' and '-')", where, name)
			}
			if prev, ok := origin["sec:"+name]; ok {
				return fmt.Errorf("%s: security scheme %q is already declared in %s", where, name, prev)
			}
			origin["sec:"+name] = where
			out := &asyncapi.SecurityScheme{
				Type:             securityTypes[sc.GetType()],
				Description:      sc.GetDescription(),
				Name:             sc.GetParameterName(),
				In:               sc.GetIn(),
				Scheme:           sc.GetScheme(),
				BearerFormat:     sc.GetBearerFormat(),
				OpenIDConnectURL: sc.GetOpenIdConnectUrl(),
				Scopes:           sc.GetScopes(),
			}
			if f := sc.GetFlows(); f != nil {
				out.Flows = &asyncapi.OAuthFlows{
					Implicit: convertFlow(f.GetImplicit()), Password: convertFlow(f.GetPassword()),
					ClientCredentials: convertFlow(f.GetClientCredentials()), AuthorizationCode: convertFlow(f.GetAuthorizationCode()),
				}
			}
			if err := decodeExtensions(fmt.Sprintf("%s: security scheme %q", where, name), sc.GetExtensions(), &out.Extensions); err != nil {
				return err
			}
			schemes.Set(name, out)
			b.security[name] = true
		}
	}
	if schemes.Len() > 0 {
		b.Components().SecuritySchemes = schemes
	}

	// The default server protocol is the plugin's only protocol, or the only
	// protocol active in this document.
	defaultProtocol := ""
	switch {
	case len(b.protocols) == 1:
		defaultProtocol = b.protocols[0].Name()
	case len(active) == 1:
		for p := range active {
			defaultProtocol = p.Name()
		}
	}
	servers := asyncapi.NewMap[*asyncapi.Server]()
	for _, s := range b.Settings {
		for _, srv := range s.Doc.GetServers() {
			name := srv.GetName()
			where := s.File.Desc.Path()
			if !asyncapi.KeyPattern.MatchString(name) {
				return fmt.Errorf("%s: server name %q is invalid (allowed: letters, digits, '.', '_' and '-')", where, name)
			}
			if prev, ok := origin["srv:"+name]; ok {
				return fmt.Errorf("%s: server %q is already declared in %s", where, name, prev)
			}
			origin["srv:"+name] = where
			if srv.GetHost() == "" {
				return fmt.Errorf("%s: server %q: host is required", where, name)
			}
			out := &asyncapi.Server{
				Host:            srv.GetHost(),
				Protocol:        firstNonEmpty(srv.GetProtocol(), defaultProtocol),
				ProtocolVersion: srv.GetProtocolVersion(),
				Pathname:        srv.GetPathname(),
				Title:           srv.GetTitle(),
				Summary:         srv.GetSummary(),
				Description:     srv.GetDescription(),
				Tags:            ConvertTags(srv.GetTags()),
				ExternalDocs:    ConvertDocs(srv.GetExternalDocs()),
			}
			if out.Protocol == "" {
				return fmt.Errorf("%s: server %q: protocol is required when the document describes several protocols", where, name)
			}
			if len(srv.GetVariables()) > 0 {
				out.Variables = asyncapi.NewMap[*asyncapi.ServerVariable]()
				for _, v := range srv.GetVariables() {
					out.Variables.Set(v.GetName(), &asyncapi.ServerVariable{
						Enum: v.GetEnum(), Default: v.GetDefault(), Description: v.GetDescription(), Examples: v.GetExamples(),
					})
				}
			}
			for _, sec := range srv.GetSecurity() {
				ref, err := b.SecurityRef(sec)
				if err != nil {
					return fmt.Errorf("%s: server %q: %w", where, name, err)
				}
				out.Security = append(out.Security, ref)
			}
			w := fmt.Sprintf("%s: server %q", where, name)
			if err := decodeBindings(w, b.Params.AsyncAPIVersion, "server", srv.GetBindings(), &out.Bindings); err != nil {
				return err
			}
			if err := decodeExtensions(w, srv.GetExtensions(), &out.Extensions); err != nil {
				return err
			}
			servers.Set(name, out)
			b.Servers[name] = srv
		}
	}
	if servers.Len() > 0 {
		b.Doc.Servers = servers
	}
	return nil
}

// SecurityRef references a declared security scheme.
func (b *Builder) SecurityRef(name string) (*asyncapi.SecurityScheme, error) {
	if !b.security[name] {
		return nil, fmt.Errorf("unknown security scheme %q (declare it in the security_schemes of the asyncapi.v3.document file option)", name)
	}
	return asyncapi.SecuritySchemeRef(name), nil
}

// Components returns the components object, creating it when needed.
func (b *Builder) Components() *asyncapi.Components {
	if b.Doc.Components == nil {
		b.Doc.Components = &asyncapi.Components{}
	}
	return b.Doc.Components
}

// AddInfoTag declares a tag in info.tags unless present.
func (b *Builder) AddInfoTag(t *asyncapi.Tag) {
	if _, ok := b.infoTags.Get(t.Name); !ok {
		b.infoTags.Set(t.Name, t)
	}
}

// ServiceTags returns the default tag of a service (its name, described by
// the first sentence of its comment) and declares it in info.tags. With
// without_default_tags it returns nothing and declares nothing.
func (b *Builder) ServiceTags(s *protogen.Service) []*asyncapi.Tag {
	if b.Params.WithoutDefaultTags {
		return nil
	}
	t := &asyncapi.Tag{Name: string(s.Desc.Name())}
	if summary, _ := summarize(describe(s.Comments)); summary != "" {
		t.Description = summary
	}
	b.AddInfoTag(t)
	return []*asyncapi.Tag{{Name: t.Name}}
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

// ChannelSpec describes a channel requested by a protocol.
type ChannelSpec struct {
	// DeclaredBy locates errors.
	DeclaredBy protoreflect.Descriptor
	// Address of the channel; channels are shared by address, or by Key
	// when set.
	Address string
	// Key identifies the channel when the address alone does not, e.g. an
	// AMQP routing key on two exchanges.
	Key string
	// Params are the parameter names appearing in the address.
	Params []string
	// DefaultID is used when no metadata sets an id.
	DefaultID string
	// Meta is channel metadata from annotations, merged in order (first
	// non-empty value wins).
	Meta []*asyncapiv3.Channel
	// Servers restricts the channel to named servers.
	Servers []string
	// Parameters are protocol-provided parameter defaults.
	Parameters map[string]*asyncapiv3.Parameter
}

// Channel returns the channel of an address, creating it when needed and
// merging metadata.
func (b *Builder) Channel(spec ChannelSpec) (*Channel, error) {
	d := spec.DeclaredBy
	var explicitID string
	for _, o := range spec.Meta {
		if o.GetId() != "" {
			explicitID = o.GetId()
			break
		}
	}
	key := spec.Address
	if spec.Key != "" {
		key = "\x00" + spec.Key
	}
	ch := b.byAddress[key]
	if ch == nil {
		id := firstNonEmpty(explicitID, spec.DefaultID)
		if !asyncapi.KeyPattern.MatchString(id) {
			return nil, Errorf(d, "channel id %q is invalid (allowed: letters, digits, '.', '_' and '-')", id)
		}
		if _, dup := b.channels.Get(id); dup {
			return nil, Errorf(d, "channel id %q is used by two different addresses; set the channel id", id)
		}
		addr := spec.Address
		ch = &Channel{
			ID:         id,
			Obj:        &asyncapi.Channel{Address: &addr},
			Params:     spec.Params,
			DeclaredBy: d,
			paramOpts:  map[string]*asyncapiv3.Parameter{},
			servers:    map[string]bool{},
		}
		b.byAddress[key] = ch
		b.channels.Set(id, ch)
		b.Doc.Channels.Set(id, ch.Obj)
	} else if explicitID != "" && explicitID != ch.ID {
		return nil, Errorf(d, "address %q already has channel id %q, cannot rename it to %q", spec.Address, ch.ID, explicitID)
	}

	c := ch.Obj
	for name, p := range spec.Parameters {
		if _, ok := ch.paramOpts[name]; !ok {
			ch.paramOpts[name] = p
		}
	}
	for _, o := range spec.Meta {
		if o == nil {
			continue
		}
		c.Title = firstNonEmpty(c.Title, o.GetTitle())
		c.Summary = firstNonEmpty(c.Summary, o.GetSummary())
		c.Description = firstNonEmpty(c.Description, o.GetDescription())
		if c.ExternalDocs == nil {
			c.ExternalDocs = ConvertDocs(o.GetExternalDocs())
		}
		for _, t := range o.GetTags() {
			c.Tags = AddTag(c.Tags, ConvertTag(t))
		}
		for _, p := range o.GetParameters() {
			if !containsString(ch.Params, p.GetName()) {
				return nil, Errorf(d, "parameter %q does not appear in address %q", p.GetName(), spec.Address)
			}
			ch.paramOpts[p.GetName()] = p // annotations override protocol defaults
		}
		w := fmt.Sprintf("%s: channel %q", Position(d), ch.ID)
		if err := decodeBindings(w, b.Params.AsyncAPIVersion, "channel", o.GetBindings(), &c.Bindings); err != nil {
			return nil, err
		}
		if err := decodeExtensions(w, o.GetExtensions(), &c.Extensions); err != nil {
			return nil, err
		}
	}
	for _, name := range spec.Servers {
		if _, ok := b.Servers[name]; !ok {
			return nil, Errorf(d, "unknown server %q", name)
		}
		if !ch.servers[name] {
			ch.servers[name] = true
			c.Servers = append(c.Servers, asyncapi.Ref("#/servers/"+name))
		}
	}
	return ch, nil
}

// ReplyChannel returns a channel with an unknown (dynamic) address, e.g. a
// reply inbox.
func (b *Builder) ReplyChannel(id, description string) *Channel {
	if ch, ok := b.channels.Get(id); ok {
		return ch
	}
	ch := &Channel{ID: id, Obj: &asyncapi.Channel{Description: description}, paramOpts: map[string]*asyncapiv3.Parameter{}, servers: map[string]bool{}}
	b.channels.Set(id, ch)
	b.Doc.Channels.Set(id, ch.Obj)
	return ch
}

// Channels returns the channels in creation order.
func (b *Builder) Channels() []*Channel {
	var out []*Channel
	for _, id := range b.channels.Keys() {
		ch, _ := b.channels.Get(id)
		out = append(out, ch)
	}
	return out
}

// ChannelServers returns the servers of an rpc's channel: those of its
// operation channel metadata, else of its service.
func ChannelServers(s *protogen.Service, m *protogen.Method) []string {
	if sv := OperationOptions(m).GetChannel().GetServers(); len(sv) > 0 {
		return sv
	}
	return ServiceOptions(s).GetServers()
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

// OpSpec describes an operation requested by a protocol.
type OpSpec struct {
	Service *protogen.Service
	Method  *protogen.Method
	// DefaultID is used unless (asyncapi.v3.operation).operation_id is set.
	DefaultID string
	Action    asyncapi.Action
	Channel   *Channel
	Payload   *Message
	// DefaultSummary is used when neither the rpc comment nor the
	// annotations provide one.
	DefaultSummary string
	// Tags are protocol tags, added after the service tag.
	Tags []*asyncapi.Tag
	// Bindings are protocol bindings of the operation.
	Bindings *asyncapi.Bindings
	// IDSuffix is appended to the operation id, for a second operation of
	// the same rpc (e.g. ".output" of a processor).
	IDSuffix string
}

// AddOperation creates an operation for an rpc, applying the
// (asyncapi.v3.service) and (asyncapi.v3.operation) annotations.
func (b *Builder) AddOperation(spec OpSpec) (*Op, error) {
	m, s := spec.Method, spec.Service
	opt, svcOpt := OperationOptions(m), ServiceOptions(s)
	id := firstNonEmpty(opt.GetOperationId(), spec.DefaultID) + spec.IDSuffix
	if !asyncapi.KeyPattern.MatchString(id) {
		return nil, Errorf(m.Desc, "operation id %q is invalid (allowed: letters, digits, '.', '_' and '-')", id)
	}
	if prev, ok := b.opIDs[id]; ok {
		return nil, Errorf(m.Desc, "operation id %q is already used by %s; set (asyncapi.v3.operation).operation_id", id, prev.FullName())
	}
	b.opIDs[id] = m.Desc

	op := &asyncapi.Operation{
		Action:       spec.Action,
		Channel:      spec.Channel.Ref(),
		Title:        opt.GetTitle(),
		ExternalDocs: ConvertDocs(opt.GetExternalDocs()),
		Bindings:     spec.Bindings,
		Extensions:   asyncapi.Extensions{},
	}
	if spec.Payload != nil {
		op.Messages = []*asyncapi.Reference{spec.Channel.MessageRef(spec.Payload)}
	}
	op.Summary, op.Description = summarize(describe(m.Comments))
	if op.Summary == "" {
		op.Summary = spec.DefaultSummary
	}
	op.Summary = firstNonEmpty(opt.GetSummary(), op.Summary)
	op.Description = firstNonEmpty(opt.GetDescription(), op.Description)
	if op.ExternalDocs == nil {
		op.ExternalDocs = ConvertDocs(svcOpt.GetExternalDocs())
	}
	op.Tags = b.ServiceTags(s)
	for _, t := range spec.Tags {
		op.Tags = AddTag(op.Tags, t)
	}
	for _, t := range append(ConvertTags(svcOpt.GetTags()), ConvertTags(opt.GetTags())...) {
		op.Tags = AddTag(op.Tags, t)
	}
	security := opt.GetSecurity()
	if len(security) == 0 {
		security = svcOpt.GetSecurity()
	}
	for _, name := range security {
		ref, err := b.SecurityRef(name)
		if err != nil {
			return nil, Errorf(m.Desc, "%v", err)
		}
		op.Security = append(op.Security, ref)
	}
	w := fmt.Sprintf("%s: operation %q", Position(m.Desc), id)
	if err := decodeBindings(w, b.Params.AsyncAPIVersion, "operation", opt.GetBindings(), &op.Bindings); err != nil {
		return nil, err
	}
	if err := decodeExtensions(w, opt.GetExtensions(), &op.Extensions); err != nil {
		return nil, err
	}
	if isDeprecated(m.Desc) {
		op.Extensions["x-deprecated"] = true
	}
	op.Extensions["x-protobuf-method"] = string(m.Desc.FullName())

	b.Doc.Operations.Set(id, op)
	oi := &Op{ID: id, Obj: op, Channel: spec.Channel, Service: s, Method: m, Payload: spec.Payload}
	b.ops = append(b.ops, oi)
	return oi, nil
}

// SetReply makes an operation a request/reply operation answered on reply
// with msgs. The (asyncapi.v3.operation).reply_address annotation, if any,
// is applied.
func (b *Builder) SetReply(op *Op, reply *Channel, msgs ...*Message) {
	op.Obj.Reply = &asyncapi.OperationReply{Channel: reply.Ref()}
	for _, m := range msgs {
		reply.AddMessage(m)
		op.Obj.Reply.Messages = append(op.Obj.Reply.Messages, reply.MessageRef(m))
	}
	if op.Method != nil {
		if a := OperationOptions(op.Method).GetReplyAddress(); a != nil {
			op.Obj.Reply.Address = &asyncapi.OperationReplyAddress{Description: a.GetDescription(), Location: a.GetLocation()}
		}
	}
}

// AddReplyMessage adds another possible reply message (e.g. an error).
func (b *Builder) AddReplyMessage(op *Op, reply *Channel, m *Message) {
	reply.AddMessage(m)
	op.Obj.Reply.Messages = append(op.Obj.Reply.Messages, reply.MessageRef(m))
}

// Ops returns the operations in creation order.
func (b *Builder) Ops() []*Op { return b.ops }

// OperationIDTaken reports whether an operation id is used.
func (b *Builder) OperationIDTaken(id string) bool {
	_, ok := b.opIDs[id]
	return ok
}

// AddSyntheticOperation adds an operation that does not come from an rpc
// (e.g. a discovery endpoint).
func (b *Builder) AddSyntheticOperation(id string, op *asyncapi.Operation, ch *Channel, payload *Message) *Op {
	b.Doc.Operations.Set(id, op)
	b.opIDs[id] = nil
	oi := &Op{ID: id, Obj: op, Channel: ch, Payload: payload}
	b.ops = append(b.ops, oi)
	return oi
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

// FindMessage finds a message of the request by full name.
func (b *Builder) FindMessage(name protoreflect.FullName) *protogen.Message {
	var found *protogen.Message
	for _, f := range b.Plugin.Files {
		if found != nil {
			break
		}
		WalkMessages(f.Messages, func(m *protogen.Message) {
			if found == nil && m.Desc.FullName() == name {
				found = m
			}
		})
	}
	return found
}

// Message returns the component message describing a Protobuf message,
// applying the (asyncapi.v3.message) and (asyncapi.v3.field) annotations.
func (b *Builder) Message(m *protogen.Message) (*Message, error) {
	if mi, ok := b.msgByName[m.Desc.FullName()]; ok {
		return mi, nil
	}
	opt := MessageOptions(m)
	id := string(m.Desc.FullName())
	msg := &asyncapi.Message{
		Name:         firstNonEmpty(opt.GetName(), string(m.Desc.Name())),
		Title:        opt.GetTitle(),
		ContentType:  opt.GetContentType(),
		Tags:         ConvertTags(opt.GetTags()),
		ExternalDocs: ConvertDocs(opt.GetExternalDocs()),
		Deprecated:   isDeprecated(m.Desc),
		Extensions:   asyncapi.Extensions{"x-protobuf-message": id},
	}
	msg.Summary, msg.Description = summarize(describe(m.Comments))
	msg.Summary = firstNonEmpty(opt.GetSummary(), msg.Summary)
	msg.Description = firstNonEmpty(opt.GetDescription(), msg.Description)
	if msg.ContentType == b.Doc.DefaultContentType {
		msg.ContentType = ""
	}
	w := fmt.Sprintf("%s: message %s", Position(m.Desc), id)
	if err := decodeBindings(w, b.Params.AsyncAPIVersion, "message", opt.GetBindings(), &msg.Bindings); err != nil {
		return nil, err
	}
	if err := decodeExtensions(w, opt.GetExtensions(), &msg.Extensions); err != nil {
		return nil, err
	}

	override, err := payloadOverride(w, opt.GetPayloadSchema(), opt.GetPayloadSchemaFormat())
	if err != nil {
		return nil, err
	}
	if override != nil {
		msg.Payload = override
	} else if msg.Payload, err = b.payload(m); err != nil {
		return nil, err
	}

	mi := &Message{ID: id, Obj: msg, Proto: m, headers: asyncapi.NewMap[*asyncapi.Schema](), required: map[string]bool{}, overridden: override != nil}
	for _, h := range opt.GetHeaders() {
		if h.GetName() == "" {
			return nil, Errorf(m.Desc, "header name is required")
		}
		s := &asyncapi.Schema{Type: "string", Description: h.GetDescription(), Pattern: h.GetPattern(), Format: h.GetFormat()}
		for _, e := range h.GetEnum() {
			s.Enum = append(s.Enum, e)
		}
		for _, e := range h.GetExamples() {
			s.Examples = append(s.Examples, e)
		}
		mi.AddHeader(h.GetName(), s, h.GetRequired())
	}

	// Correlation ID.
	if loc := opt.GetCorrelationId(); loc != "" {
		if !asyncapi.RuntimeExpression.MatchString(loc) {
			return nil, Errorf(m.Desc, "correlation_id %q is not a runtime expression such as \"$message.header#/Trace-Id\" or \"$message.payload#/id\"", loc)
		}
		msg.CorrelationID = &asyncapi.CorrelationID{Location: loc, Description: opt.GetCorrelationIdDescription()}
	}
	for _, f := range m.Fields {
		if fieldOptions(f).GetCorrelationId() && override != nil {
			return nil, Errorf(f.Desc, "correlation_id: the payload of %s is replaced by payload_schema, so set (asyncapi.v3.message).correlation_id instead", m.Desc.Name())
		}
		if fieldOptions(f).GetCorrelationId() {
			if msg.CorrelationID != nil {
				return nil, Errorf(f.Desc, "message %s already has a correlation ID", m.Desc.Name())
			}
			msg.CorrelationID = &asyncapi.CorrelationID{
				Description: firstNonEmpty(opt.GetCorrelationIdDescription(), firstLine(describe(f.Comments))),
				Location:    "$message.payload#/" + JSONPointerEscape(b.schemas.fieldName(f)),
			}
		}
	}

	// Examples.
	for i, ex := range opt.GetExamples() {
		out := &asyncapi.MessageExample{Name: ex.GetName(), Summary: ex.GetSummary()}
		if ex.GetPayload() != "" {
			var v any
			if err := json.Unmarshal([]byte(ex.GetPayload()), &v); err != nil {
				return nil, Errorf(m.Desc, "example %d: payload is not valid JSON: %v", i+1, err)
			}
			out.Payload = v
		}
		if len(ex.GetHeaders()) > 0 {
			out.Headers = map[string]any{}
			for k, v := range ex.GetHeaders() {
				out.Headers[k] = v
			}
		}
		msg.Examples = append(msg.Examples, out)
	}
	if len(msg.Examples) == 0 && override == nil {
		if ex, err := b.schemas.autoExample(m); err != nil {
			return nil, err
		} else if ex != nil {
			msg.Examples = []*asyncapi.MessageExample{{Payload: ex}}
		}
	}

	b.msgByName[m.Desc.FullName()] = mi
	b.messages.Set(id, mi)
	return mi, nil
}

// SyntheticMessage returns (creating once) a message that does not come
// from Protobuf, with a JSON payload schema stored under components.schemas.
func (b *Builder) SyntheticMessage(id string, msg *asyncapi.Message, schema *asyncapi.Schema) *Message {
	if mi, ok := b.messages.Get(id); ok {
		return mi
	}
	if schema != nil {
		b.schemas.schemas.Set(id, schema)
		msg.Payload = asyncapi.SchemaRef(id)
		if msg.ContentType == "" && b.Doc.DefaultContentType != "application/json" {
			msg.ContentType = "application/json"
		}
	}
	mi := &Message{ID: id, Obj: msg, headers: asyncapi.NewMap[*asyncapi.Schema](), required: map[string]bool{}}
	b.messages.Set(id, mi)
	return mi
}

// Schema returns the payload schema of a message (a $ref), registering it.
func (b *Builder) Schema(m *protogen.Message) (*asyncapi.Schema, error) {
	return b.schemas.messageRef(m)
}

// FieldSchema returns the schema of a field, as it appears in its
// message's schema.
func (b *Builder) FieldSchema(f *protogen.Field) (*asyncapi.Schema, error) {
	return b.schemas.fieldSchema(f)
}

// SetSchema stores a named component schema.
func (b *Builder) SetSchema(name string, s *asyncapi.Schema) { b.schemas.schemas.Set(name, s) }

// FieldName returns the JSON property name of a field.
func (b *Builder) FieldName(f *protogen.Field) string { return b.schemas.fieldName(f) }

// FieldExamples returns the (asyncapi.v3.field) examples of a field.
func FieldExamples(f *protogen.Field) []string { return fieldOptions(f).GetExamples() }

// payload returns the payload schema of a message in the configured format.
func (b *Builder) payload(m *protogen.Message) (any, error) {
	if b.Params.Payload == "protobuf" {
		src, syntax, err := printProto(m)
		if err != nil {
			return nil, Errorf(m.Desc, "%v", err)
		}
		return &asyncapi.MultiFormatSchema{
			SchemaFormat: "application/vnd.google.protobuf;version=" + syntax,
			Schema:       src,
		}, nil
	}
	return b.schemas.messageRef(m)
}

// ---------------------------------------------------------------------------
// Finishing touches
// ---------------------------------------------------------------------------

func (b *Builder) finish() error {
	// Parameters: annotations and protocol defaults first, then bind to
	// payload fields.
	for _, ch := range b.Channels() {
		if len(ch.Params) == 0 {
			continue
		}
		ch.Obj.Parameters = asyncapi.NewMap[*asyncapi.Parameter]()
		for _, name := range ch.Params {
			p := &asyncapi.Parameter{}
			if o := ch.paramOpts[name]; o != nil {
				p = &asyncapi.Parameter{
					Description: o.GetDescription(), Enum: o.GetEnum(), Default: o.GetDefault(),
					Examples: o.GetExamples(), Location: o.GetLocation(),
				}
				if p.Location != "" && !asyncapi.RuntimeExpression.MatchString(p.Location) {
					return Errorf(ch.DeclaredBy, "parameter %q: location %q is not a runtime expression such as \"$message.payload#/id\"", name, p.Location)
				}
			}
			b.bindParameter(ch, name, p)
			ch.Obj.Parameters.Set(name, p)
		}
	}

	// Message headers.
	for _, id := range b.messages.Keys() {
		mi, _ := b.messages.Get(id)
		if mi.headers.Len() == 0 {
			continue
		}
		h := &asyncapi.Schema{Type: "object", Properties: mi.headers}
		for _, k := range mi.headers.Keys() {
			if mi.required[k] {
				h.Required = append(h.Required, k)
			}
		}
		mi.Obj.Headers = h
	}

	// Unless trimmed, every message and enum of the documented files gets a
	// schema, whether or not an operation references it.
	if !b.Params.TrimUnusedSchemas && b.Params.Payload == "jsonschema" {
		if err := b.schemas.addAll(b.Files); err != nil {
			return err
		}
	}

	// Components.
	if b.schemas.schemas.Len() > 0 {
		b.Components().Schemas = b.schemas.schemas
	}
	if b.messages.Len() > 0 {
		msgs := asyncapi.NewMap[*asyncapi.Message]()
		for _, id := range b.messages.Keys() {
			mi, _ := b.messages.Get(id)
			msgs.Set(id, mi.Obj)
		}
		b.Components().Messages = msgs
	}
	for _, k := range b.infoTags.Keys() {
		t, _ := b.infoTags.Get(k)
		b.Doc.Info.Tags = append(b.Doc.Info.Tags, t)
	}
	for _, id := range b.Doc.Operations.Keys() {
		op, _ := b.Doc.Operations.Get(id)
		if len(op.Extensions) == 0 {
			op.Extensions = nil
		}
	}
	if b.Doc.Channels.Len() == 0 {
		b.Doc.Channels = nil
	}
	if b.Doc.Operations.Len() == 0 {
		b.Doc.Operations = nil
	}
	return nil
}

// bindParameter derives the location, description and allowed values of an
// address parameter from a payload field with a matching name.
func (b *Builder) bindParameter(ch *Channel, name string, p *asyncapi.Parameter) {
	for _, mi := range ch.Messages {
		if mi.Proto == nil || mi.overridden {
			continue
		}
		for _, f := range mi.Proto.Fields {
			if normalizeName(string(f.Desc.Name())) != normalizeName(name) || f.Desc.IsList() || f.Desc.IsMap() {
				continue
			}
			if p.Location == "" {
				p.Location = "$message.payload#/" + JSONPointerEscape(b.schemas.fieldName(f))
			}
			if p.Description == "" {
				p.Description = firstLine(describe(f.Comments))
			}
			if len(p.Enum) == 0 && f.Enum != nil && b.Params.EnumValues == EnumNames {
				for _, v := range f.Enum.Values {
					p.Enum = append(p.Enum, string(v.Desc.Name()))
				}
			}
			if len(p.Examples) == 0 {
				for _, e := range fieldOptions(f).GetExamples() {
					var v any
					if decodeInto(e, &v) == nil {
						p.Examples = append(p.Examples, fmt.Sprint(v))
					}
				}
			}
			return
		}
	}
}

func containsString(list []string, s string) bool {
	for _, e := range list {
		if e == s {
			return true
		}
	}
	return false
}
