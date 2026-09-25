// Package asyncapi is a complete Go model of AsyncAPI 3.0.0 and 3.1.0
// documents, with deterministic YAML and JSON encoding.
//
// Every object of the specification is modelled. Objects that the
// specification allows to be replaced by a Reference Object carry a Ref
// field: when it is set, the object marshals as {"$ref": Ref} and every
// other field is ignored. Objects that allow specification extensions carry
// an Extensions map ("x-..." keys), rendered inline. Struct field order is the
// key order of the output; maps use Map, which keeps insertion order.
package asyncapi

// Versions are the AsyncAPI versions the model can emit. 3.1.0 only adds the
// ROS 2 bindings to 3.0.0.
var Versions = []string{"3.1.0", "3.0.0"}

// DefaultVersion is emitted unless another version is requested.
const DefaultVersion = "3.1.0"

// Extensions holds specification extensions; keys must start with "x-".
type Extensions = map[string]any

// Document is the root AsyncAPI Object.
type Document struct {
	AsyncAPI           string           `yaml:"asyncapi"`
	ID                 string           `yaml:"id,omitempty"`
	Info               Info             `yaml:"info"`
	Servers            *Map[*Server]    `yaml:"servers,omitempty"`
	DefaultContentType string           `yaml:"defaultContentType,omitempty"`
	Channels           *Map[*Channel]   `yaml:"channels,omitempty"`
	Operations         *Map[*Operation] `yaml:"operations,omitempty"`
	Components         *Components      `yaml:"components,omitempty"`
	Extensions         Extensions       `yaml:",inline"`
}

// Info is the Info Object.
type Info struct {
	Title          string        `yaml:"title"`
	Version        string        `yaml:"version"`
	Description    string        `yaml:"description,omitempty"`
	TermsOfService string        `yaml:"termsOfService,omitempty"`
	Contact        *Contact      `yaml:"contact,omitempty"`
	License        *License      `yaml:"license,omitempty"`
	Tags           []*Tag        `yaml:"tags,omitempty"`
	ExternalDocs   *ExternalDocs `yaml:"externalDocs,omitempty"`
	Extensions     Extensions    `yaml:",inline"`
}

// Contact is the Contact Object.
type Contact struct {
	Name       string     `yaml:"name,omitempty"`
	URL        string     `yaml:"url,omitempty"`
	Email      string     `yaml:"email,omitempty"`
	Extensions Extensions `yaml:",inline"`
}

// License is the License Object.
type License struct {
	Name       string     `yaml:"name"`
	URL        string     `yaml:"url,omitempty"`
	Extensions Extensions `yaml:",inline"`
}

// Reference is the Reference Object.
type Reference struct {
	Ref string `yaml:"$ref"`
}

// Ref returns a Reference Object for a JSON pointer such as
// "#/channels/orders".
func Ref(pointer string) *Reference { return &Reference{Ref: pointer} }

// Server is the Server Object (or a Reference when Ref is set).
type Server struct {
	Ref             string                `yaml:"-"`
	Host            string                `yaml:"host"`
	Protocol        string                `yaml:"protocol"`
	ProtocolVersion string                `yaml:"protocolVersion,omitempty"`
	Pathname        string                `yaml:"pathname,omitempty"`
	Title           string                `yaml:"title,omitempty"`
	Summary         string                `yaml:"summary,omitempty"`
	Description     string                `yaml:"description,omitempty"`
	Variables       *Map[*ServerVariable] `yaml:"variables,omitempty"`
	Security        []*SecurityScheme     `yaml:"security,omitempty"`
	Tags            []*Tag                `yaml:"tags,omitempty"`
	ExternalDocs    *ExternalDocs         `yaml:"externalDocs,omitempty"`
	Bindings        *Bindings             `yaml:"bindings,omitempty"`
	Extensions      Extensions            `yaml:",inline"`
}

// ServerVariable is the Server Variable Object.
type ServerVariable struct {
	Ref         string     `yaml:"-"`
	Enum        []string   `yaml:"enum,omitempty"`
	Default     string     `yaml:"default,omitempty"`
	Description string     `yaml:"description,omitempty"`
	Examples    []string   `yaml:"examples,omitempty"`
	Extensions  Extensions `yaml:",inline"`
}

// Channel is the Channel Object. A nil Address marshals as null: the address
// is unknown or dynamic (e.g. a reply inbox).
type Channel struct {
	Ref          string           `yaml:"-"`
	Address      *string          `yaml:"address"`
	Messages     *Map[*Message]   `yaml:"messages,omitempty"`
	Title        string           `yaml:"title,omitempty"`
	Summary      string           `yaml:"summary,omitempty"`
	Description  string           `yaml:"description,omitempty"`
	Servers      []*Reference     `yaml:"servers,omitempty"`
	Parameters   *Map[*Parameter] `yaml:"parameters,omitempty"`
	Tags         []*Tag           `yaml:"tags,omitempty"`
	ExternalDocs *ExternalDocs    `yaml:"externalDocs,omitempty"`
	Bindings     *Bindings        `yaml:"bindings,omitempty"`
	Extensions   Extensions       `yaml:",inline"`
}

// Parameter is the Parameter Object.
type Parameter struct {
	Ref         string     `yaml:"-"`
	Enum        []string   `yaml:"enum,omitempty"`
	Default     string     `yaml:"default,omitempty"`
	Description string     `yaml:"description,omitempty"`
	Examples    []string   `yaml:"examples,omitempty"`
	Location    string     `yaml:"location,omitempty"`
	Extensions  Extensions `yaml:",inline"`
}

// Action is the action of an Operation.
type Action string

const (
	ActionSend    Action = "send"
	ActionReceive Action = "receive"
)

// Operation is the Operation Object.
type Operation struct {
	Ref          string            `yaml:"-"`
	Action       Action            `yaml:"action"`
	Channel      *Reference        `yaml:"channel"`
	Title        string            `yaml:"title,omitempty"`
	Summary      string            `yaml:"summary,omitempty"`
	Description  string            `yaml:"description,omitempty"`
	Security     []*SecurityScheme `yaml:"security,omitempty"`
	Tags         []*Tag            `yaml:"tags,omitempty"`
	ExternalDocs *ExternalDocs     `yaml:"externalDocs,omitempty"`
	Bindings     *Bindings         `yaml:"bindings,omitempty"`
	Traits       []*OperationTrait `yaml:"traits,omitempty"`
	Messages     []*Reference      `yaml:"messages,omitempty"`
	Reply        *OperationReply   `yaml:"reply,omitempty"`
	Extensions   Extensions        `yaml:",inline"`
}

// OperationTrait is the Operation Trait Object.
type OperationTrait struct {
	Ref          string            `yaml:"-"`
	Title        string            `yaml:"title,omitempty"`
	Summary      string            `yaml:"summary,omitempty"`
	Description  string            `yaml:"description,omitempty"`
	Security     []*SecurityScheme `yaml:"security,omitempty"`
	Tags         []*Tag            `yaml:"tags,omitempty"`
	ExternalDocs *ExternalDocs     `yaml:"externalDocs,omitempty"`
	Bindings     *Bindings         `yaml:"bindings,omitempty"`
	Extensions   Extensions        `yaml:",inline"`
}

// OperationReply is the Operation Reply Object.
type OperationReply struct {
	Ref        string                 `yaml:"-"`
	Address    *OperationReplyAddress `yaml:"address,omitempty"`
	Channel    *Reference             `yaml:"channel,omitempty"`
	Messages   []*Reference           `yaml:"messages,omitempty"`
	Extensions Extensions             `yaml:",inline"`
}

// OperationReplyAddress is the Operation Reply Address Object.
type OperationReplyAddress struct {
	Ref         string     `yaml:"-"`
	Description string     `yaml:"description,omitempty"`
	Location    string     `yaml:"location"`
	Extensions  Extensions `yaml:",inline"`
}

// Message is the Message Object. Headers and Payload hold a *Schema or a
// *MultiFormatSchema.
type Message struct {
	Ref           string            `yaml:"-"`
	Headers       any               `yaml:"headers,omitempty"`
	Payload       any               `yaml:"payload,omitempty"`
	CorrelationID *CorrelationID    `yaml:"correlationId,omitempty"`
	ContentType   string            `yaml:"contentType,omitempty"`
	Name          string            `yaml:"name,omitempty"`
	Title         string            `yaml:"title,omitempty"`
	Summary       string            `yaml:"summary,omitempty"`
	Description   string            `yaml:"description,omitempty"`
	Tags          []*Tag            `yaml:"tags,omitempty"`
	ExternalDocs  *ExternalDocs     `yaml:"externalDocs,omitempty"`
	Deprecated    bool              `yaml:"deprecated,omitempty"`
	Bindings      *Bindings         `yaml:"bindings,omitempty"`
	Examples      []*MessageExample `yaml:"examples,omitempty"`
	Traits        []*MessageTrait   `yaml:"traits,omitempty"`
	Extensions    Extensions        `yaml:",inline"`
}

// MessageTrait is the Message Trait Object.
type MessageTrait struct {
	Ref           string            `yaml:"-"`
	Headers       any               `yaml:"headers,omitempty"`
	CorrelationID *CorrelationID    `yaml:"correlationId,omitempty"`
	ContentType   string            `yaml:"contentType,omitempty"`
	Name          string            `yaml:"name,omitempty"`
	Title         string            `yaml:"title,omitempty"`
	Summary       string            `yaml:"summary,omitempty"`
	Description   string            `yaml:"description,omitempty"`
	Tags          []*Tag            `yaml:"tags,omitempty"`
	ExternalDocs  *ExternalDocs     `yaml:"externalDocs,omitempty"`
	Deprecated    bool              `yaml:"deprecated,omitempty"`
	Bindings      *Bindings         `yaml:"bindings,omitempty"`
	Examples      []*MessageExample `yaml:"examples,omitempty"`
	Extensions    Extensions        `yaml:",inline"`
}

// MessageExample is the Message Example Object. It does not allow
// specification extensions.
type MessageExample struct {
	Name    string         `yaml:"name,omitempty"`
	Summary string         `yaml:"summary,omitempty"`
	Headers map[string]any `yaml:"headers,omitempty"`
	Payload any            `yaml:"payload,omitempty"`
}

// MultiFormatSchema is the Multi Format Schema Object: a schema in another
// format, e.g. "application/vnd.google.protobuf;version=3".
type MultiFormatSchema struct {
	Ref          string     `yaml:"-"`
	SchemaFormat string     `yaml:"schemaFormat"`
	Schema       any        `yaml:"schema"`
	Extensions   Extensions `yaml:",inline"`
}

// CorrelationID is the Correlation ID Object.
type CorrelationID struct {
	Ref         string     `yaml:"-"`
	Description string     `yaml:"description,omitempty"`
	Location    string     `yaml:"location"`
	Extensions  Extensions `yaml:",inline"`
}

// Tag is the Tag Object.
type Tag struct {
	Ref          string        `yaml:"-"`
	Name         string        `yaml:"name"`
	Description  string        `yaml:"description,omitempty"`
	ExternalDocs *ExternalDocs `yaml:"externalDocs,omitempty"`
	Extensions   Extensions    `yaml:",inline"`
}

// ExternalDocs is the External Documentation Object.
type ExternalDocs struct {
	Ref         string     `yaml:"-"`
	Description string     `yaml:"description,omitempty"`
	URL         string     `yaml:"url"`
	Extensions  Extensions `yaml:",inline"`
}

// Components is the Components Object.
type Components struct {
	Schemas           *Map[*Schema]                `yaml:"schemas,omitempty"`
	Servers           *Map[*Server]                `yaml:"servers,omitempty"`
	Channels          *Map[*Channel]               `yaml:"channels,omitempty"`
	Operations        *Map[*Operation]             `yaml:"operations,omitempty"`
	Messages          *Map[*Message]               `yaml:"messages,omitempty"`
	SecuritySchemes   *Map[*SecurityScheme]        `yaml:"securitySchemes,omitempty"`
	ServerVariables   *Map[*ServerVariable]        `yaml:"serverVariables,omitempty"`
	Parameters        *Map[*Parameter]             `yaml:"parameters,omitempty"`
	CorrelationIDs    *Map[*CorrelationID]         `yaml:"correlationIds,omitempty"`
	Replies           *Map[*OperationReply]        `yaml:"replies,omitempty"`
	ReplyAddresses    *Map[*OperationReplyAddress] `yaml:"replyAddresses,omitempty"`
	ExternalDocs      *Map[*ExternalDocs]          `yaml:"externalDocs,omitempty"`
	Tags              *Map[*Tag]                   `yaml:"tags,omitempty"`
	OperationTraits   *Map[*OperationTrait]        `yaml:"operationTraits,omitempty"`
	MessageTraits     *Map[*MessageTrait]          `yaml:"messageTraits,omitempty"`
	ServerBindings    *Map[*Bindings]              `yaml:"serverBindings,omitempty"`
	ChannelBindings   *Map[*Bindings]              `yaml:"channelBindings,omitempty"`
	OperationBindings *Map[*Bindings]              `yaml:"operationBindings,omitempty"`
	MessageBindings   *Map[*Bindings]              `yaml:"messageBindings,omitempty"`
	Extensions        Extensions                   `yaml:",inline"`
}

// SecurityScheme is the Security Scheme Object. Which fields are allowed
// depends on Type; see Validate.
type SecurityScheme struct {
	Ref              string      `yaml:"-"`
	Type             string      `yaml:"type"`
	Description      string      `yaml:"description,omitempty"`
	Name             string      `yaml:"name,omitempty"`
	In               string      `yaml:"in,omitempty"`
	Scheme           string      `yaml:"scheme,omitempty"`
	BearerFormat     string      `yaml:"bearerFormat,omitempty"`
	Flows            *OAuthFlows `yaml:"flows,omitempty"`
	OpenIDConnectURL string      `yaml:"openIdConnectUrl,omitempty"`
	Scopes           []string    `yaml:"scopes,omitempty"`
	Extensions       Extensions  `yaml:",inline"`
}

// Security scheme types.
const (
	SecurityUserPassword         = "userPassword"
	SecurityAPIKey               = "apiKey"
	SecurityX509                 = "X509"
	SecuritySymmetricEncryption  = "symmetricEncryption"
	SecurityAsymmetricEncryption = "asymmetricEncryption"
	SecurityHTTPAPIKey           = "httpApiKey"
	SecurityHTTP                 = "http"
	SecurityOAuth2               = "oauth2"
	SecurityOpenIDConnect        = "openIdConnect"
	SecurityPlain                = "plain"
	SecurityScramSHA256          = "scramSha256"
	SecurityScramSHA512          = "scramSha512"
	SecurityGSSAPI               = "gssapi"
)

// OAuthFlows is the OAuth Flows Object. It does not allow specification
// extensions (each OAuthFlow does).
type OAuthFlows struct {
	Implicit          *OAuthFlow `yaml:"implicit,omitempty"`
	Password          *OAuthFlow `yaml:"password,omitempty"`
	ClientCredentials *OAuthFlow `yaml:"clientCredentials,omitempty"`
	AuthorizationCode *OAuthFlow `yaml:"authorizationCode,omitempty"`
}

// OAuthFlow is the OAuth Flow Object.
type OAuthFlow struct {
	AuthorizationURL string       `yaml:"authorizationUrl,omitempty"`
	TokenURL         string       `yaml:"tokenUrl,omitempty"`
	RefreshURL       string       `yaml:"refreshUrl,omitempty"`
	AvailableScopes  *Map[string] `yaml:"availableScopes,omitempty"`
	Extensions       Extensions   `yaml:",inline"`
}

// Bindings is a Server, Channel, Operation or Message Bindings Object: a map
// from protocol ("kafka", "nats", "x-temporal", ...) to its binding object.
type Bindings struct {
	Ref    string
	Values *Map[any]
}

// NewBindings returns bindings holding one protocol binding.
func NewBindings(protocol string, binding any) *Bindings {
	b := &Bindings{}
	b.Set(protocol, binding)
	return b
}

// Set adds or replaces the binding of a protocol.
func (b *Bindings) Set(protocol string, binding any) {
	if b.Values == nil {
		b.Values = NewMap[any]()
	}
	b.Values.Set(protocol, binding)
}

// Get returns the binding of a protocol.
func (b *Bindings) Get(protocol string) (any, bool) {
	if b == nil {
		return nil, false
	}
	return b.Values.Get(protocol)
}

// Schema is the AsyncAPI Schema Object: JSON Schema Draft 07 plus the
// AsyncAPI vocabulary (discriminator, externalDocs, deprecated). A Ref
// renders as a "$ref" keyword; unlike other objects, siblings are kept.
type Schema struct {
	Ref                  string        `yaml:"$ref,omitempty"`
	ID                   string        `yaml:"$id,omitempty"`
	SchemaURI            string        `yaml:"$schema,omitempty"`
	Comment              string        `yaml:"$comment,omitempty"`
	Title                string        `yaml:"title,omitempty"`
	Description          string        `yaml:"description,omitempty"`
	Type                 any           `yaml:"type,omitempty"` // string or []string
	Format               string        `yaml:"format,omitempty"`
	ContentMediaType     string        `yaml:"contentMediaType,omitempty"`
	ContentEncoding      string        `yaml:"contentEncoding,omitempty"`
	Const                any           `yaml:"const,omitempty"`
	Enum                 []any         `yaml:"enum,omitempty"`
	Default              any           `yaml:"default,omitempty"`
	MultipleOf           any           `yaml:"multipleOf,omitempty"`
	Minimum              any           `yaml:"minimum,omitempty"`
	ExclusiveMinimum     any           `yaml:"exclusiveMinimum,omitempty"`
	Maximum              any           `yaml:"maximum,omitempty"`
	ExclusiveMaximum     any           `yaml:"exclusiveMaximum,omitempty"`
	MinLength            *uint64       `yaml:"minLength,omitempty"`
	MaxLength            *uint64       `yaml:"maxLength,omitempty"`
	Pattern              string        `yaml:"pattern,omitempty"`
	Items                any           `yaml:"items,omitempty"` // *Schema or []*Schema
	AdditionalItems      any           `yaml:"additionalItems,omitempty"`
	Contains             *Schema       `yaml:"contains,omitempty"`
	MinItems             *uint64       `yaml:"minItems,omitempty"`
	MaxItems             *uint64       `yaml:"maxItems,omitempty"`
	UniqueItems          bool          `yaml:"uniqueItems,omitempty"`
	Properties           *Map[*Schema] `yaml:"properties,omitempty"`
	PatternProperties    *Map[*Schema] `yaml:"patternProperties,omitempty"`
	PropertyNames        *Schema       `yaml:"propertyNames,omitempty"`
	AdditionalProperties any           `yaml:"additionalProperties,omitempty"` // bool or *Schema
	MinProperties        *uint64       `yaml:"minProperties,omitempty"`
	MaxProperties        *uint64       `yaml:"maxProperties,omitempty"`
	Required             []string      `yaml:"required,omitempty"`
	Dependencies         *Map[any]     `yaml:"dependencies,omitempty"`
	AllOf                []*Schema     `yaml:"allOf,omitempty"`
	AnyOf                []*Schema     `yaml:"anyOf,omitempty"`
	OneOf                []*Schema     `yaml:"oneOf,omitempty"`
	Not                  *Schema       `yaml:"not,omitempty"`
	If                   *Schema       `yaml:"if,omitempty"`
	Then                 *Schema       `yaml:"then,omitempty"`
	Else                 *Schema       `yaml:"else,omitempty"`
	Definitions          *Map[*Schema] `yaml:"definitions,omitempty"`
	ReadOnly             bool          `yaml:"readOnly,omitempty"`
	WriteOnly            bool          `yaml:"writeOnly,omitempty"`
	Deprecated           bool          `yaml:"deprecated,omitempty"`
	Discriminator        string        `yaml:"discriminator,omitempty"`
	ExternalDocs         *ExternalDocs `yaml:"externalDocs,omitempty"`
	Examples             []any         `yaml:"examples,omitempty"`
	Extensions           Extensions    `yaml:",inline"`
}

// Clone returns a shallow copy of s.
func (s *Schema) Clone() *Schema {
	c := *s
	return &c
}

// SchemaRef returns a schema referencing a component schema.
func SchemaRef(name string) *Schema { return &Schema{Ref: "#/components/schemas/" + name} }

// Reference constructors for objects that may be replaced by a Reference.

func ServerRef(name string) *Server { return &Server{Ref: "#/servers/" + name} }

// SecuritySchemeRef references a component security scheme.
func SecuritySchemeRef(name string) *SecurityScheme {
	return &SecurityScheme{Ref: "#/components/securitySchemes/" + name}
}

// MessageRef references a component message.
func MessageRef(id string) *Message { return &Message{Ref: "#/components/messages/" + id} }
