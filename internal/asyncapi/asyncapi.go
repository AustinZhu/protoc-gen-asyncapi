// Package asyncapi models the parts of an AsyncAPI 3.0/3.1 document the plugin
// emits, and encodes it as YAML or JSON.
//
// Struct field order is the key order of the marshaled document; maps use Map
// so output never depends on Go map iteration order.
package asyncapi

// Versions are the AsyncAPI specification versions the plugin can emit. They
// differ only in bindings the plugin does not use, so documents are otherwise
// identical.
var Versions = []string{"3.1.0", "3.0.0"}

// DefaultVersion is the AsyncAPI version emitted unless asyncapi_version is set.
const DefaultVersion = "3.1.0"

// BindingVersion is the version of the (unofficial) x-temporal binding objects.
const BindingVersion = "0.1.0"

// BindingKey is the Bindings Object key of the Temporal binding. AsyncAPI has
// no official Temporal binding and the Bindings Object only admits known
// protocols or specification extensions, hence the x- prefix.
const BindingKey = "x-temporal"

type Document struct {
	AsyncAPI           string           `json:"asyncapi" yaml:"asyncapi"`
	ID                 string           `json:"id,omitempty" yaml:"id,omitempty"`
	Info               Info             `json:"info" yaml:"info"`
	Servers            *Map[*Server]    `json:"servers,omitempty" yaml:"servers,omitempty"`
	DefaultContentType string           `json:"defaultContentType,omitempty" yaml:"defaultContentType,omitempty"`
	Channels           *Map[*Channel]   `json:"channels,omitempty" yaml:"channels,omitempty"`
	Operations         *Map[*Operation] `json:"operations,omitempty" yaml:"operations,omitempty"`
	Components         *Components      `json:"components,omitempty" yaml:"components,omitempty"`
}

type Info struct {
	Title       string `json:"title" yaml:"title"`
	Version     string `json:"version" yaml:"version"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Tags        []Tag  `json:"tags,omitempty" yaml:"tags,omitempty"`
}

type Tag struct {
	Name         string        `json:"name" yaml:"name"`
	Description  string        `json:"description,omitempty" yaml:"description,omitempty"`
	ExternalDocs *ExternalDocs `json:"externalDocs,omitempty" yaml:"externalDocs,omitempty"`
}

type ExternalDocs struct {
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	URL         string `json:"url" yaml:"url"`
}

type Server struct {
	Host        string `json:"host" yaml:"host"`
	Protocol    string `json:"protocol" yaml:"protocol"`
	Pathname    string `json:"pathname,omitempty" yaml:"pathname,omitempty"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Namespace   string `json:"x-temporal-namespace,omitempty" yaml:"x-temporal-namespace,omitempty"`
}

// Ref is a Reference Object.
type Ref struct {
	Ref string `json:"$ref" yaml:"$ref"`
}

type Channel struct {
	Address     string           `json:"address" yaml:"address"`
	Title       string           `json:"title,omitempty" yaml:"title,omitempty"`
	Summary     string           `json:"summary,omitempty" yaml:"summary,omitempty"`
	Description string           `json:"description,omitempty" yaml:"description,omitempty"`
	Messages    *Map[Ref]        `json:"messages,omitempty" yaml:"messages,omitempty"`
	Parameters  *Map[*Parameter] `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Tags        []Tag            `json:"tags,omitempty" yaml:"tags,omitempty"`
	Bindings    *Ref             `json:"bindings,omitempty" yaml:"bindings,omitempty"`
}

type Parameter struct {
	Description string   `json:"description,omitempty" yaml:"description,omitempty"`
	Examples    []string `json:"examples,omitempty" yaml:"examples,omitempty"`
}

type Operation struct {
	Action      string `json:"action" yaml:"action"`
	Channel     Ref    `json:"channel" yaml:"channel"`
	Title       string `json:"title,omitempty" yaml:"title,omitempty"`
	Summary     string `json:"summary,omitempty" yaml:"summary,omitempty"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Tags        []Tag  `json:"tags,omitempty" yaml:"tags,omitempty"`
	Bindings    *Ref   `json:"bindings,omitempty" yaml:"bindings,omitempty"`
	Messages    []Ref  `json:"messages,omitempty" yaml:"messages,omitempty"`
	Reply       *Reply `json:"reply,omitempty" yaml:"reply,omitempty"`
}

type Reply struct {
	Channel  *Ref  `json:"channel,omitempty" yaml:"channel,omitempty"`
	Messages []Ref `json:"messages,omitempty" yaml:"messages,omitempty"`
}

type Bindings struct {
	Temporal *TemporalBinding `json:"x-temporal,omitempty" yaml:"x-temporal,omitempty"`
}

// TemporalBinding carries the statically declared Temporal settings of an
// operation. Channel bindings carry only the identifying fields; operation
// bindings add timeouts, retry policy and handler linkage.
type TemporalBinding struct {
	Kind           string       `json:"kind" yaml:"kind"`
	Name           string       `json:"name" yaml:"name"`
	TaskQueue      string       `json:"taskQueue,omitempty" yaml:"taskQueue,omitempty"`
	ContinueAsNew  bool         `json:"continueAsNew,omitempty" yaml:"continueAsNew,omitempty"`
	Workflows      []string     `json:"workflows,omitempty" yaml:"workflows,omitempty"`
	Timeouts       *Timeouts    `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`
	RetryPolicy    *RetryPolicy `json:"retryPolicy,omitempty" yaml:"retryPolicy,omitempty"`
	BindingVersion string       `json:"bindingVersion" yaml:"bindingVersion"`
}

type Timeouts struct {
	Execution       string `json:"execution,omitempty" yaml:"execution,omitempty"`
	Run             string `json:"run,omitempty" yaml:"run,omitempty"`
	Task            string `json:"task,omitempty" yaml:"task,omitempty"`
	ScheduleToClose string `json:"scheduleToClose,omitempty" yaml:"scheduleToClose,omitempty"`
	StartToClose    string `json:"startToClose,omitempty" yaml:"startToClose,omitempty"`
	ScheduleToStart string `json:"scheduleToStart,omitempty" yaml:"scheduleToStart,omitempty"`
	Heartbeat       string `json:"heartbeat,omitempty" yaml:"heartbeat,omitempty"`
}

type RetryPolicy struct {
	InitialInterval        string   `json:"initialInterval,omitempty" yaml:"initialInterval,omitempty"`
	BackoffCoefficient     float64  `json:"backoffCoefficient,omitempty" yaml:"backoffCoefficient,omitempty"`
	MaximumInterval        string   `json:"maximumInterval,omitempty" yaml:"maximumInterval,omitempty"`
	MaximumAttempts        int32    `json:"maximumAttempts,omitempty" yaml:"maximumAttempts,omitempty"`
	NonRetryableErrorTypes []string `json:"nonRetryableErrorTypes,omitempty" yaml:"nonRetryableErrorTypes,omitempty"`
}

type Components struct {
	ChannelBindings   *Map[*Bindings] `json:"channelBindings,omitempty" yaml:"channelBindings,omitempty"`
	OperationBindings *Map[*Bindings] `json:"operationBindings,omitempty" yaml:"operationBindings,omitempty"`
	Schemas           *Map[*Schema]   `json:"schemas,omitempty" yaml:"schemas,omitempty"`
	Messages          *Map[*Message]  `json:"messages,omitempty" yaml:"messages,omitempty"`
}

type Message struct {
	Name        string  `json:"name,omitempty" yaml:"name,omitempty"`
	Title       string  `json:"title,omitempty" yaml:"title,omitempty"`
	Summary     string  `json:"summary,omitempty" yaml:"summary,omitempty"`
	Description string  `json:"description,omitempty" yaml:"description,omitempty"`
	ContentType string  `json:"contentType,omitempty" yaml:"contentType,omitempty"`
	Headers     *Schema `json:"headers,omitempty" yaml:"headers,omitempty"`
	Payload     *Schema `json:"payload" yaml:"payload"`
}

// Schema is the subset of JSON Schema the converter emits. Field order is the
// order keywords appear in the marshaled output.
type Schema struct {
	Ref              string        `json:"$ref,omitempty" yaml:"$ref,omitempty"`
	Title            string        `json:"title,omitempty" yaml:"title,omitempty"`
	Description      string        `json:"description,omitempty" yaml:"description,omitempty"`
	Type             any           `json:"type,omitempty" yaml:"type,omitempty"` // string or []string
	Format           string        `json:"format,omitempty" yaml:"format,omitempty"`
	Pattern          string        `json:"pattern,omitempty" yaml:"pattern,omitempty"`
	ContentEncoding  string        `json:"contentEncoding,omitempty" yaml:"contentEncoding,omitempty"`
	Enum             []any         `json:"enum,omitempty" yaml:"enum,omitempty"`
	Const            any           `json:"const,omitempty" yaml:"const,omitempty"`
	Minimum          any           `json:"minimum,omitempty" yaml:"minimum,omitempty"`
	ExclusiveMinimum any           `json:"exclusiveMinimum,omitempty" yaml:"exclusiveMinimum,omitempty"`
	Maximum          any           `json:"maximum,omitempty" yaml:"maximum,omitempty"`
	ExclusiveMaximum any           `json:"exclusiveMaximum,omitempty" yaml:"exclusiveMaximum,omitempty"`
	MinLength        *uint64       `json:"minLength,omitempty" yaml:"minLength,omitempty"`
	MaxLength        *uint64       `json:"maxLength,omitempty" yaml:"maxLength,omitempty"`
	Items            *Schema       `json:"items,omitempty" yaml:"items,omitempty"`
	MinItems         *uint64       `json:"minItems,omitempty" yaml:"minItems,omitempty"`
	MaxItems         *uint64       `json:"maxItems,omitempty" yaml:"maxItems,omitempty"`
	UniqueItems      bool          `json:"uniqueItems,omitempty" yaml:"uniqueItems,omitempty"`
	Properties       *Map[*Schema] `json:"properties,omitempty" yaml:"properties,omitempty"`
	Required         []string      `json:"required,omitempty" yaml:"required,omitempty"`
	PropertyNames    *Schema       `json:"propertyNames,omitempty" yaml:"propertyNames,omitempty"`
	AdditionalProps  any           `json:"additionalProperties,omitempty" yaml:"additionalProperties,omitempty"` // bool or *Schema
	MinProperties    *uint64       `json:"minProperties,omitempty" yaml:"minProperties,omitempty"`
	MaxProperties    *uint64       `json:"maxProperties,omitempty" yaml:"maxProperties,omitempty"`
	OneOf            []*Schema     `json:"oneOf,omitempty" yaml:"oneOf,omitempty"`
	AnyOf            []*Schema     `json:"anyOf,omitempty" yaml:"anyOf,omitempty"`
	AllOf            []*Schema     `json:"allOf,omitempty" yaml:"allOf,omitempty"`
	Not              *Schema       `json:"not,omitempty" yaml:"not,omitempty"`
	Deprecated       bool          `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`
}

// Clone returns a shallow copy, enough to decorate a shared schema with
// per-field keywords.
func (s *Schema) Clone() *Schema {
	c := *s
	return &c
}
