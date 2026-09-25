// Package asyncapi models the parts of an AsyncAPI 3 document the plugin
// emits and builds one from discovered Temporal operations.
//
// Struct field order is the key order of the marshaled document; maps use
// ordered.Map so output never depends on Go map iteration order.
package asyncapi

import (
	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/jsonschema"
	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/ordered"
)

// Version is the AsyncAPI specification version emitted.
const Version = "3.1.0"

// BindingVersion is the version of the (unofficial) x-temporal binding objects.
const BindingVersion = "0.1.0"

// BindingKey is the Bindings Object key of the Temporal binding. AsyncAPI has
// no official Temporal binding and the Bindings Object only admits known
// protocols or specification extensions, hence the x- prefix.
const BindingKey = "x-temporal"

type Document struct {
	AsyncAPI           string                   `json:"asyncapi" yaml:"asyncapi"`
	ID                 string                   `json:"id,omitempty" yaml:"id,omitempty"`
	Info               Info                     `json:"info" yaml:"info"`
	Servers            *ordered.Map[*Server]    `json:"servers,omitempty" yaml:"servers,omitempty"`
	DefaultContentType string                   `json:"defaultContentType,omitempty" yaml:"defaultContentType,omitempty"`
	Channels           *ordered.Map[*Channel]   `json:"channels,omitempty" yaml:"channels,omitempty"`
	Operations         *ordered.Map[*Operation] `json:"operations,omitempty" yaml:"operations,omitempty"`
	Components         *Components              `json:"components,omitempty" yaml:"components,omitempty"`
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
	Address     string                   `json:"address" yaml:"address"`
	Title       string                   `json:"title,omitempty" yaml:"title,omitempty"`
	Summary     string                   `json:"summary,omitempty" yaml:"summary,omitempty"`
	Description string                   `json:"description,omitempty" yaml:"description,omitempty"`
	Messages    *ordered.Map[Ref]        `json:"messages,omitempty" yaml:"messages,omitempty"`
	Parameters  *ordered.Map[*Parameter] `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Tags        []Tag                    `json:"tags,omitempty" yaml:"tags,omitempty"`
	Bindings    *Ref                     `json:"bindings,omitempty" yaml:"bindings,omitempty"`
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
	Kind                  string       `json:"kind" yaml:"kind"`
	Name                  string       `json:"name" yaml:"name"`
	TaskQueue             string       `json:"taskQueue,omitempty" yaml:"taskQueue,omitempty"`
	SupportsContinueAsNew bool         `json:"supportsContinueAsNew,omitempty" yaml:"supportsContinueAsNew,omitempty"`
	Workflows             []string     `json:"workflows,omitempty" yaml:"workflows,omitempty"`
	Timeouts              *Timeouts    `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`
	RetryPolicy           *RetryPolicy `json:"retryPolicy,omitempty" yaml:"retryPolicy,omitempty"`
	BindingVersion        string       `json:"bindingVersion" yaml:"bindingVersion"`
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
	ChannelBindings   *ordered.Map[*Bindings]          `json:"channelBindings,omitempty" yaml:"channelBindings,omitempty"`
	OperationBindings *ordered.Map[*Bindings]          `json:"operationBindings,omitempty" yaml:"operationBindings,omitempty"`
	Schemas           *ordered.Map[*jsonschema.Schema] `json:"schemas,omitempty" yaml:"schemas,omitempty"`
	Messages          *ordered.Map[*Message]           `json:"messages,omitempty" yaml:"messages,omitempty"`
}

type Message struct {
	Name        string             `json:"name,omitempty" yaml:"name,omitempty"`
	Title       string             `json:"title,omitempty" yaml:"title,omitempty"`
	Summary     string             `json:"summary,omitempty" yaml:"summary,omitempty"`
	Description string             `json:"description,omitempty" yaml:"description,omitempty"`
	ContentType string             `json:"contentType,omitempty" yaml:"contentType,omitempty"`
	Headers     *jsonschema.Schema `json:"headers,omitempty" yaml:"headers,omitempty"`
	Payload     *jsonschema.Schema `json:"payload" yaml:"payload"`
}
