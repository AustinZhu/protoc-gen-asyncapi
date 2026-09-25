// Package temporal documents Temporal Workflows, Activities, Signals,
// Queries and Updates declared as annotated Protobuf services.
package temporal

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
	temporalv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/temporal/asyncapi/v1"
)

// BindingKey is the key of the Temporal binding. AsyncAPI has no official
// Temporal binding and bindings objects only admit known protocols or
// specification extensions, hence the x- prefix.
const BindingKey = "x-temporal"

// BindingVersion is the version of the x-temporal binding objects.
const BindingVersion = "0.1.0"

// Protocol implements core.Protocol for Temporal.
type Protocol struct {
	// seen detects duplicate (kind, name) pairs across the documents of a
	// run.
	seen duplicates
	// ops are the operations discovered for the current document, by rpc.
	ops map[protoreflect.FullName]*Operation
	// grouped are workflow names that have handlers linked to them.
	grouped map[string]bool
	// serverURL and namespace are the server_url and namespace options.
	serverURL, namespace string
}

// New returns the Temporal protocol.
func New() core.Protocol { return &Protocol{seen: duplicates{}} }

// Plugin is protoc-gen-temporal-asyncapi.
var Plugin = core.Plugin{
	Name:      "protoc-gen-temporal-asyncapi",
	Protocols: func() []core.Protocol { return []core.Protocol{New()} },
}

func (p *Protocol) Name() string { return "temporal" }

func (p *Protocol) Options() []core.Option {
	str := func(dst *string) func(string) error { return func(v string) error { *dst = v; return nil } }
	return []core.Option{
		{Name: "server_url", Usage: "Temporal frontend address (host:port or URL); adds a server named temporal", Set: str(&p.serverURL)},
		{Name: "namespace", Usage: "default Temporal namespace of the document's Temporal servers", Set: str(&p.namespace)},
	}
}

// ServerName is the name of the server added by the server_url option.
const ServerName = "temporal"

// optionServer returns the server described by the server_url option.
func optionServer(raw string) (*asyncapi.Server, error) {
	srv := &asyncapi.Server{Protocol: "temporal", Host: raw, Description: "Temporal Frontend Service."}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("server_url: %w", err)
		}
		if u.Host == "" {
			return nil, fmt.Errorf("server_url %q has no host", raw)
		}
		srv.Host = u.Host
		if u.Path != "" && u.Path != "/" {
			srv.Pathname = u.Path
		}
	}
	return srv, nil
}

// Claims documents services with Temporal annotations.
func (p *Protocol) Claims(_ *core.Builder, s *protogen.Service) bool {
	if ServiceOptions(s.Desc) != nil {
		return true
	}
	for _, m := range s.Methods {
		if OperationOptions(m.Desc) != nil {
			return true
		}
	}
	return false
}

func (p *Protocol) HasContent(*core.Builder) bool { return false }

func documentOptions(f *protogen.File) *temporalv1.Document {
	return core.Extension[*temporalv1.Document](f.Desc.Options(), temporalv1.E_Document)
}

// Begin adds namespaces to servers and discovers every claimed service, so
// all annotation problems of a document are reported at once.
func (p *Protocol) Begin(b *core.Builder) error {
	if p.serverURL != "" {
		if _, ok := b.Doc.Servers.Get(ServerName); ok {
			return fmt.Errorf("server_url: a server named %q is already declared", ServerName)
		}
		srv, err := optionServer(p.serverURL)
		if err != nil {
			return err
		}
		if b.Doc.Servers == nil {
			b.Doc.Servers = asyncapi.NewMap[*asyncapi.Server]()
		}
		b.Doc.Servers.Set(ServerName, srv)
	}
	for _, f := range b.Imports() {
		for _, srv := range documentOptions(f).GetServers() {
			out, ok := b.Doc.Servers.Get(srv.GetName())
			if !ok {
				return fmt.Errorf("%s: temporal server %q is not declared in the servers of the asyncapi.v3.document file option", f.Desc.Path(), srv.GetName())
			}
			if srv.GetNamespace() != "" {
				if out.Extensions == nil {
					out.Extensions = asyncapi.Extensions{}
				}
				out.Extensions["x-temporal-namespace"] = srv.GetNamespace()
			}
		}
	}
	if p.namespace != "" && b.Doc.Servers != nil {
		for _, name := range b.Doc.Servers.Keys() {
			out, _ := b.Doc.Servers.Get(name)
			if out.Protocol != "temporal" {
				continue
			}
			if _, ok := out.Extensions["x-temporal-namespace"]; !ok {
				if out.Extensions == nil {
					out.Extensions = asyncapi.Extensions{}
				}
				out.Extensions["x-temporal-namespace"] = p.namespace
			}
		}
	}

	p.ops = map[protoreflect.FullName]*Operation{}
	p.grouped = map[string]bool{}
	var errs []error
	var all []*Operation
	for _, f := range b.Files {
		for _, s := range f.Services {
			if b.ClaimedBy(s) != p {
				continue
			}
			ops, svcErrs := discoverService(s.Desc)
			errs = append(errs, svcErrs...)
			all = append(all, ops...)
		}
	}
	errs = append(errs, p.seen.check(all)...)
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	for _, op := range all {
		p.ops[op.Method.FullName()] = op
		for _, w := range op.Workflows {
			p.grouped[w] = true
		}
	}
	return nil
}

func (p *Protocol) Finish(*core.Builder) error { return nil }

// kindInfo describes how each Temporal primitive is rendered.
type kindInfo struct {
	tag     asyncapi.Tag
	address func(name string) string // input channel address
	summary string                   // %s is the operation name
	// targetsExecution marks handlers addressed to a running Workflow Execution.
	targetsExecution bool
}

var kindInfos = map[Kind]kindInfo{
	Workflow: {
		tag: asyncapi.Tag{Name: "Workflows", Description: "Workflow types. Starting one returns immediately; its result is delivered on the reply channel when the execution completes.",
			ExternalDocs: &asyncapi.ExternalDocs{URL: "https://docs.temporal.io/workflows"}},
		address: func(n string) string { return "workflow/" + n },
		summary: "Start the %s workflow.",
	},
	Activity: {
		tag: asyncapi.Tag{Name: "Activities", Description: "Activity types, scheduled by Workflows.",
			ExternalDocs: &asyncapi.ExternalDocs{URL: "https://docs.temporal.io/activities"}},
		address: func(n string) string { return "activity/" + n },
		summary: "Execute the %s activity.",
	},
	Signal: {
		tag: asyncapi.Tag{Name: "Signals", Description: "Asynchronous, fire-and-forget messages to a running Workflow Execution.",
			ExternalDocs: &asyncapi.ExternalDocs{URL: "https://docs.temporal.io/sending-messages#sending-signals"}},
		address:          func(n string) string { return "workflow/{workflowId}/signal/" + n },
		summary:          "Send the %s signal to a running workflow.",
		targetsExecution: true,
	},
	Query: {
		tag: asyncapi.Tag{Name: "Queries", Description: "Synchronous, read-only requests for a Workflow Execution's state.",
			ExternalDocs: &asyncapi.ExternalDocs{URL: "https://docs.temporal.io/sending-messages#sending-queries"}},
		address:          func(n string) string { return "workflow/{workflowId}/query/" + n },
		summary:          "Query a workflow's state with the %s query.",
		targetsExecution: true,
	},
	Update: {
		tag: asyncapi.Tag{Name: "Updates", Description: "Synchronous, tracked requests that may change a Workflow Execution's state and return a result.",
			ExternalDocs: &asyncapi.ExternalDocs{URL: "https://docs.temporal.io/sending-messages#sending-updates"}},
		address:          func(n string) string { return "workflow/{workflowId}/update/" + n },
		summary:          "Send the %s update to a running workflow and await its outcome.",
		targetsExecution: true,
	},
}

var workflowIDParam = map[string]*asyncapiv3.Parameter{
	"workflowId": {Name: "workflowId", Description: "Workflow ID of the target Workflow Execution."},
}

var unsafeKey = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// id makes s a valid AsyncAPI key.
func id(s string) string { return unsafeKey.ReplaceAllString(s, "_") }

func workflowTag(name string) *asyncapi.Tag {
	return &asyncapi.Tag{Name: "workflow:" + name, Description: "The " + name + " workflow and its Signal, Query and Update handlers."}
}

// AddService maps each rpc onto a channel (and a result channel) and an
// operation.
func (p *Protocol) AddService(b *core.Builder, s *protogen.Service) error {
	for _, m := range s.Methods {
		op, ok := p.ops[m.Desc.FullName()]
		if !ok || core.OperationOptions(m).GetSkip() {
			continue
		}
		if err := p.addOperation(b, s, m, op); err != nil {
			return err
		}
	}
	return nil
}

func (p *Protocol) addOperation(b *core.Builder, s *protogen.Service, m *protogen.Method, op *Operation) error {
	info := kindInfos[op.Kind]
	key := id(string(op.Kind) + "." + op.Name)
	address := info.address(op.Name)

	var params []string
	var paramDefaults map[string]*asyncapiv3.Parameter
	if info.targetsExecution {
		params, paramDefaults = []string{"workflowId"}, workflowIDParam
	}
	ch, err := b.Channel(core.ChannelSpec{
		DeclaredBy: m.Desc,
		Address:    address,
		Params:     params,
		Parameters: paramDefaults,
		DefaultID:  key,
		Meta:       metas(core.OperationOptions(m).GetChannel()),
		Servers:    core.ChannelServers(s, m),
	})
	if err != nil {
		return err
	}
	if ch.Obj.Title == "" {
		ch.Obj.Title = op.Name
	}
	if ch.Obj.Description == "" {
		ch.Obj.Description = fmt.Sprintf("Input of the %s %s.", op.Name, op.Kind)
	}
	if ch.Obj.Bindings == nil {
		ch.Obj.Bindings = &asyncapi.Bindings{}
	}
	ch.Obj.Bindings.Set(BindingKey, channelBinding(op))

	in, err := p.message(b, m.Input)
	if err != nil {
		return err
	}
	ch.AddMessage(in)

	// The Worker receives every input; clients send them.
	action := asyncapi.ActionSend
	if b.Params.Perspective == "server" {
		action = asyncapi.ActionReceive
	}
	b.AddInfoTag(&info.tag)
	tags := []*asyncapi.Tag{{Name: info.tag.Name}}
	if op.Kind == Workflow && p.grouped[op.Name] {
		b.AddInfoTag(workflowTag(op.Name))
		tags = append(tags, &asyncapi.Tag{Name: workflowTag(op.Name).Name})
	}
	for _, w := range op.Workflows {
		b.AddInfoTag(workflowTag(w))
		tags = append(tags, &asyncapi.Tag{Name: workflowTag(w).Name})
	}
	o, err := b.AddOperation(core.OpSpec{
		Service:        s,
		Method:         m,
		DefaultID:      key,
		Action:         action,
		Channel:        ch,
		Payload:        in,
		DefaultSummary: fmt.Sprintf(info.summary, op.Name),
		Tags:           tags,
		Bindings:       asyncapi.NewBindings(BindingKey, operationBinding(op)),
	})
	if err != nil {
		return err
	}

	if op.Result != nil {
		out, err := p.message(b, m.Output)
		if err != nil {
			return err
		}
		resultAddress := address + "/result"
		var resultParams []string
		if op.Kind == Workflow {
			// A workflow's result belongs to one execution.
			resultAddress = address + "/{workflowId}/result"
		}
		if op.Kind == Workflow || info.targetsExecution {
			resultParams = []string{"workflowId"}
		}
		reply, err := b.Channel(core.ChannelSpec{
			DeclaredBy: m.Desc,
			Address:    resultAddress,
			Params:     resultParams,
			Parameters: workflowIDParam,
			DefaultID:  key + ".result",
			Servers:    core.ChannelServers(s, m),
		})
		if err != nil {
			return err
		}
		reply.Obj.Title = op.Name + " result"
		reply.Obj.Description = fmt.Sprintf("Result of the %s %s.", op.Name, op.Kind)
		if reply.Obj.Bindings == nil {
			reply.Obj.Bindings = &asyncapi.Bindings{}
		}
		reply.Obj.Bindings.Set(BindingKey, channelBinding(op))
		b.SetReply(o, reply, out)
	}

	if op.ContinueAsNew {
		// Continue-as-new is always issued by the workflow's own code.
		canID := o.ID + ".continueAsNew"
		b.AddSyntheticOperation(canID, &asyncapi.Operation{
			Action:   asyncapi.ActionSend,
			Channel:  ch.Ref(),
			Title:    op.Name + " (continue-as-new)",
			Summary:  fmt.Sprintf("A running %s workflow continues-as-new: it completes its current run and starts a new run, with fresh history and this input, under the same Workflow ID.", op.Name),
			Tags:     o.Obj.Tags,
			Bindings: asyncapi.NewBindings(BindingKey, operationBinding(op)),
			Messages: []*asyncapi.Reference{ch.MessageRef(in)},
		}, ch, in)
	}
	return nil
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

// message returns the component message of a payload, documenting the
// Temporal Payload metadata as headers.
func (p *Protocol) message(b *core.Builder, m *protogen.Message) (*core.Message, error) {
	mi, err := b.Message(m)
	if err != nil {
		return nil, err
	}
	encoding := "json/protobuf"
	if b.Params.Payload == "protobuf" {
		encoding = "binary/protobuf"
	}
	mi.AddHeader("encoding", &asyncapi.Schema{Type: "string", Const: encoding, Description: "Temporal payload encoding."}, false)
	mi.AddHeader("messageType", &asyncapi.Schema{Type: "string", Const: string(m.Desc.FullName()), Description: "Fully-qualified Protobuf message type."}, false)
	return mi, nil
}

// channelBinding carries the identifying fields of an operation.
func channelBinding(op *Operation) *asyncapi.Map[any] {
	b := asyncapi.NewMap[any]()
	b.Set("kind", string(op.Kind))
	b.Set("name", op.Name)
	if op.TaskQueue != "" {
		b.Set("taskQueue", op.TaskQueue)
	}
	b.Set("bindingVersion", BindingVersion)
	return b
}

// operationBinding carries every statically declared setting.
func operationBinding(op *Operation) *asyncapi.Map[any] {
	b := asyncapi.NewMap[any]()
	b.Set("kind", string(op.Kind))
	b.Set("name", op.Name)
	if op.TaskQueue != "" {
		b.Set("taskQueue", op.TaskQueue)
	}
	if op.ContinueAsNew {
		b.Set("continueAsNew", true)
	}
	if len(op.Workflows) > 0 {
		b.Set("workflows", op.Workflows)
	}
	if t := op.Timeouts; t != nil {
		m := asyncapi.NewMap[any]()
		for _, kv := range [][2]string{
			{"execution", t.Execution}, {"run", t.Run}, {"task", t.Task},
			{"scheduleToClose", t.ScheduleToClose}, {"startToClose", t.StartToClose},
			{"scheduleToStart", t.ScheduleToStart}, {"heartbeat", t.Heartbeat},
		} {
			if kv[1] != "" {
				m.Set(kv[0], kv[1])
			}
		}
		b.Set("timeouts", m)
	}
	if r := op.RetryPolicy; r != nil {
		m := asyncapi.NewMap[any]()
		if r.GetInitialInterval() != "" {
			m.Set("initialInterval", r.GetInitialInterval())
		}
		if r.GetBackoffCoefficient() != 0 {
			m.Set("backoffCoefficient", r.GetBackoffCoefficient())
		}
		if r.GetMaximumInterval() != "" {
			m.Set("maximumInterval", r.GetMaximumInterval())
		}
		if r.GetMaximumAttempts() != 0 {
			m.Set("maximumAttempts", r.GetMaximumAttempts())
		}
		if len(r.GetNonRetryableErrorTypes()) > 0 {
			m.Set("nonRetryableErrorTypes", r.GetNonRetryableErrorTypes())
		}
		b.Set("retryPolicy", m)
	}
	b.Set("bindingVersion", BindingVersion)
	return b
}
