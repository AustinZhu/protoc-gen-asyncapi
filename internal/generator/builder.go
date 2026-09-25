package generator

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/asyncapi"
	asyncapiv1 "github.com/AustinZhu/protoc-gen-temporal-asyncapi/proto/temporal/asyncapi/v1"
)

// ContentType is the media type of Temporal's json/protobuf payload encoding.
const ContentType = "application/json"

// Perspective decides whose side of the interaction the document describes.
type Perspective string

const (
	// Client documents the application that starts Workflows, executes
	// Activities and sends Signals, Queries and Updates: operations "send".
	Client Perspective = "client"
	// Worker documents the Worker that hosts them: operations "receive".
	Worker Perspective = "worker"
)

// Config holds document-level settings.
type Config struct {
	// AsyncAPIVersion is "3.1.0" or "3.0.0"; defaults to asyncapi.DefaultVersion.
	AsyncAPIVersion string
	Title           string
	Version         string
	Description     string
	ID              string
	ServerURL       string
	Namespace       string
	Perspective     Perspective
	// Extra lists message and enum descriptors whose schemas are included in
	// components.schemas even if no operation references them.
	Extra []protoreflect.Descriptor
}

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

var workflowIDParam = &asyncapi.Parameter{Description: "Workflow ID of the target Workflow Execution."}

// Build assembles the AsyncAPI document for ops. conv collects the payload
// schemas; its options decide property naming and protovalidate enrichment.
func Build(ops []*Operation, conv *Converter, cfg Config) (*asyncapi.Document, error) {
	if cfg.Perspective == "" {
		cfg.Perspective = Client
	}
	if cfg.AsyncAPIVersion == "" {
		cfg.AsyncAPIVersion = asyncapi.DefaultVersion
	}
	action := "send"
	if cfg.Perspective == Worker {
		action = "receive"
	}
	doc := &asyncapi.Document{
		AsyncAPI:           cfg.AsyncAPIVersion,
		ID:                 cfg.ID,
		Info:               asyncapi.Info{Title: cfg.Title, Version: cfg.Version, Description: description(cfg)},
		DefaultContentType: ContentType,
		Channels:           &asyncapi.Map[*asyncapi.Channel]{},
		Operations:         &asyncapi.Map[*asyncapi.Operation]{},
		Components: &asyncapi.Components{
			ChannelBindings:   &asyncapi.Map[*asyncapi.Bindings]{},
			OperationBindings: &asyncapi.Map[*asyncapi.Bindings]{},
			Schemas:           &asyncapi.Map[*asyncapi.Schema]{},
			Messages:          &asyncapi.Map[*asyncapi.Message]{},
		},
	}
	if cfg.ServerURL != "" {
		srv, err := server(cfg.ServerURL, cfg.Namespace)
		if err != nil {
			return nil, err
		}
		doc.Servers = &asyncapi.Map[*asyncapi.Server]{}
		doc.Servers.Set("temporal", srv)
	}

	tags := newTagger(ops)
	msgKeys := messageKeys(ops)
	addMessage := func(md protoreflect.MessageDescriptor) string {
		key := msgKeys[md.FullName()]
		if _, ok := doc.Components.Messages.Get(key); !ok {
			doc.Components.Messages.Set(key, message(md, conv.Ref(md)))
		}
		return key
	}

	for _, op := range ops {
		info := kindInfos[op.Kind]
		opTags := tags.forOperation(op)
		id := sanitize(string(op.Kind) + "." + op.Name)
		doc.Components.ChannelBindings.Set(id, &asyncapi.Bindings{Temporal: channelBinding(op)})
		doc.Components.OperationBindings.Set(id, &asyncapi.Bindings{Temporal: operationBinding(op)})
		chBinding := &asyncapi.Ref{Ref: "#/components/channelBindings/" + id}
		opBinding := &asyncapi.Ref{Ref: "#/components/operationBindings/" + id}
		address := info.address(op.Name)

		inKey := addMessage(op.Input)
		in := &asyncapi.Channel{
			Address:     address,
			Title:       op.Name,
			Description: fmt.Sprintf("Input of the %s %s.", op.Name, op.Kind),
			Messages:    refs("#/components/messages/", inKey),
			Tags:        opTags,
			Bindings:    chBinding,
		}
		if info.targetsExecution {
			in.Parameters = workflowIDParams()
		}
		doc.Channels.Set(id, in)

		operation := &asyncapi.Operation{
			Action:      action,
			Channel:     asyncapi.Ref{Ref: "#/channels/" + id},
			Title:       op.Name,
			Summary:     fmt.Sprintf(info.summary, op.Name),
			Description: op.Description(),
			Tags:        opTags,
			Bindings:    opBinding,
			Messages:    []asyncapi.Ref{{Ref: "#/channels/" + id + "/messages/" + inKey}},
		}
		if op.Result != nil {
			resultID := id + ".result"
			outKey := addMessage(op.Result)
			out := &asyncapi.Channel{
				Address:     address + "/result",
				Title:       op.Name + " result",
				Description: fmt.Sprintf("Result of the %s %s.", op.Name, op.Kind),
				Messages:    refs("#/components/messages/", outKey),
				Tags:        opTags,
				Bindings:    chBinding,
			}
			if op.Kind == Workflow {
				// A workflow's result belongs to one execution.
				out.Address = address + "/{workflowId}/result"
			}
			if strings.Contains(out.Address, "{workflowId}") {
				out.Parameters = workflowIDParams()
			}
			doc.Channels.Set(resultID, out)
			operation.Reply = &asyncapi.Reply{
				Channel:  &asyncapi.Ref{Ref: "#/channels/" + resultID},
				Messages: []asyncapi.Ref{{Ref: "#/channels/" + resultID + "/messages/" + outKey}},
			}
		}
		doc.Operations.Set(id, operation)

		if op.ContinueAsNew {
			// Continue-as-new is always issued by the workflow's own code.
			doc.Operations.Set(id+".continueAsNew", &asyncapi.Operation{
				Action:   "send",
				Channel:  asyncapi.Ref{Ref: "#/channels/" + id},
				Title:    op.Name + " (continue-as-new)",
				Summary:  fmt.Sprintf("A running %s workflow continues-as-new: it completes its current run and starts a new run, with fresh history and this input, under the same Workflow ID.", op.Name),
				Tags:     opTags,
				Bindings: opBinding,
				Messages: []asyncapi.Ref{{Ref: "#/channels/" + id + "/messages/" + inKey}},
			})
		}
	}
	doc.Info.Tags = tags.definitions()

	for _, d := range cfg.Extra {
		conv.Ref(d)
	}
	defs := conv.Definitions()
	for _, name := range conv.SortedNames() {
		doc.Components.Schemas.Set(name, defs[name])
	}
	return doc, nil
}

func description(cfg Config) string {
	if cfg.Description != "" {
		return cfg.Description
	}
	who := "a Temporal client: the application that starts Workflows and sends them Signals, Queries and Updates (and the Workflows that execute Activities)"
	if cfg.Perspective == Worker {
		who = "a Temporal Worker: the process hosting the Workflows and Activities, which receives their inputs"
	}
	return "Temporal Workflows, Activities, Signals, Queries and Updates, generated from Protobuf definitions by protoc-gen-temporal-asyncapi.\n\n" +
		"Operations are described from the perspective of " + who + ". " +
		"Payloads use Temporal's `json/protobuf` encoding (canonical Protobuf JSON); request/reply primitives model their result as the operation's reply."
}

func workflowIDParams() *asyncapi.Map[*asyncapi.Parameter] {
	m := &asyncapi.Map[*asyncapi.Parameter]{}
	m.Set("workflowId", workflowIDParam)
	return m
}

func channelBinding(op *Operation) *asyncapi.TemporalBinding {
	return &asyncapi.TemporalBinding{
		Kind:           string(op.Kind),
		Name:           op.Name,
		TaskQueue:      op.TaskQueue,
		BindingVersion: asyncapi.BindingVersion,
	}
}

func operationBinding(op *Operation) *asyncapi.TemporalBinding {
	return &asyncapi.TemporalBinding{
		Kind:           string(op.Kind),
		Name:           op.Name,
		TaskQueue:      op.TaskQueue,
		ContinueAsNew:  op.ContinueAsNew,
		Workflows:      op.Workflows,
		Timeouts:       timeouts(op.Timeouts),
		RetryPolicy:    retryPolicy(op.RetryPolicy),
		BindingVersion: asyncapi.BindingVersion,
	}
}

func timeouts(t *Timeouts) *asyncapi.Timeouts {
	if t == nil {
		return nil
	}
	return &asyncapi.Timeouts{
		Execution:       t.Execution,
		Run:             t.Run,
		Task:            t.Task,
		ScheduleToClose: t.ScheduleToClose,
		StartToClose:    t.StartToClose,
		ScheduleToStart: t.ScheduleToStart,
		Heartbeat:       t.Heartbeat,
	}
}

func retryPolicy(p *asyncapiv1.RetryPolicy) *asyncapi.RetryPolicy {
	if p == nil {
		return nil
	}
	return &asyncapi.RetryPolicy{
		InitialInterval:        p.GetInitialInterval(),
		BackoffCoefficient:     p.GetBackoffCoefficient(),
		MaximumInterval:        p.GetMaximumInterval(),
		MaximumAttempts:        p.GetMaximumAttempts(),
		NonRetryableErrorTypes: p.GetNonRetryableErrorTypes(),
	}
}

// tagger assigns operation tags: one per kind, one per package when several
// packages are documented together, and one per workflow that has handlers
// (Signals/Queries/Updates) linked to it, so UIs can group a Workflow with its
// handlers.
type tagger struct {
	multiPackage bool
	grouped      map[string]bool // workflow names with linked handlers
	used         []asyncapi.Tag
	seen         map[string]bool
}

func newTagger(ops []*Operation) *tagger {
	t := &tagger{grouped: map[string]bool{}, seen: map[string]bool{}}
	pkgs := map[string]bool{}
	for _, op := range ops {
		pkgs[op.Package()] = true
		for _, w := range op.Workflows {
			t.grouped[w] = true
		}
	}
	t.multiPackage = len(pkgs) > 1
	return t
}

func (t *tagger) forOperation(op *Operation) []asyncapi.Tag {
	var tags []asyncapi.Tag
	add := func(def asyncapi.Tag) {
		tags = append(tags, asyncapi.Tag{Name: def.Name})
		if !t.seen[def.Name] {
			t.seen[def.Name] = true
			t.used = append(t.used, def)
		}
	}
	add(kindInfos[op.Kind].tag)
	if t.multiPackage {
		add(asyncapi.Tag{Name: op.Package(), Description: "Operations declared in proto package " + op.Package() + "."})
	}
	if op.Kind == Workflow && t.grouped[op.Name] {
		add(workflowTag(op.Name))
	}
	for _, w := range op.Workflows {
		add(workflowTag(w))
	}
	return tags
}

func workflowTag(name string) asyncapi.Tag {
	return asyncapi.Tag{Name: "workflow:" + name, Description: "The " + name + " workflow and its Signal, Query and Update handlers."}
}

// definitions returns every tag used, in first-use order, with descriptions.
func (t *tagger) definitions() []asyncapi.Tag { return t.used }

func message(md protoreflect.MessageDescriptor, payload *asyncapi.Schema) *asyncapi.Message {
	doc := comments(md)
	m := &asyncapi.Message{
		Name:        string(md.Name()),
		Title:       string(md.Name()),
		ContentType: ContentType,
		Headers:     payloadMetadata(md),
		Payload:     payload,
	}
	if doc != "" {
		m.Summary, _, _ = strings.Cut(doc, "\n")
		if m.Summary != doc {
			m.Description = doc
		}
	}
	return m
}

// payloadMetadata describes the Temporal Payload metadata that accompanies a
// json/protobuf-encoded message.
func payloadMetadata(md protoreflect.MessageDescriptor) *asyncapi.Schema {
	props := &asyncapi.Map[*asyncapi.Schema]{}
	props.Set("encoding", &asyncapi.Schema{Type: "string", Const: "json/protobuf", Description: "Temporal payload encoding."})
	props.Set("messageType", &asyncapi.Schema{Type: "string", Const: string(md.FullName()), Description: "Fully-qualified Protobuf message type."})
	return &asyncapi.Schema{Type: "object", Description: "Temporal Payload metadata.", Properties: props}
}

func refs(prefix string, keys ...string) *asyncapi.Map[asyncapi.Ref] {
	m := &asyncapi.Map[asyncapi.Ref]{}
	for _, k := range keys {
		m.Set(k, asyncapi.Ref{Ref: prefix + k})
	}
	return m
}

// messageKeys assigns every input and result message a components.messages
// key: its short name when that is unambiguous, else its full name.
func messageKeys(ops []*Operation) map[protoreflect.FullName]string {
	var all []protoreflect.MessageDescriptor
	seen := map[protoreflect.FullName]bool{}
	for _, op := range ops {
		for _, md := range []protoreflect.MessageDescriptor{op.Input, op.Result} {
			if md != nil && !seen[md.FullName()] {
				seen[md.FullName()] = true
				all = append(all, md)
			}
		}
	}
	count := map[protoreflect.Name]int{}
	for _, md := range all {
		count[md.Name()]++
	}
	keys := map[protoreflect.FullName]string{}
	for _, md := range all {
		if count[md.Name()] == 1 {
			keys[md.FullName()] = sanitize(string(md.Name()))
		} else {
			keys[md.FullName()] = sanitize(string(md.FullName()))
		}
	}
	return keys
}

var unsafeKey = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// sanitize makes s a valid AsyncAPI component/channel/operation key
// (^[A-Za-z0-9\.\-_]+$), which also needs no JSON pointer escaping.
func sanitize(s string) string { return unsafeKey.ReplaceAllString(s, "_") }

func server(raw, namespace string) (*asyncapi.Server, error) {
	srv := &asyncapi.Server{Protocol: "temporal", Namespace: namespace, Host: raw}
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
	srv.Description = "Temporal Frontend Service."
	if namespace != "" {
		srv.Description = "Temporal Frontend Service, namespace `" + namespace + "`."
	}
	return srv, nil
}
