// Discovery of rpcs annotated with (temporal.asyncapi.v1.operation)
// and resolves them into validated Temporal operations.
package generator

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	asyncapiv1 "github.com/AustinZhu/protoc-gen-temporal-asyncapi/proto/temporal/asyncapi/v1"
)

// Kind is the lower-case name of a Temporal primitive, as used in the
// generated document ("workflow", "activity", "signal", "query", "update").
type Kind string

const (
	Workflow Kind = "workflow"
	Activity Kind = "activity"
	Signal   Kind = "signal"
	Query    Kind = "query"
	Update   Kind = "update"
)

// Plural returns the plural form used in messages ("activities").
func (k Kind) Plural() string {
	switch k {
	case Activity:
		return "activities"
	case Query:
		return "queries"
	}
	return string(k) + "s"
}

// Operation is one annotated rpc and what it resolves to.
type Operation struct {
	Kind          Kind
	Name          string
	TaskQueue     string
	ContinueAsNew bool
	Method        protoreflect.MethodDescriptor
	Input         protoreflect.MessageDescriptor
	// Result is nil when the rpc returns google.protobuf.Empty.
	Result protoreflect.MessageDescriptor
	// Workflows names (by Temporal name) the Workflows handling a Signal,
	// Query or Update.
	Workflows []string
	// Timeouts is nil when none are declared.
	Timeouts    *Timeouts
	RetryPolicy *asyncapiv1.RetryPolicy
}

// Timeouts are the statically declared timeouts, as Go-style duration strings.
type Timeouts struct {
	Execution, Run, Task                                      string // workflows
	ScheduleToClose, StartToClose, ScheduleToStart, Heartbeat string // activities
}

// Package returns the proto package of the rpc's service.
func (o *Operation) Package() string { return string(o.Method.ParentFile().Package()) }

// Description is the rpc's comment, falling back to its input message's.
func (o *Operation) Description() string {
	if d := comments(o.Method); d != "" {
		return d
	}
	return comments(o.Input)
}

const emptyName = "google.protobuf.Empty"

// Discover scans every service of files, in declaration order, and returns the
// operations their rpcs declare. All problems found are reported together.
func Discover(files []protoreflect.FileDescriptor) ([]*Operation, error) {
	var (
		ops  []*Operation
		errs []error
	)
	for _, fd := range files {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			svcOps, svcErrs := discoverService(services.Get(i))
			ops = append(ops, svcOps...)
			errs = append(errs, svcErrs...)
		}
	}
	errs = append(errs, checkDuplicates(ops)...)
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return ops, nil
}

// ServiceOptions returns the (temporal.asyncapi.v1.service) option, or nil.
func ServiceOptions(sd protoreflect.ServiceDescriptor) *asyncapiv1.Service {
	opts, ok := sd.Options().(*descriptorpb.ServiceOptions)
	if !ok || opts == nil || !proto.HasExtension(opts, asyncapiv1.E_Service) {
		return nil
	}
	return proto.GetExtension(opts, asyncapiv1.E_Service).(*asyncapiv1.Service)
}

// OperationOptions returns the (temporal.asyncapi.v1.operation) option, or nil.
func OperationOptions(md protoreflect.MethodDescriptor) *asyncapiv1.Operation {
	opts, ok := md.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil || !proto.HasExtension(opts, asyncapiv1.E_Operation) {
		return nil
	}
	return proto.GetExtension(opts, asyncapiv1.E_Operation).(*asyncapiv1.Operation)
}

// handler holds a Signal/Query/Update's declared workflow references until
// the service's workflows are known.
type handler struct {
	op   *Operation
	refs []string
}

func discoverService(sd protoreflect.ServiceDescriptor) ([]*Operation, []error) {
	svc := ServiceOptions(sd)
	var (
		ops       []*Operation
		errs      []error
		handlers  []handler
		workflows = map[protoreflect.Name]*Operation{} // by rpc name
		wfOrder   []*Operation
	)
	methods := sd.Methods()
	for i := 0; i < methods.Len(); i++ {
		md := methods.Get(i)
		opt := OperationOptions(md)
		if opt == nil {
			if svc != nil {
				errs = append(errs, failf(md, "every rpc of a service with (temporal.asyncapi.v1.service) must declare (temporal.asyncapi.v1.operation)"))
			}
			continue
		}
		op, refs, err := resolve(md, opt, svc)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		ops = append(ops, op)
		switch op.Kind {
		case Workflow:
			workflows[md.Name()] = op
			wfOrder = append(wfOrder, op)
		case Signal, Query, Update:
			handlers = append(handlers, handler{op, refs})
		}
	}

	for _, h := range handlers {
		if len(h.refs) == 0 {
			if len(wfOrder) == 1 {
				h.op.Workflows = []string{wfOrder[0].Name}
			}
			continue
		}
		for _, ref := range h.refs {
			wf, ok := workflows[protoreflect.Name(ref)]
			if !ok {
				errs = append(errs, failf(h.op.Method, "workflows: %q is not a workflow rpc of service %s", ref, sd.FullName()))
				continue
			}
			h.op.Workflows = append(h.op.Workflows, wf.Name)
		}
	}
	return ops, errs
}

func failf(md protoreflect.MethodDescriptor, format string, args ...any) error {
	return fmt.Errorf("%s: rpc %s: %s", Location(md), md.FullName(), fmt.Sprintf(format, args...))
}

// resolve turns one annotated rpc into an Operation, returning a handler's
// unresolved workflow references alongside it.
func resolve(md protoreflect.MethodDescriptor, opt *asyncapiv1.Operation, svc *asyncapiv1.Service) (*Operation, []string, error) {
	fail := func(format string, args ...any) error { return failf(md, format, args...) }
	if md.IsStreamingClient() || md.IsStreamingServer() {
		return nil, nil, fail("streaming rpcs cannot be Temporal operations")
	}
	op := &Operation{Method: md, Input: md.Input(), Result: md.Output()}
	if op.Result.FullName() == emptyName {
		op.Result = nil
	}
	var (
		name  string
		refs  []string
		durs  [][2]string // (field, value) in declaration order
		retry *asyncapiv1.RetryPolicy
	)
	switch k := opt.GetKind().(type) {
	case *asyncapiv1.Operation_Workflow:
		w := k.Workflow
		op.Kind, name, op.TaskQueue, op.ContinueAsNew, retry = Workflow, w.GetName(), w.GetTaskQueue(), w.GetContinueAsNew(), w.GetRetryPolicy()
		durs = [][2]string{{"execution_timeout", w.GetExecutionTimeout()}, {"run_timeout", w.GetRunTimeout()}, {"task_timeout", w.GetTaskTimeout()}}
		if t := (Timeouts{Execution: w.GetExecutionTimeout(), Run: w.GetRunTimeout(), Task: w.GetTaskTimeout()}); t != (Timeouts{}) {
			op.Timeouts = &t
		}
	case *asyncapiv1.Operation_Activity:
		a := k.Activity
		op.Kind, name, op.TaskQueue, retry = Activity, a.GetName(), a.GetTaskQueue(), a.GetRetryPolicy()
		durs = [][2]string{
			{"schedule_to_close_timeout", a.GetScheduleToCloseTimeout()}, {"start_to_close_timeout", a.GetStartToCloseTimeout()},
			{"schedule_to_start_timeout", a.GetScheduleToStartTimeout()}, {"heartbeat_timeout", a.GetHeartbeatTimeout()},
		}
		if t := (Timeouts{
			ScheduleToClose: a.GetScheduleToCloseTimeout(), StartToClose: a.GetStartToCloseTimeout(),
			ScheduleToStart: a.GetScheduleToStartTimeout(), Heartbeat: a.GetHeartbeatTimeout(),
		}); t != (Timeouts{}) {
			op.Timeouts = &t
		}
	case *asyncapiv1.Operation_Signal:
		op.Kind, name, refs = Signal, k.Signal.GetName(), k.Signal.GetWorkflows()
		if op.Result != nil {
			return nil, nil, fail("signals return nothing: the rpc must return google.protobuf.Empty, not %s", op.Result.FullName())
		}
	case *asyncapiv1.Operation_Query:
		op.Kind, name, refs = Query, k.Query.GetName(), k.Query.GetWorkflows()
		if op.Result == nil {
			return nil, nil, fail("queries must return a result, not google.protobuf.Empty")
		}
	case *asyncapiv1.Operation_Update:
		op.Kind, name, refs = Update, k.Update.GetName(), k.Update.GetWorkflows()
	default:
		return nil, nil, fail("(temporal.asyncapi.v1.operation) must set one of workflow, activity, signal, query or update")
	}

	op.Name = name
	if op.Name == "" {
		op.Name = string(md.Name())
	}
	if strings.TrimSpace(op.Name) == "" {
		return nil, nil, fail("name must not be blank")
	}
	if op.TaskQueue == "" && (op.Kind == Workflow || op.Kind == Activity) {
		op.TaskQueue = svc.GetTaskQueue()
	}
	for _, d := range durs {
		if err := checkDuration(op.Kind, d[0], d[1]); err != nil {
			return nil, nil, fail("%v", err)
		}
	}
	if err := checkRetryPolicy(op.Kind, retry); err != nil {
		return nil, nil, fail("%v", err)
	}
	op.RetryPolicy = retry
	return op, refs, nil
}

func checkRetryPolicy(kind Kind, p *asyncapiv1.RetryPolicy) error {
	if p == nil {
		return nil
	}
	if err := checkDuration(kind, "retry_policy.initial_interval", p.GetInitialInterval()); err != nil {
		return err
	}
	if err := checkDuration(kind, "retry_policy.maximum_interval", p.GetMaximumInterval()); err != nil {
		return err
	}
	if c := p.GetBackoffCoefficient(); c != 0 && c < 1 {
		return fmt.Errorf("%s.retry_policy.backoff_coefficient must be >= 1, got %v", kind, c)
	}
	if p.GetMaximumAttempts() < 0 {
		return fmt.Errorf("%s.retry_policy.maximum_attempts must be >= 0, got %d", kind, p.GetMaximumAttempts())
	}
	return nil
}

func checkDuration(kind Kind, field, value string) error {
	if value == "" {
		return nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s.%s: %q is not a duration like \"30s\" or \"1h30m\"", kind, field, value)
	}
	if d <= 0 {
		return fmt.Errorf("%s.%s must be positive, got %q", kind, field, value)
	}
	return nil
}

// checkDuplicates rejects two rpcs declaring the same (kind, name): Temporal
// names are global within a namespace.
func checkDuplicates(ops []*Operation) []error {
	type key struct {
		kind Kind
		name string
	}
	first := map[key]*Operation{}
	var errs []error
	for _, op := range ops {
		k := key{op.Kind, op.Name}
		if prev, ok := first[k]; ok {
			errs = append(errs, failf(op.Method, "duplicate %s %q, already declared by rpc %s at %s",
				op.Kind, op.Name, prev.Method.FullName(), Location(prev.Method)))
			continue
		}
		first[k] = op
	}
	return errs
}

// Location formats d's position as file:line:column when source info is
// available, or just the file path otherwise.
func Location(d protoreflect.Descriptor) string {
	path := d.ParentFile().Path()
	loc := d.ParentFile().SourceLocations().ByDescriptor(d)
	if loc.Path == nil {
		return path
	}
	return fmt.Sprintf("%s:%d:%d", path, loc.StartLine+1, loc.StartColumn+1)
}
