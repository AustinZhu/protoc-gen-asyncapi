// Package options discovers messages annotated with (temporal.v1.operation)
// and resolves them into validated Temporal operations.
package options

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	temporalv1 "github.com/AustinZhu/protoc-gen-temporal-asyncapi/gen/temporalv1"
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

var kinds = map[temporalv1.OperationKind]Kind{
	temporalv1.OperationKind_OPERATION_KIND_WORKFLOW: Workflow,
	temporalv1.OperationKind_OPERATION_KIND_ACTIVITY: Activity,
	temporalv1.OperationKind_OPERATION_KIND_SIGNAL:   Signal,
	temporalv1.OperationKind_OPERATION_KIND_QUERY:    Query,
	temporalv1.OperationKind_OPERATION_KIND_UPDATE:   Update,
}

// Operation is one annotated input message and what it resolves to.
type Operation struct {
	Kind                  Kind
	Name                  string
	TaskQueue             string
	SupportsContinueAsNew bool
	Input                 protoreflect.MessageDescriptor
	// Result is nil for Signals and for operations that declare no result.
	Result protoreflect.MessageDescriptor
	// Workflows names the Workflow types handling a Signal, Query or Update.
	Workflows []string
	// Timeouts and RetryPolicy are nil when not declared.
	Timeouts    *temporalv1.Timeouts
	RetryPolicy *temporalv1.RetryPolicy
}

// DefaultName derives an operation name from its input message name by
// dropping a conventional suffix: ChargeCardInput -> ChargeCard.
func DefaultName(md protoreflect.MessageDescriptor) string {
	name := string(md.Name())
	for _, suffix := range []string{"Input", "Request", "Args", "Params"} {
		if trimmed := strings.TrimSuffix(name, suffix); trimmed != name && trimmed != "" {
			return trimmed
		}
	}
	return name
}

// Package returns the proto package of the input message.
func (o *Operation) Package() string { return string(o.Input.ParentFile().Package()) }

// Discover scans every message (top-level and nested) of files, in
// declaration order, and returns the operations they declare. Results are
// resolved against registry. All problems found are reported together.
func Discover(files []protoreflect.FileDescriptor, registry *protoregistry.Files) ([]*Operation, error) {
	var (
		ops  []*Operation
		errs []error
	)
	for _, fd := range files {
		walkMessages(fd.Messages(), func(md protoreflect.MessageDescriptor) {
			op, err := resolve(md, registry)
			if err != nil {
				errs = append(errs, err)
			} else if op != nil {
				ops = append(ops, op)
			}
		})
	}
	errs = append(errs, checkDuplicates(ops)...)
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return ops, nil
}

func walkMessages(msgs protoreflect.MessageDescriptors, fn func(protoreflect.MessageDescriptor)) {
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)
		if md.IsMapEntry() {
			continue
		}
		fn(md)
		walkMessages(md.Messages(), fn)
	}
}

// Extension returns the (temporal.v1.operation) option on md, or nil.
func Extension(md protoreflect.MessageDescriptor) *temporalv1.OperationOptions {
	opts, ok := md.Options().(*descriptorpb.MessageOptions)
	if !ok || opts == nil || !proto.HasExtension(opts, temporalv1.E_Operation) {
		return nil
	}
	return proto.GetExtension(opts, temporalv1.E_Operation).(*temporalv1.OperationOptions)
}

func resolve(md protoreflect.MessageDescriptor, registry *protoregistry.Files) (*Operation, error) {
	ext := Extension(md)
	if ext == nil {
		return nil, nil
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%s: message %s: %s", Location(md), md.FullName(), fmt.Sprintf(format, args...))
	}
	kind, ok := kinds[ext.GetKind()]
	if !ok {
		return nil, fail("(temporal.v1.operation).kind must be set to a supported OperationKind, got %s", ext.GetKind())
	}
	name := ext.GetName()
	if name == "" {
		name = DefaultName(md)
	}
	if strings.TrimSpace(name) == "" {
		return nil, fail("(temporal.v1.operation).name must not be blank")
	}
	op := &Operation{
		Kind:                  kind,
		Name:                  name,
		TaskQueue:             ext.GetTaskQueue(),
		SupportsContinueAsNew: ext.GetSupportsContinueAsNew(),
		Input:                 md,
		Workflows:             ext.GetWorkflows(),
		Timeouts:              ext.GetTimeouts(),
		RetryPolicy:           ext.GetRetryPolicy(),
	}
	if op.SupportsContinueAsNew && kind != Workflow {
		return nil, fail("supports_continue_as_new is only valid for workflows, not %s", kind.Plural())
	}
	if len(op.Workflows) > 0 && (kind == Workflow || kind == Activity) {
		return nil, fail("workflows is only valid for signals, queries and updates, not %s", kind.Plural())
	}
	if op.RetryPolicy != nil && kind != Workflow && kind != Activity {
		return nil, fail("retry_policy is only valid for workflows and activities, not %s", kind.Plural())
	}
	if err := checkTimeouts(kind, op.Timeouts); err != nil {
		return nil, fail("%v", err)
	}
	if err := checkRetryPolicy(op.RetryPolicy); err != nil {
		return nil, fail("%v", err)
	}
	if ext.GetResult() != "" {
		if kind == Signal {
			return nil, fail("signals cannot declare a result (got %q)", ext.GetResult())
		}
		result, err := resolveResult(md, ext.GetResult(), registry)
		if err != nil {
			return nil, fail("%v", err)
		}
		op.Result = result
	}
	return op, nil
}

// resolveResult looks name up relative to md's package. Results must live in
// the same package as the input; names in other packages are rejected rather
// than guessed at.
func resolveResult(md protoreflect.MessageDescriptor, name string, registry *protoregistry.Files) (protoreflect.MessageDescriptor, error) {
	pkg := md.ParentFile().Package()
	rel := strings.TrimPrefix(name, ".")
	full := protoreflect.FullName(rel)
	if !strings.HasPrefix(name, ".") && pkg != "" {
		full = protoreflect.FullName(string(pkg) + "." + rel)
	}
	if d, err := registry.FindDescriptorByName(full); err == nil {
		m, ok := d.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, fmt.Errorf("result %q resolves to %s, which is not a message", name, full)
		}
		if m.ParentFile().Package() != pkg {
			return nil, fmt.Errorf("result %q is in package %q; results must be in the input's package %q", name, m.ParentFile().Package(), pkg)
		}
		return m, nil
	}
	// The name may already be fully qualified; accept it if it is in the
	// same package, and give a precise error when it exists elsewhere.
	if d, err := registry.FindDescriptorByName(protoreflect.FullName(rel)); err == nil {
		if m, ok := d.(protoreflect.MessageDescriptor); ok {
			if m.ParentFile().Package() == pkg {
				return m, nil
			}
			return nil, fmt.Errorf("result %q is in package %q; results must be in the input's package %q", name, m.ParentFile().Package(), pkg)
		}
	}
	return nil, fmt.Errorf("result %q not found in package %q", name, pkg)
}

func checkTimeouts(kind Kind, t *temporalv1.Timeouts) error {
	if t == nil {
		return nil
	}
	fields := []struct {
		name, value string
		kind        Kind
	}{
		{"execution", t.GetExecution(), Workflow},
		{"run", t.GetRun(), Workflow},
		{"task", t.GetTask(), Workflow},
		{"schedule_to_close", t.GetScheduleToClose(), Activity},
		{"start_to_close", t.GetStartToClose(), Activity},
		{"schedule_to_start", t.GetScheduleToStart(), Activity},
		{"heartbeat", t.GetHeartbeat(), Activity},
	}
	for _, f := range fields {
		if f.value == "" {
			continue
		}
		if kind != f.kind {
			return fmt.Errorf("timeouts.%s is only valid for %s, not %s", f.name, f.kind.Plural(), kind.Plural())
		}
		if err := checkDuration("timeouts."+f.name, f.value); err != nil {
			return err
		}
	}
	return nil
}

func checkRetryPolicy(p *temporalv1.RetryPolicy) error {
	if p == nil {
		return nil
	}
	if err := checkDuration("retry_policy.initial_interval", p.GetInitialInterval()); err != nil {
		return err
	}
	if err := checkDuration("retry_policy.maximum_interval", p.GetMaximumInterval()); err != nil {
		return err
	}
	if c := p.GetBackoffCoefficient(); c != 0 && c < 1 {
		return fmt.Errorf("retry_policy.backoff_coefficient must be >= 1, got %v", c)
	}
	if p.GetMaximumAttempts() < 0 {
		return fmt.Errorf("retry_policy.maximum_attempts must be >= 0, got %d", p.GetMaximumAttempts())
	}
	return nil
}

func checkDuration(field, value string) error {
	if value == "" {
		return nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s: %q is not a duration like \"30s\" or \"1h30m\"", field, value)
	}
	if d <= 0 {
		return fmt.Errorf("%s must be positive, got %q", field, value)
	}
	return nil
}

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
			errs = append(errs, fmt.Errorf("%s: message %s: duplicate %s %q, already declared by message %s at %s",
				Location(op.Input), op.Input.FullName(), op.Kind, op.Name, prev.Input.FullName(), Location(prev.Input)))
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
