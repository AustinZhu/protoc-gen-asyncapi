package core

import (
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	textmessage "golang.org/x/text/message"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
)

var (
	draft07Once sync.Once
	draft07     *jsonschema.Schema
	draft07Err  error
)

// metaSchema returns the JSON Schema draft-07 meta-schema, which AsyncAPI
// Schema Objects extend; it is built into the validator, so nothing is
// fetched.
func metaSchema() (*jsonschema.Schema, error) {
	draft07Once.Do(func() {
		draft07, draft07Err = jsonschema.NewCompiler().Compile("http://json-schema.org/draft-07/schema")
	})
	return draft07, draft07Err
}

// decodeSchema parses a schema override: a JSON object that must be a valid
// AsyncAPI Schema Object.
func decodeSchema(what, src string) (*asyncapi.Map[any], error) {
	v, err := asyncapi.DecodeJSON([]byte(src))
	if err != nil {
		return nil, fmt.Errorf("%s is not valid JSON: %v", what, err)
	}
	m, ok := v.(*asyncapi.Map[any])
	if !ok {
		return nil, fmt.Errorf("%s must be a JSON object, e.g. {\"type\": \"integer\"}", what)
	}
	meta, err := metaSchema()
	if err != nil {
		return nil, err
	}
	if err := meta.Validate(asyncapi.ToPlain(m)); err != nil {
		return nil, fmt.Errorf("%s is not a valid JSON Schema: %s", what, validationMessage(err))
	}
	return m, nil
}

// validationMessage flattens a validation error to its causes.
func validationMessage(err error) string {
	ve, ok := err.(*jsonschema.ValidationError)
	if !ok {
		return err.Error()
	}
	printer := textmessage.NewPrinter(language.English)
	type cause struct{ loc, msg string }
	var causes []cause
	seen := map[string]bool{}
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			// The first cause per location reads best; alternatives of the
			// meta-schema (e.g. a type name or a list of them) repeat it.
			loc := "/" + strings.Join(e.InstanceLocation, "/")
			if !seen[loc] {
				seen[loc] = true
				causes = append(causes, cause{loc, e.ErrorKind.LocalizedString(printer)})
			}
			return
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(ve)
	var out []string
	for _, c := range causes {
		// A cause inside this location is the specific one; this one only
		// reports another alternative of the meta-schema.
		deeper := false
		for _, d := range causes {
			if strings.HasPrefix(d.loc, strings.TrimSuffix(c.loc, "/")+"/") && d.loc != c.loc {
				deeper = true
			}
		}
		if !deeper {
			out = append(out, fmt.Sprintf("at %s: %s", c.loc, c.msg))
		}
	}
	return strings.Join(out, "; ")
}

// payloadOverride returns the (asyncapi.v3.message).payload_schema of a
// message rendered as a payload, or nil.
func payloadOverride(where string, schema, format string) (any, error) {
	if schema == "" {
		if format != "" {
			return nil, fmt.Errorf("%s: payload_schema_format requires payload_schema", where)
		}
		return nil, nil
	}
	if format == "" {
		m, err := decodeSchema(where+": payload_schema", schema)
		if err != nil {
			return nil, err
		}
		return &asyncapi.Schema{Raw: m}, nil
	}
	v, err := asyncapi.DecodeJSON([]byte(schema))
	if err != nil {
		return nil, fmt.Errorf("%s: payload_schema is not valid JSON: %v", where, err)
	}
	return &asyncapi.MultiFormatSchema{SchemaFormat: format, Schema: v}, nil
}

// decodeJSON parses a JSON value from an annotation into plain Go values,
// keeping integers exact (encoding/json turns them into float64, which
// corrupts 64-bit values such as nanosecond timestamps).
func decodeJSON(src string) (any, error) {
	v, err := asyncapi.DecodeJSON([]byte(src))
	if err != nil {
		return nil, err
	}
	return plain(v), nil
}

func plain(v any) any {
	switch t := v.(type) {
	case *asyncapi.Map[any]:
		out := make(map[string]any, t.Len())
		for _, k := range t.Keys() {
			x, _ := t.Get(k)
			out[k] = plain(x)
		}
		return out
	case []any:
		for i, x := range t {
			t[i] = plain(x)
		}
	}
	return v
}

// decodeInto is decodeJSON with the shape of json.Unmarshal.
func decodeInto(src string, v *any) error {
	x, err := decodeJSON(src)
	if err != nil {
		return err
	}
	*v = x
	return nil
}
