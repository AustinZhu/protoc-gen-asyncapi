// Package spec validates documents against the official AsyncAPI 3.0.0 and
// 3.1.0 JSON Schemas (from github.com/asyncapi/spec-json-schemas), embedded
// in the package. It is used by tests.
package spec

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
)

//go:embed 3.0.0.json 3.1.0.json
var files embed.FS

var (
	once     sync.Once
	schemas  map[string]*jsonschema.Schema
	loadErrs error
)

func load() {
	schemas = map[string]*jsonschema.Schema{}
	for _, v := range []string{"3.0.0", "3.1.0"} {
		raw, err := files.ReadFile(v + ".json")
		if err != nil {
			loadErrs = err
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			loadErrs = err
			return
		}
		c := jsonschema.NewCompiler()
		url := "https://asyncapi.com/schemas/" + v + ".json"
		if err := c.AddResource(url, doc); err != nil {
			loadErrs = err
			return
		}
		s, err := c.Compile(url)
		if err != nil {
			loadErrs = fmt.Errorf("compile AsyncAPI %s schema: %w", v, err)
			return
		}
		schemas[v] = s
	}
}

// Decode parses a YAML or JSON document into JSON-compatible values.
func Decode(content []byte) (any, error) {
	var y any
	if err := yaml.Unmarshal(content, &y); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(y)
	if err != nil {
		return nil, err
	}
	var doc any
	err = json.Unmarshal(raw, &doc)
	return doc, err
}

// Validate checks a YAML or JSON document against the JSON Schema of the
// AsyncAPI version it declares.
func Validate(content []byte) error {
	once.Do(load)
	if loadErrs != nil {
		return loadErrs
	}
	doc, err := Decode(content)
	if err != nil {
		return err
	}
	m, _ := doc.(map[string]any)
	version, _ := m["asyncapi"].(string)
	s, ok := schemas[version]
	if !ok {
		return fmt.Errorf("unsupported asyncapi version %q", version)
	}
	return s.Validate(doc)
}

// CompileJSONSchema checks that content is a valid JSON Schema document (its
// $schema dialect, e.g. draft 2020-12) whose references all resolve.
func CompileJSONSchema(content []byte) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(content))
	if err != nil {
		return err
	}
	c := jsonschema.NewCompiler()
	const url = "https://example.invalid/schema.json"
	if err := c.AddResource(url, doc); err != nil {
		return err
	}
	_, err = c.Compile(url)
	return err
}
