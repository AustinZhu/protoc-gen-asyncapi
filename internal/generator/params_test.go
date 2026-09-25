package generator

import (
	"reflect"
	"strings"
	"testing"
)

func TestParams(t *testing.T) {
	p, err := ParseParams("")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, DefaultParams()) {
		t.Errorf("empty parameter = %+v, want defaults", p)
	}
	d := DefaultParams()
	if d.Format != "yaml" || d.Merge || d.MergeFileName != "asyncapi" || d.Perspective != "client" ||
		d.AsyncAPIVersion != "3.1.0" || d.Version != "0.0.0" || !d.JSONNames || !d.Protovalidate || d.TrimUnusedSchemas {
		t.Errorf("defaults = %+v", d)
	}

	p, err = ParseParams("format=json, merge, merge_file_name=shop,perspective=worker,asyncapi_version=3.0.0," +
		"title=My API,version=1.0.0,description=Hi,id=urn:x,server_url=localhost:7233,namespace=default," +
		"json_names=false,trim_unused_schemas=true,protovalidate=false,paths=source_relative")
	if err != nil {
		t.Fatal(err)
	}
	want := Params{
		Format: "json", Merge: true, MergeFileName: "shop", Perspective: "worker", AsyncAPIVersion: "3.0.0",
		Title: "My API", Version: "1.0.0", Description: "Hi", ID: "urn:x", ServerURL: "localhost:7233", Namespace: "default",
		JSONNames: false, TrimUnusedSchemas: true, Protovalidate: false,
	}
	if !reflect.DeepEqual(p, want) {
		t.Errorf("params =\n%+v\nwant\n%+v", p, want)
	}
	if p.extension() != ".json" {
		t.Errorf("extension = %q", p.extension())
	}
}

func TestParamsErrors(t *testing.T) {
	for param, want := range map[string]string{
		"fromat=json":               `unknown option "fromat"`,
		"format=xml":                `invalid value "xml" for option "format" (want yaml or json)`,
		"asyncapi_version=2.6.0":    `(want 3.1.0 or 3.0.0)`,
		"perspective=server":        `(want client or worker)`,
		"merge=maybe":               `(want true or false)`,
		"merge_file_name=../escape": `want a plain file name`,
		"merge_file_name=":          `want a plain file name`,
	} {
		_, err := ParseParams(param)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", param, err, want)
		}
	}
}
