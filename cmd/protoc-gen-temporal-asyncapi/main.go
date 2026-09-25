// Command protoc-gen-temporal-asyncapi is a protoc / buf plugin that generates
// AsyncAPI 3 documents for Temporal Workflows, Activities, Signals, Queries and
// Updates declared as annotated Protobuf services.
//
// Usage with protoc:
//
//	protoc --temporal-asyncapi_out=. --temporal-asyncapi_opt=format=yaml orders.proto
//
// Usage with buf (buf.gen.yaml):
//
//	plugins:
//	  - local: protoc-gen-temporal-asyncapi
//	    out: gen/asyncapi
//	    opt: [format=yaml]
package main

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/generator"
	// Registers the temporal.asyncapi.v1 extensions so they parse as typed options.
	_ "github.com/AustinZhu/protoc-gen-temporal-asyncapi/proto/temporal/asyncapi/v1"
)

// version is set by release builds (-ldflags "-X main.version=v1.2.3").
var version = ""

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "--version", "-version", "version":
			fmt.Println("protoc-gen-temporal-asyncapi", buildVersion())
			return
		case "--help", "-help", "-h", "help":
			fmt.Print(usage)
			return
		}
		fmt.Fprintf(os.Stderr, "protoc-gen-temporal-asyncapi: unexpected argument %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err := run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "protoc-gen-temporal-asyncapi: %v\n", err)
		os.Exit(1)
	}
}

// run reads a CodeGeneratorRequest from r and writes the response to w.
// Generation errors are reported in the response, as the plugin protocol
// requires; only I/O failures are returned.
func run(r io.Reader, w io.Writer) error {
	in, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	req := &pluginpb.CodeGeneratorRequest{}
	if err := proto.Unmarshal(in, req); err != nil {
		return fmt.Errorf("reading CodeGeneratorRequest: %w", err)
	}
	out, err := proto.Marshal(generator.Run(req, buildVersion()))
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

func buildVersion() string {
	if version != "" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "(devel)"
}

var usage = `protoc-gen-temporal-asyncapi is a protoc plugin; run it through protoc or buf:

  protoc --temporal-asyncapi_out=OUT_DIR [--temporal-asyncapi_opt=key=value,...] FILES
  buf generate   (with protoc-gen-temporal-asyncapi in buf.gen.yaml)

Options:
` + generator.Usage()
