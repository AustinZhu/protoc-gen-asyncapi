// Command protoc-gen-temporal-asyncapi is a protoc/buf plugin that documents
// Protobuf messages annotated with (temporal.v1.operation) as an AsyncAPI
// document describing Temporal Workflows, Activities, Signals, Queries and
// Updates.
package main

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"

	// Registers the temporal.v1.operation extension so it parses as a typed option.
	_ "github.com/AustinZhu/protoc-gen-temporal-asyncapi/gen/temporalv1"
	"github.com/AustinZhu/protoc-gen-temporal-asyncapi/internal/plugin"
)

func main() {
	// `go install ...@vX.Y.Z` builds carry their module version.
	if info, ok := debug.ReadBuildInfo(); ok && plugin.Version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
		plugin.Version = info.Main.Version
	}
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "--version", "-version", "version":
			fmt.Println(plugin.Name, plugin.Version)
			return
		case "--help", "-help", "-h", "help":
			fmt.Printf("%s %s\n\nA protoc/buf plugin; run it through protoc or buf, not directly.\n\nParameters (comma-separated key=value pairs):\n%s", plugin.Name, plugin.Version, plugin.Usage())
			return
		}
	}
	if err := run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", plugin.Name, err)
		os.Exit(1)
	}
}

func run(in io.Reader, out io.Writer) error {
	b, err := io.ReadAll(in)
	if err != nil {
		return fmt.Errorf("reading request: %w", err)
	}
	req := &pluginpb.CodeGeneratorRequest{}
	if err := proto.Unmarshal(b, req); err != nil {
		return fmt.Errorf("parsing request: %w", err)
	}
	b, err = proto.Marshal(plugin.Run(req))
	if err != nil {
		return fmt.Errorf("encoding response: %w", err)
	}
	_, err = out.Write(b)
	return err
}
