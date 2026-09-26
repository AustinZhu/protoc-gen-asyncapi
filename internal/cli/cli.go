// Package cli is the shared entry point of the plugin binaries.
package cli

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

	// Register the annotation extensions so options parse as typed values.
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/amqp/asyncapi/v1"
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/nats/asyncapi/v1"
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/redis/asyncapi/v1"
	_ "github.com/AustinZhu/protoc-gen-asyncapi/pb/temporal/asyncapi/v1"
)

// Main runs a plugin binary. version is set by release builds.
func Main(pl core.Plugin, version string) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "--version", "-version", "version":
			fmt.Println(pl.Name, BuildVersion(version))
			return
		case "--help", "-help", "-h", "help":
			fmt.Print(usage(pl))
			return
		}
		fmt.Fprintf(os.Stderr, "%s: unexpected argument %q\n\n%s", pl.Name, os.Args[1], usage(pl))
		os.Exit(2)
	}
	if err := Run(pl, os.Stdin, os.Stdout, BuildVersion(version)); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", pl.Name, err)
		os.Exit(1)
	}
}

// Run reads a CodeGeneratorRequest from r and writes the response to w.
// Generation errors are reported in the response, as the plugin protocol
// requires; only I/O failures are returned.
func Run(pl core.Plugin, r io.Reader, w io.Writer, version string) error {
	in, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	req := &pluginpb.CodeGeneratorRequest{}
	if err := proto.Unmarshal(in, req); err != nil {
		return fmt.Errorf("reading CodeGeneratorRequest: %w", err)
	}
	out, err := proto.Marshal(pl.Run(req, version))
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

// BuildVersion returns the release version, the module version of
// `go install ...@vX` builds, or "(devel)".
func BuildVersion(version string) string {
	if version != "" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "(devel)"
}

func usage(pl core.Plugin) string {
	return fmt.Sprintf(`%s is a protoc plugin; run it through protoc or buf:

  protoc --%s_out=OUT_DIR [--%s_opt=key=value,...] FILES
  buf generate   (with %s in buf.gen.yaml)

Options:
%s`, pl.Name, trim(pl.Name), trim(pl.Name), pl.Name, pl.Usage())
}

func trim(name string) string {
	const prefix = "protoc-gen-"
	if len(name) > len(prefix) && name[:len(prefix)] == prefix {
		return name[len(prefix):]
	}
	return name
}
