// Command protoc-gen-googlepubsub-asyncapi is a protoc / buf plugin generating AsyncAPI
// 3.0/3.1 documents from annotated Protobuf services. Run it with --help for
// its options.
package main

import (
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/cli"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/googlepubsub"
)

// version is set by release builds (-ldflags "-X main.version=v1.2.3").
var version = ""

func main() { cli.Main(googlepubsub.Plugin, version) }
