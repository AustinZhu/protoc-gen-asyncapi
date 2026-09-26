package sqs

import "github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

// Plugin is protoc-gen-sqs-asyncapi.
var Plugin = core.Plugin{
	Name:      "protoc-gen-sqs-asyncapi",
	Protocols: func() []core.Protocol { return []core.Protocol{New()} },
}
