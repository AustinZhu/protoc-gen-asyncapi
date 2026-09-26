package amqp

import "github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

// Plugin is protoc-gen-amqp-asyncapi.
var Plugin = core.Plugin{
	Name:      "protoc-gen-amqp-asyncapi",
	Protocols: func() []core.Protocol { return []core.Protocol{New()} },
}
