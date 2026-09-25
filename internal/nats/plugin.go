package nats

import "github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

// Plugin is protoc-gen-nats-asyncapi.
var Plugin = core.Plugin{
	Name:      "protoc-gen-nats-asyncapi",
	Protocols: func() []core.Protocol { return []core.Protocol{New()} },
}
