package kafka

import "github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

// Plugin is protoc-gen-kafka-asyncapi.
var Plugin = core.Plugin{
	Name:      "protoc-gen-kafka-asyncapi",
	Protocols: func() []core.Protocol { return []core.Protocol{New()} },
}
