package mqtt

import "github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

// Plugin is protoc-gen-mqtt-asyncapi.
var Plugin = core.Plugin{
	Name:      "protoc-gen-mqtt-asyncapi",
	Protocols: func() []core.Protocol { return []core.Protocol{New()} },
}
