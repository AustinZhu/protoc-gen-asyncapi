package redis

import "github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

// Plugin is protoc-gen-redis-asyncapi.
var Plugin = core.Plugin{
	Name:      "protoc-gen-redis-asyncapi",
	Protocols: func() []core.Protocol { return []core.Protocol{New()} },
}
