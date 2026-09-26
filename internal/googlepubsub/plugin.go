package googlepubsub

import "github.com/AustinZhu/protoc-gen-asyncapi/internal/core"

// Plugin is protoc-gen-googlepubsub-asyncapi.
var Plugin = core.Plugin{
	Name:      "protoc-gen-googlepubsub-asyncapi",
	Protocols: func() []core.Protocol { return []core.Protocol{New()} },
}
