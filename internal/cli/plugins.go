package cli

import (
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/nats"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/temporal"
)

// All is protoc-gen-asyncapi: every protocol, so one document can describe
// several. Temporal claims first: its services are explicitly annotated,
// while NATS also infers services.
var All = core.Plugin{
	Name: "protoc-gen-asyncapi",
	Protocols: func() []core.Protocol {
		return []core.Protocol{temporal.New(), nats.New()}
	},
}
