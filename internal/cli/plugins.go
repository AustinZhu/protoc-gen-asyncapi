package cli

import (
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/amqp"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/googlepubsub"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/kafka"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/mqtt"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/nats"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/redis"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/sqs"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/temporal"
)

// All is protoc-gen-asyncapi: every protocol, so one document can describe
// several. Every protocol but NATS claims only explicitly annotated services, so
// they claim first: NATS also infers services.
var All = core.Plugin{
	Name: "protoc-gen-asyncapi",
	Protocols: func() []core.Protocol {
		return []core.Protocol{temporal.New(), redis.New(), amqp.New(), googlepubsub.New(), kafka.New(), mqtt.New(), sqs.New(), nats.New()}
	},
}
