package nats

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"

	"github.com/AustinZhu/protoc-gen-asyncapi/internal/asyncapi"
	"github.com/AustinZhu/protoc-gen-asyncapi/internal/core"
	asyncapiv3 "github.com/AustinZhu/protoc-gen-asyncapi/pb/asyncapi/v3"
	natsv1 "github.com/AustinZhu/protoc-gen-asyncapi/pb/nats/asyncapi/v1"
)

// microErrorMessage describes the error replies of NATS micro endpoints.
func (p *Protocol) microErrorMessage() *core.Message {
	mi := p.b.SyntheticMessage("nats.micro.Error", &asyncapi.Message{
		Name:        "MicroError",
		Title:       "NATS micro error",
		Description: "Error reply of a NATS micro endpoint. The error is carried in headers; the payload is optional.",
	}, nil)
	mi.AddHeader("Nats-Service-Error", &asyncapi.Schema{Type: "string", Description: "Error description."}, true)
	mi.AddHeader("Nats-Service-Error-Code", &asyncapi.Schema{Type: "string", Pattern: "^[0-9]+$", Description: "Error code."}, true)
	return mi
}

// addMicroDiscovery documents the $SRV.PING, $SRV.INFO and $SRV.STATS
// endpoints every NATS micro service answers.
func (p *Protocol) addMicroDiscovery(s *protogen.Service, m *natsv1.Micro) error {
	b := p.b
	verbs := []struct{ verb, schema, summary string }{
		{"PING", "PingResponse", "Discovers running instances of the service."},
		{"INFO", "InfoResponse", "Returns the endpoints and metadata of the service instances."},
		{"STATS", "StatsResponse", "Returns request statistics of the service instances."},
	}
	schemas := microSchemas()
	request := b.SyntheticMessage("nats.micro.DiscoveryRequest", &asyncapi.Message{Name: "DiscoveryRequest", Description: "Empty discovery request."}, nil)
	for _, v := range verbs {
		address := "$SRV." + v.verb + "." + m.GetName()
		ch, err := p.channel(s.Desc, address, "", core.ChannelSpec{Meta: []*asyncapiv3.Channel{{
			Description: fmt.Sprintf("NATS micro discovery. Instances also answer on `$SRV.%s` (all services) and `$SRV.%s.%s.<id>` (a single instance).",
				v.verb, v.verb, m.GetName()),
		}}})
		if err != nil {
			return err
		}
		ch.AddMessage(request)
		resp := b.SyntheticMessage("nats.micro."+v.schema, &asyncapi.Message{Name: v.schema}, schemas["nats.micro."+v.schema])
		id := string(s.Desc.Name()) + ".discovery." + v.verb
		if b.OperationIDTaken(id) {
			return core.Errorf(s.Desc, "operation id %q is already used", id)
		}
		action := asyncapi.ActionReceive
		if b.Params.Perspective == "client" {
			action = asyncapi.ActionSend
		}
		x := asyncapi.NewMap[any]()
		x.Set("service", m.GetName())
		x.Set("discovery", v.verb)
		op := b.AddSyntheticOperation(id, &asyncapi.Operation{
			Action:     action,
			Channel:    ch.Ref(),
			Summary:    v.summary,
			Tags:       []*asyncapi.Tag{b.ServiceTag(s)},
			Messages:   []*asyncapi.Reference{ch.MessageRef(request)},
			Extensions: asyncapi.Extensions{"x-nats-micro": x},
		}, ch, request)
		reply := b.ReplyChannel(ch.ID+".reply", "Replies of the service instances, sent to the inbox of the requester.")
		b.SetReply(op, reply, resp)
	}
	return nil
}

// microSchemas returns the JSON schemas of the micro discovery responses
// (io.nats.micro.v1.*_response).
func microSchemas() map[string]*asyncapi.Schema {
	str := func(desc string) *asyncapi.Schema { return &asyncapi.Schema{Type: "string", Description: desc} }
	metadata := &asyncapi.Schema{Type: "object", AdditionalProperties: &asyncapi.Schema{Type: "string"}}
	base := func(typ string) *asyncapi.Map[*asyncapi.Schema] {
		p := asyncapi.NewMap[*asyncapi.Schema]()
		p.Set("type", &asyncapi.Schema{Type: "string", Const: typ})
		p.Set("name", str("Service name."))
		p.Set("id", str("Instance identifier."))
		p.Set("version", str("Service version (semver)."))
		p.Set("metadata", metadata)
		return p
	}
	obj := func(desc string, props *asyncapi.Map[*asyncapi.Schema]) *asyncapi.Schema {
		return &asyncapi.Schema{Type: "object", Description: desc, Properties: props, Required: []string{"type", "name", "id", "version"}}
	}
	out := map[string]*asyncapi.Schema{}
	out["nats.micro.PingResponse"] = obj("Reply to `$SRV.PING` (`io.nats.micro.v1.ping_response`).", base("io.nats.micro.v1.ping_response"))

	endpoint := asyncapi.NewMap[*asyncapi.Schema]()
	endpoint.Set("name", str("Endpoint name."))
	endpoint.Set("subject", str("Subject the endpoint listens on."))
	endpoint.Set("queue_group", str("Queue group of the endpoint."))
	endpoint.Set("metadata", metadata)
	info := base("io.nats.micro.v1.info_response")
	info.Set("description", str("Service description."))
	info.Set("endpoints", &asyncapi.Schema{Type: "array", Items: &asyncapi.Schema{Type: "object", Properties: endpoint}})
	out["nats.micro.InfoResponse"] = obj("Reply to `$SRV.INFO` (`io.nats.micro.v1.info_response`).", info)

	nanos := func(desc string) *asyncapi.Schema { return &asyncapi.Schema{Type: "integer", Description: desc} }
	stat := asyncapi.NewMap[*asyncapi.Schema]()
	stat.Set("name", str("Endpoint name."))
	stat.Set("subject", str("Subject the endpoint listens on."))
	stat.Set("queue_group", str("Queue group of the endpoint."))
	stat.Set("num_requests", nanos("Number of requests received."))
	stat.Set("num_errors", nanos("Number of errors returned."))
	stat.Set("last_error", str("Last error returned."))
	stat.Set("processing_time", nanos("Total processing time in nanoseconds."))
	stat.Set("average_processing_time", nanos("Average processing time in nanoseconds."))
	stat.Set("data", &asyncapi.Schema{Description: "Custom statistics."})
	stats := base("io.nats.micro.v1.stats_response")
	stats.Set("started", &asyncapi.Schema{Type: "string", Format: "date-time", Description: "Start time of the instance."})
	stats.Set("endpoints", &asyncapi.Schema{Type: "array", Items: &asyncapi.Schema{Type: "object", Properties: stat}})
	out["nats.micro.StatsResponse"] = obj("Reply to `$SRV.STATS` (`io.nats.micro.v1.stats_response`).", stats)
	return out
}
