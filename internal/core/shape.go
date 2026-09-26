package core

import "google.golang.org/protobuf/compiler/protogen"

// Shape is the messaging pattern an rpc's shape implies. Shapes only pick
// core AsyncAPI constructs: an operation's action, its messages and its
// reply.
type Shape int

const (
	// ShapeRequestReply: the service receives the request and replies with
	// the response (a receive operation with a reply).
	ShapeRequestReply Shape = iota + 1
	// ShapePublish: the service sends messages.
	ShapePublish
	// ShapeSubscribe: the service receives messages.
	ShapeSubscribe
	// ShapeProcess: the service receives the input on one channel and sends
	// the output on another (two operations).
	ShapeProcess
)

const emptyName = "google.protobuf.Empty"

// IsEmpty reports google.protobuf.Empty, which means "no message".
func IsEmpty(m *protogen.Message) bool { return m.Desc.FullName() == emptyName }

// InferShape infers the messaging pattern of an rpc from its shape:
//
//	Req → Resp, Empty → Resp, Req → stream Resp   request/reply
//	Req → Empty, stream Req → Empty               subscribe
//	Empty → stream Resp                           publish
//	stream Req → stream Resp                      process
//
// stream Req → Resp has no core AsyncAPI form and is rejected; option names
// the annotation that sets the pattern explicitly.
func InferShape(m *protogen.Method, option string) (Shape, error) {
	in, out := m.Desc.IsStreamingClient(), m.Desc.IsStreamingServer()
	switch {
	case in && out:
		return ShapeProcess, nil
	case in && !IsEmpty(m.Output):
		return 0, Errorf(m.Desc, "a client streaming rpc with a response has no AsyncAPI form; set %s.pattern", option)
	case in, !out && IsEmpty(m.Output):
		return ShapeSubscribe, nil
	case out && IsEmpty(m.Input):
		return ShapePublish, nil
	}
	return ShapeRequestReply, nil
}

// Payloads returns the messages of an rpc for a pattern: the payload of its
// operation and, for request/reply and process, the response or output.
func Payloads(m *protogen.Method, s Shape) (payload, response *protogen.Message) {
	switch s {
	case ShapeRequestReply, ShapeProcess:
		return m.Input, m.Output
	case ShapePublish:
		if m.Desc.IsStreamingServer() || IsEmpty(m.Input) {
			return m.Output, nil
		}
	}
	return m.Input, nil
}

// CheckProcess validates an rpc documented as a processor.
func CheckProcess(m *protogen.Method) error {
	if IsEmpty(m.Input) || IsEmpty(m.Output) {
		return Errorf(m.Desc, "a processor receives its input and sends its output: neither may be google.protobuf.Empty")
	}
	return nil
}
