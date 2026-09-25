package asyncapi

// MarshalYAML methods render objects with a Ref as Reference Objects. The
// local "plain" types drop the method set, so the non-reference case is
// encoded field by field without recursing.

func (o *Server) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain Server
	return (*plain)(o), nil
}

func (o *ServerVariable) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain ServerVariable
	return (*plain)(o), nil
}

func (o *Channel) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain Channel
	return (*plain)(o), nil
}

func (o *Parameter) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain Parameter
	return (*plain)(o), nil
}

func (o *Operation) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain Operation
	return (*plain)(o), nil
}

func (o *OperationTrait) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain OperationTrait
	return (*plain)(o), nil
}

func (o *OperationReply) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain OperationReply
	return (*plain)(o), nil
}

func (o *OperationReplyAddress) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain OperationReplyAddress
	return (*plain)(o), nil
}

func (o *Message) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain Message
	return (*plain)(o), nil
}

func (o *MessageTrait) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain MessageTrait
	return (*plain)(o), nil
}

func (o *MultiFormatSchema) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain MultiFormatSchema
	return (*plain)(o), nil
}

func (o *CorrelationID) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain CorrelationID
	return (*plain)(o), nil
}

func (o *Tag) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain Tag
	return (*plain)(o), nil
}

func (o *ExternalDocs) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain ExternalDocs
	return (*plain)(o), nil
}

func (o *SecurityScheme) MarshalYAML() (any, error) {
	if o.Ref != "" {
		return Ref(o.Ref), nil
	}
	type plain SecurityScheme
	return (*plain)(o), nil
}

// MarshalYAML renders bindings as a Reference or as the protocol map.
func (b *Bindings) MarshalYAML() (any, error) {
	if b.Ref != "" {
		return Ref(b.Ref), nil
	}
	if b.Values == nil {
		return NewMap[any](), nil
	}
	return b.Values, nil
}
