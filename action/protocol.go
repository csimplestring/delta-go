package action

type Protocol struct {
	MinReaderVersion int32 `json:"minReaderVersion,omitempty"`
	MinWriterVersion int32 `json:"minWriterVersion,omitempty"`
}

func (p *Protocol) Wrap() *SingleAction {
	return &SingleAction{Protocol: p}
}

func (p *Protocol) Json() (string, error) {
	return jsonString(p)
}

func (p *Protocol) Equals(other *Protocol) bool {
	if other == nil {
		return false
	}
	return p.MinReaderVersion == other.MinReaderVersion && p.MinWriterVersion == other.MinWriterVersion
}

func DefaultProtocol() *Protocol {
	return &Protocol{
		MinReaderVersion: 1,
		MinWriterVersion: 2,
	}
}
