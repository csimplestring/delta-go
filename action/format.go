package action

type Format struct {
	Proviver string            `json:"provider,omitempty"`
	Options  map[string]string `json:"options,omitempty"`
}
