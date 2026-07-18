package model

// Event はパイプラインを流れる基本単位。
type Event struct {
	Service   string         `json:"service"`
	Level     string         `json:"level"`
	Timestamp int64          `json:"ts"`
	Fields    map[string]any `json:"fields,omitempty"`
}
