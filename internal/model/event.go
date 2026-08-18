package model

// Event はパイプラインを流れる基本単位。
type Event struct {
	Service   string         `json:"service"`
	Level     string         `json:"level"`
	Timestamp int64          `json:"ts"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// Envelope は Event に、パイプラインでの処理が完了（= 窓に取り込まれ永続化）
// した後に呼ばれる Ack コールバックを添えて運ぶ封筒。NATS のような
// at-least-once ingester が「enqueue 時点」ではなく「処理完了後」に ack を
// 送れるようにするためのもので、transport の関心事を Event 本体に混ぜない。
// Ack は nil でもよい（例: HTTP ingestion には ack の概念がない）。
type Envelope struct {
	Event Event
	Ack   func()
}
