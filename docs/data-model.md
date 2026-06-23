# Data Model — StreamRail

## コアデータ構造

```go
// 受信イベント
type Event struct {
    Service   string            `json:"service"`
    Level     string            `json:"level"`
    Timestamp int64             `json:"ts"`       // Unix秒
    Fields    map[string]any    `json:"fields"`
}

// 時間窓バケツ
type WindowKey struct {
    RuleName  string
    GroupKey  string    // group_by の値
    WindowStart time.Time
}

type WindowBucket struct {
    Key    WindowKey
    Events []Event
    Count  int64
    Sum    float64
}

// 集計結果
type AggResult struct {
    WindowKey WindowKey
    WindowEnd time.Time
    Value     float64
    Count     int64
}

// アラート
type Alert struct {
    RuleName  string
    GroupKey  string
    Window    [2]time.Time
    Value     float64
    Threshold float64
    EmittedAt time.Time
}
```

## ルール設定

```go
type Rule struct {
    Name    string      `yaml:"name"`
    Source  string      `yaml:"source"`
    Window  WindowConf  `yaml:"window"`
    Filter  FilterConf  `yaml:"filter"`
    GroupBy string      `yaml:"group_by"`
    Agg     AggConf     `yaml:"aggregate"`
    Having  HavingConf  `yaml:"having"`
    Emit    string      `yaml:"emit"`    // "console" | "grpc" | "webhook"
}

type WindowConf struct {
    Type string        `yaml:"type"`    // "tumbling" | "sliding"
    Size time.Duration `yaml:"size"`
}
```

## BadgerDB キースキーマ

```
window/{rule_name}/{group_key}/{window_start_unix} → WindowBucket (msgpack)
checkpoint/{rule_name}                              → 最終処理済み timestamp
```

## Channel パイプライン

```
eventCh    chan Event          (バッファ: 10,000)
batchCh    chan WindowBatch    (バッファ: 1,000)
aggCh      chan AggResult      (バッファ: 1,000)
alertCh    chan Alert          (バッファ: 100)
```
