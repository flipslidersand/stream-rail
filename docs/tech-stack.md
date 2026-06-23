# Tech Stack — StreamRail

## 言語・バージョン

- Go 1.22+

## 主要パッケージと選定理由

| パッケージ                       | バージョン | 役割                                     | 選定理由                              |
| -------------------------------- | ---------- | ---------------------------------------- | ------------------------------------- |
| `github.com/dgraph-io/badger/v4` | v4         | 窓状態の永続化 (KV store)                | 組み込み・高速書き込み・Go ネイティブ |
| `github.com/nats-io/nats.go`     | v1         | イベントソース (NATS JetStream, Phase 5) | 軽量 MQ、ローカル起動が容易           |
| `google.golang.org/grpc`         | v1         | 通知 gRPC エンドポイント (Phase 6)       | Proto3 で型安全な通知 API             |
| `go.opentelemetry.io/otel`       | v1         | 処理遅延・スループットのメトリクス       | OTel 標準で既存基盤に統合しやすい     |
| `github.com/spf13/cobra`         | v1         | CLI                                      | Go の事実上の標準 CLI フレームワーク  |
| `github.com/spf13/viper`         | v1         | rules.yaml 読み込み                      | cobra と相性が良い設定管理            |
| `go.uber.org/zap`                | v1         | 構造化ログ                               | 高速・JSON ログ出力                   |

## アーキテクチャ

```
HTTP / NATS
  ↓
[Ingester]         goroutine でイベントを受信し channel に投入
  ↓ chan Event
[WindowManager]    Tumbling Window ごとにイベントをバケツ分け
  ↓ chan WindowBatch
[Aggregator]       COUNT / SUM / AVG を計算
  ↓ chan AggResult
[RuleEvaluator]    HAVING 条件を判定
  ↓ chan Alert
[Notifier]         コンソール / gRPC / Webhook で通知
  ↓
[Store (BadgerDB)]  窓状態のチェックポイント保存
```

## 開発ツール

| ツール          | 用途                              |
| --------------- | --------------------------------- |
| `go test -race` | データレース検出                  |
| `go tool pprof` | goroutine・メモリプロファイリング |
| `golangci-lint` | linting                           |
