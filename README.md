# StreamRail

A lightweight stream processing engine that ingests events via HTTP / NATS, aggregates them in fixed time windows, evaluates conditions, and fires alerts.
Built to explore goroutines, channels, backpressure, window processing, and watermarks in Go.

Without heavy infrastructure like Flink or Kafka Streams, StreamRail achieves tumbling window aggregation and threshold alerting in a **single binary**.

---

イベントを HTTP / NATS で受信し、固定時間窓ごとに集計・条件判定・通知する小型ストリーム処理エンジン。goroutine / channel / backpressure / window 処理 / watermark を Go で学ぶことを目的にしています。

Flink / Kafka Streams のような重い基盤を使わず、**単一バイナリ**で tumbling window 集計と閾値アラートを実現します。

## Features / 主な機能

- Ingests events via HTTP `POST /events` (with backpressure when the channel is full)
- Consumes from NATS JetStream (at-least-once delivery)
- Tumbling window aggregation — `COUNT` / `SUM`
- Rule-based pipeline (`rules.yaml`): `filter` → `group_by` on arbitrary fields → aggregate → `HAVING` check → console notification
- Per-rule window size and `group_by`, with fan-out to multiple aggregation streams
- Window state persistence via BadgerDB (resumes in-flight aggregations after process restart)
- Watermark (`max event ts − allowed lateness`) for window closing + late-event correction and re-alerting (watermark is also persisted when BadgerDB is enabled)
- NATS messages are fed into all matching windows and ACKed after persistence when configured

---

- HTTP `POST /events` でイベント受信（チャネルフル時は backpressure）
- NATS JetStream からの受信（at-least-once）
- Tumbling Window（固定時間窓）での集計 — `COUNT` / `SUM`
- ルール（`rules.yaml`）による `filter` → 任意フィールドの `group_by` → 集計 → `HAVING` 判定 → コンソール通知
- ルールごとに異なる窓サイズと `group_by` を指定し、複数の集計ストリームへ fan-out
- BadgerDB による窓状態の永続化（プロセス再起動で進行中の集計を resume）
- Watermark（`max event ts − 許容遅延`）による窓クローズ + 遅延イベントの補正・再アラート（BadgerDB 利用時は watermark も永続化）
- NATS メッセージをすべての対象窓に取り込み、設定時は永続化した後に ACK

## Tech Stack / 使用技術

| Area / 領域 | Technology / 技術 |
|-------------|-------------------|
| Language / 言語 | Go 1.22 |
| CLI | `spf13/cobra` |
| Persistence / 永続化 | `dgraph-io/badger/v4` |
| Messaging / メッセージング | `nats-io/nats.go`（JetStream） |
| Config / 設定 | `gopkg.in/yaml.v3` |
| Logging / ログ | `go.uber.org/zap`（HTTP ingester） |
| Metrics / メトリクス | `prometheus/client_golang` |

## Directory Structure / ディレクトリ構成

```
cmd/streamrail/      Entry point (run command) / エントリポイント（run コマンド）
internal/
  model/             Shared Event type / パイプライン共通の Event 型
  ingester/          HTTP / NATS receivers / HTTP / NATS の受信口
  window/            Tumbling Window + watermark + persistence hooks / Tumbling Window + watermark + 永続化フック
  aggregator/        Batch → COUNT/SUM
  rule/              Rule / Filter / Having types + rules.yaml loader / Rule / Filter / Having 型 + rules.yaml ローダー
  notifier/          HAVING evaluation + console notification / HAVING 判定 + コンソール通知
  store/             BadgerDB window state persistence / BadgerDB による窓状態の永続化
  engine/            Stage wiring (fan-out by window size / group_by) / 各ステージの配線（window size / group_by 別 fan-out）
docs/                spec / data-model / tech-stack / ADR / implementation guide / 実装ガイド
examples/rules.yaml  Sample rules / サンプルルール
```

## Setup / セットアップ

```bash
git clone https://github.com/flipslidersand/stream-rail.git
cd stream-rail
go build ./cmd/streamrail
```

## Usage / 実行方法

### Built-in rule (error-spike) / 組み込みルール（error-spike）で起動

```bash
go run ./cmd/streamrail run --window 10s --threshold 20
```

Send events from another terminal / 別ターミナルからイベント投入:

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"service":"api","level":"ERROR","ts":1718000000}'
# 202 Accepted。ERROR が窓内で閾値を超えるとアラート:
# [ALERT] rule=error-spike service=api count=21 > 20 (10:00-10:05)
```

### Define rules via rules.yaml / rules.yaml でルールを定義

```bash
go run ./cmd/streamrail run --config examples/rules.yaml
```

### Persist window state with BadgerDB (resume after restart) / BadgerDB で窓状態を永続化（再起動で resume）

```bash
go run ./cmd/streamrail run --data ./data --window 60s
```

### Consume from NATS JetStream / NATS JetStream から受信

```bash
docker run -p 4222:4222 nats -js
go run ./cmd/streamrail run --nats nats://localhost:4222 --nats-subject application_logs
```

### Late event correction (watermark) / 遅延イベント補正（watermark）

```bash
go run ./cmd/streamrail run --window 10s --lateness 30s --threshold 20
# クローズ済み窓に遅延到着したイベントで再アラート:
# [ALERT] ... count=22 > 20 (corrected)
```

### Multi-field group_by (rules.yaml) / 複数フィールドで group_by（rules.yaml）

```yaml
rules:
  - name: error-by-service-region
    filter: {field: level, op: eq, value: ERROR}
    window: {size: 10s}
    aggregate: {func: COUNT, field: level}
    group_by: [service, region]
    having: {op: gt, value: 5}
```

### Prometheus metrics / Prometheus メトリクス

```bash
go run ./cmd/streamrail run --metrics-addr :9090
# 別ターミナル
curl http://localhost:9090/metrics
# streamrail_events_received_total, streamrail_events_dropped_total,
# streamrail_windows_closed_total, streamrail_alerts_fired_total
```

## Flags / 主なフラグ

| Flag / フラグ | Default / デフォルト | Description / 説明 |
|---------------|----------------------|--------------------|
| `--addr` | `:8080` | HTTP listen address / HTTP リッスンアドレス |
| `--window` | `5m` | Default window size (for rules without `window.size`) / デフォルト窓サイズ（`window.size` 未指定ルール） |
| `--threshold` | `20` | Built-in error-spike threshold (used when `--config` is not set) / 組み込み error-spike の閾値（`--config` 未指定時） |
| `--config` | (empty) | Path to `rules.yaml` / `rules.yaml` のパス |
| `--data` | (empty) | BadgerDB directory (empty = in-memory) / BadgerDB ディレクトリ（空=インメモリ） |
| `--nats` | (empty) | NATS server URL (empty = HTTP only) / NATS サーバ URL（空=HTTP のみ） |
| `--nats-subject` | `application_logs` | JetStream subject/stream / JetStream の subject/stream |
| `--nats-dedupe-ttl` | `24h` | NATS redelivery dedupe TTL / NATS 再配信 dedupe の TTL |
| `--lateness` | `0` | Allowed lateness for late-event correction (0 = disabled) / 遅延イベント補正の許容遅延（0=無効） |
| `--metrics-addr` | (empty) | Prometheus `/metrics` listen address (empty = disabled) / Prometheus `/metrics` のリッスンアドレス（空=無効） |

## Testing / テスト

```bash
go test ./...
go vet ./...
```

End-to-end NATS testing requires Docker (`docker run -p 4222:4222 nats -js`).

NATS の end-to-end 確認は Docker が必要です（`docker run -p 4222:4222 nats -js`）。

## Environment Variables / 環境変数

No environment variables are used. All configuration is done via CLI flags and `rules.yaml`. No secrets are stored.

環境変数は使用しません。設定は CLI フラグと `rules.yaml` で行います。秘密情報も保持しません。

## Notes / 注意事項

- When using `--data`, window state and watermark namespaces are separated by window size and `group_by` (e.g. `10s/service`). Resume requires the same window size and `group_by`.
- Only tumbling windows are supported; notifications go to console only.
- `filter` / `group_by` can reference `service`, `level`, or any key inside the event's `fields`. `SUM` targets numeric fields inside `fields`.

---

- `--data` を使う場合、窓状態と watermark の名前空間は窓サイズと `group_by`（例 `10s/service`）で分離されます。resume には同じ窓サイズと `group_by` が必要です。
- 窓は tumbling window、通知先はコンソールのみ対応しています。
- `filter` / `group_by` は `service`、`level`、またはイベントの `fields` 内のキーを指定できます。`SUM` の対象は `fields` 内の数値フィールドです。

## Future Work / 今後の改善予定

- Sliding / Session Window support
- Notification targets beyond console (Webhook / HTTP notifier)

---

- Sliding / Session Window の追加
- コンソール以外の通知先の追加（Webhook / HTTP notifier）

## License / ライセンス

MIT License — see [LICENSE](LICENSE).

MIT License — [LICENSE](LICENSE) を参照。
