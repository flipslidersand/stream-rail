# StreamRail

イベントを HTTP / NATS で受信し、固定時間窓ごとに集計・条件判定・通知する小型ストリーム処理エンジン。goroutine / channel / backpressure / window 処理 / watermark を Go で学ぶことを目的にしています。

Flink / Kafka Streams のような重い基盤を使わず、**単一バイナリ**で tumbling window 集計と閾値アラートを実現します。

## 主な機能

- HTTP `POST /events` でイベント受信（チャネルフル時は backpressure）
- NATS JetStream からの受信（at-least-once）
- Tumbling Window（固定時間窓）での集計 — `COUNT` / `SUM`
- ルール（`rules.yaml`）による `filter` → 集計 → `HAVING` 判定 → コンソール通知
- BadgerDB による窓状態の永続化（プロセス再起動で進行中の集計を resume）
- Watermark（`max event ts − 許容遅延`）による窓クローズ + 遅延イベントの補正・再アラート

## 使用技術

| 領域 | 技術 |
|------|------|
| 言語 | Go 1.22+ |
| CLI | `spf13/cobra` |
| 永続化 | `dgraph-io/badger/v4` |
| メッセージング | `nats-io/nats.go`（JetStream） |
| 設定 | `gopkg.in/yaml.v3` |
| ログ | `go.uber.org/zap`（HTTP ingester） |

## ディレクトリ構成

```
cmd/streamrail/      エントリポイント（run コマンド）
internal/
  model/             パイプライン共通の Event 型
  ingester/          HTTP / NATS の受信口
  window/            Tumbling Window + watermark + 永続化フック
  aggregator/        Batch → COUNT/SUM
  rule/              Rule / Filter / Having 型 + rules.yaml ローダー
  notifier/          HAVING 判定 + コンソール通知
  store/             BadgerDB による窓状態の永続化
  engine/            各ステージの配線（window size 別 fan-out）
docs/                spec / data-model / tech-stack / ADR / 実装ガイド
examples/rules.yaml  サンプルルール
```

## セットアップ

```bash
git clone https://github.com/flipslidersand/stream-rail.git
cd stream-rail
go build ./cmd/streamrail
```

## 実行方法

### 組み込みルール（error-spike）で起動

```bash
go run ./cmd/streamrail run --window 10s --threshold 20
```

別ターミナルからイベント投入:

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"service":"api","level":"ERROR","ts":1718000000}'
# 202 Accepted。ERROR が窓内で閾値を超えるとアラート:
# [ALERT] rule=error-spike service=api count=21 > 20 (10:00-10:05)
```

### rules.yaml でルールを定義

```bash
go run ./cmd/streamrail run --config examples/rules.yaml
```

### BadgerDB で窓状態を永続化（再起動で resume）

```bash
go run ./cmd/streamrail run --data ./data --window 60s
```

### NATS JetStream から受信

```bash
docker run -p 4222:4222 nats -js
go run ./cmd/streamrail run --nats nats://localhost:4222 --nats-subject application_logs
```

### 遅延イベント補正（watermark）

```bash
go run ./cmd/streamrail run --window 10s --lateness 30s --threshold 20
# クローズ済み窓に遅延到着したイベントで再アラート:
# [ALERT] ... count=22 > 20 (corrected)
```

## 主なフラグ

| フラグ | デフォルト | 説明 |
|--------|-----------|------|
| `--addr` | `:8080` | HTTP リッスンアドレス |
| `--window` | `5m` | デフォルト窓サイズ（`window.size` 未指定ルール） |
| `--threshold` | `20` | 組み込み error-spike の閾値（`--config` 未指定時） |
| `--config` | （空） | `rules.yaml` のパス |
| `--data` | （空） | BadgerDB ディレクトリ（空=インメモリ） |
| `--nats` | （空） | NATS サーバ URL（空=HTTP のみ） |
| `--nats-subject` | `application_logs` | JetStream の subject/stream |
| `--lateness` | `0` | 遅延イベント補正の許容遅延（0=無効） |

## テスト

```bash
go test ./...
go vet ./...
```

NATS の end-to-end 確認は Docker が必要です（`docker run -p 4222:4222 nats -js`）。

## 環境変数

環境変数は使用しません。設定は CLI フラグと `rules.yaml` で行います。秘密情報も保持しません。

## 注意事項

- `--data` を使う場合、窓状態の名前空間は窓サイズ（例 `10s` / `1m0s`）で分離されるため、**resume は同じ `--window` でのみ有効**です。
- `group_by` は現状 `service` のみ対応（#17）。
- watermark はプロセス跨ぎで永続化されません（#18）。
- NATS の ACK は enqueue 時点（#19）。

## 今後の改善予定

- [#17] `group_by` を任意フィールド対応にする
- [#18] watermark を永続化しプロセス跨ぎで継続する
- [#19] NATS ACK を集計/永続化後（end-to-end）にする

## ライセンス

MIT License — [LICENSE](LICENSE) を参照。
