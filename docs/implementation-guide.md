# Implementation Guide — StreamRail

## Phase 1: HTTP イベント受信（1週）

### 実装内容

- `internal/ingester/http.go` — `net/http` で `/events` エンドポイントを実装
- JSON デコードして `chan Event` に投入
- バッファフル時は `429 Too Many Requests` を返す (バックプレッシャー)

### 完成条件

```bash
go run ./cmd/streamrail
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"service":"api","level":"ERROR","ts":1718000000}'
# → 202 Accepted, channel に Event が届く
```

---

## Phase 2: Tumbling Window（1週）

### 実装内容

- `internal/window/tumbling.go` — `time.Ticker` でウィンドウの境界を計算
- イベントを `WindowKey{RuleName, GroupKey, WindowStart}` でバケツ分け
- ウィンドウ終了時に `chan WindowBatch` へ送出

### 完成条件

```bash
# 5分間イベントを送り続け、5分ごとにバッチが流れることをログで確認
```

### 難所

- 時計の進み方とウィンドウ境界のズレ → `time.Truncate(size)` で境界を正規化
- グループキーの多様性 → `map[string]*WindowBucket` でグループ管理

---

## Phase 3: 集計 + アラート（1週）

### 実装内容

- `internal/aggregator/agg.go` — `WindowBatch` から COUNT / SUM を計算
- `internal/notifier/console.go` — HAVING 条件チェック + コンソール出力

### 完成条件

```bash
# ERROR イベントを 21件送ると
[ALERT] rule=error-spike service=api count=21 > 20 (10:00-10:05)
```

---

## Phase 4: BadgerDB 状態保存（1週）

### 実装内容

- `internal/store/badger.go` — BadgerDB で窓状態の読み書き
- プロセス起動時に途中の窓状態を復元
- `go-codec` または `encoding/json` で `WindowBucket` をシリアライズ

### 完成条件

```bash
# プロセスを再起動しても進行中の窓集計が引き継がれる
```

---

## Phase 5: NATS JetStream 対応（1週）

### 実装内容

- `internal/ingester/nats.go` — NATS JetStream の Consumer でイベントを受信
- `nats.Connect` + `js.Subscribe` で `eventCh` に投入
- at-least-once 保証のために ACK を集計後に送る

### 完成条件

```bash
docker run -p 4222:4222 nats -js
nats pub application_logs '{"service":"api","level":"ERROR","ts":...}'
# StreamRail でアラートが出る
```

---

## Phase 6: Watermark + 遅延イベント（2週）

### 実装内容

- Watermark = 現在処理中の最大 ts − 許容遅延
- Watermark より古いイベントはすでにクローズした窓に補正して投入
- クローズ済み窓の再集計と再通知

---

## 実装順序の根拠

Go の goroutine / channel を最初のフェーズで体で覚える。
チャネルパイプラインが動いてから永続化 (Phase 4) を追加することで、
「状態があるとどう複雑になるか」を実感しながら設計できる。
