# Spec — StreamRail

## プロジェクトの目的

イベントを HTTP で受信し、固定時間窓ごとに集計・条件判定・通知する小型ストリーム処理エンジン。
goroutine / channel / backpressure / window 処理を Go で実装しながら習得する。

## 解決する問題

| 問題                                             | StreamRail での解決策                              |
| ------------------------------------------------ | -------------------------------------------------- |
| ログを全件 DB に入れてから集計すると遅延が大きい | ストリーム上でリアルタイム集計し、閾値超過を即通知 |
| Flink / Kafka Streams は設定が重い               | 軽量な単一バイナリで同等のウィンドウ処理を実現     |
| 遅延イベントで集計がずれる                       | Watermark で遅延許容時間を定義して補正 (Phase 5)   |

## 利用イメージ

```bash
# ルール定義 (YAML)
streamrail run --config rules.yaml

# イベント投入
curl -X POST http://localhost:8080/events \
  -d '{"service":"api","level":"ERROR","ts":1718000000}'

# アラート確認 (コンソール)
[ALERT] service=api ERROR count=23 > threshold=20 (window: 10:00-10:05)
```

## ルール定義 (rules.yaml)

```yaml
rules:
  - name: error-spike
    source: application_logs
    window:
      type: tumbling
      size: 5m
    filter:
      field: level
      eq: ERROR
    group_by: service
    aggregate:
      func: count
    having:
      gt: 20
    emit: console
```

## MVP の境界線

### やること (Phase 1〜5)

- HTTP POST `/events` でイベントを受信
- Tumbling Window (固定時間窓) の実装
- COUNT / SUM 集計
- HAVING 条件判定
- コンソール通知
- BadgerDB による窓状態の永続化

### やらないこと (Phase 1)

- NATS JetStream (Phase 4 で追加)
- Sliding / Session Window
- 遅延イベント / Watermark
- 独自クエリ言語
- 複数ワーカー

## 成功条件

| Phase   | 完成条件                                              |
| ------- | ----------------------------------------------------- |
| Phase 1 | HTTP `/events` でイベントを受信し channel に流せる    |
| Phase 2 | Tumbling Window ごとにイベントをバケツ分けできる      |
| Phase 3 | COUNT / SUM を集計し HAVING 条件でアラートを出力      |
| Phase 4 | BadgerDB に窓状態を保存しプロセス再起動後に継続できる |
| Phase 5 | NATS JetStream をソースとして追加                     |
| Phase 6 | Watermark で遅延イベントを許容時間内に補正            |
