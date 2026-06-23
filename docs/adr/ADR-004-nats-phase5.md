# ADR-004: メッセージブローカーは Phase 5 で NATS JetStream を追加

- **日付**: 2026-06-20
- **状態**: Accepted

## 背景

イベントソースとして最初から NATS を使うか、HTTP から始めるかの選択がある。

## 決定

Phase 1〜4 は HTTP POST のみ対応し、Phase 5 で NATS JetStream を追加する。

## 理由

- HTTP は `curl` で簡単にテストでき、Phase 1〜3 のコアロジック開発に集中できる
- NATS の at-least-once 保証・ACK 管理は窓状態の永続化（Phase 4）が整ってから追加する
- Kafka より NATS の方が `docker run` 1 コマンドで起動でき、開発環境の依存が軽い

## トレードオフ

- Phase 4 までは障害時にイベントが失われる（本番ユースケースには不十分）
