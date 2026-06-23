# ADR-001: パイプラインに goroutine + channel を使う

- **日付**: 2026-06-20
- **状態**: Accepted

## 背景

ストリーム処理のステージ間通信として、共有メモリ+mutex・actor モデル・channel パイプラインの選択肢がある。

## 決定

各ステージ（Ingester / WindowManager / Aggregator / Notifier）を goroutine にし、buffered channel で接続する。

## 理由

- Go の設計思想「Don't communicate by sharing memory; share memory by communicating」に沿う
- バッファ付き channel がそのままバックプレッシャーの仕組みになる（フル時は送信側がブロック）
- goroutine のリークを `context.Context` キャンセルで管理しやすい

## トレードオフ

- channel が詰まると上流が止まる（バックプレッシャーは利点でもあり詰まりの原因にもなる）
- `-race` フラグで data race を常にチェックする必要がある
