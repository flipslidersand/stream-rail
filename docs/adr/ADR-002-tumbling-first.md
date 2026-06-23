# ADR-002: ウィンドウは Tumbling から実装する

- **日付**: 2026-06-20
- **状態**: Accepted

## 背景

時間窓の種類として Tumbling（固定）・Sliding（スライド）・Session（セッション）がある。

## 決定

Phase 1〜5 は Tumbling Window のみ実装し、Phase 6 以降で Sliding / Session を追加する。

## 理由

- Tumbling は境界が明確（`ts.Truncate(size)`）で実装が最もシンプル
- ウィンドウ管理・状態保存・Watermark のコア概念を Tumbling で習得してから複雑な窓に進む
- ほとんどのアラートユースケースは Tumbling で十分

## トレードオフ

- イベントが窓境界をまたいだ場合の分割処理が Sliding より難しくなる（Tumbling では不要）
