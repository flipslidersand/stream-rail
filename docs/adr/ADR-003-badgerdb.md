# ADR-003: 状態保存に BadgerDB を使う

- **日付**: 2026-06-20
- **状態**: Accepted

## 背景

窓状態の永続化として、SQLite・BadgerDB・BoltDB・Redis の選択肢がある。

## 決定

`BadgerDB v4` を使う。

## 理由

- キーに `window/{rule}/{group}/{ts}` を使うことで prefix scan が高速（LSM ツリー）
- Go ネイティブ・組み込みで外部プロセス不要
- SQLite より書き込みスループットが高く、ストリーム処理の高頻度書き込みに向いている
- BoltDB は書き込みが単一ライター制限で高頻度更新に不向き

## トレードオフ

- BadgerDB は LSM 特有のコンパクション管理が必要（`RunValueLogGC` を定期実行）
- SQL クエリが使えないためデバッグ時のデータ確認が面倒
