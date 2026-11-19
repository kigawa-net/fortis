# Fortis ロードマップ

本書は issue #20（全体追跡用 umbrella）の現状追跡である。
設計定義は `docs/architecture.md` を参照。本書はフェーズ・状態・残件に特化し、設計の重複記述は避ける。

> 注記: #20 本文は計画時点の記述であり、チェックボックス状態も計画当時のものである。
> 本書は PR #69〜#73 がマージされた `main` の実装状態を基準とする。#20 本文との差分は「#20 本文との差分注記」にまとめる。

## 目的と対応表

| Phase | Issue | 役割 | 状態 |
| --- | --- | --- | --- |
| 1 | — | Storage 基盤 | 完了 |
| 2 | #18 / #17 / #26 | Raft 基盤 / Snapshot 回復 / CI | ほぼ完了（#17 完了） |
| 3 | #21 | MVCC・版付き KV・snapshot read | 単一ノードで完了、複数レプリカは一部実装済み |
| 4 | #22 | Range Sharding・Multi-Raft・Routing | 複数レンジで実装済み |
| 5 | #23 | Meta Raft・Membership・配置・Rebalance | Meta Raft・昇格・移動計画を実装済み（実データ移動は未実装） |
| 6 | #24 | KV Transaction・Cross-Shard 2PC・Recovery | 固定レンジ構成で 2PC・クラッシュ復旧まで実装済み |
| 7 | #25 | LSM・可観測性・セキュリティ・運用 | 第一歩＋第二歩完了 |

#19 は SQL 非対応の方針定義であり、実装フェーズとは分けて扱う。

## フェーズ別状態

### Phase 1 — Storage 基盤: 完了

- Memory / File I/O / WAL / Disk、Raft durable log は利用可能。

### Phase 2 — Raft 基盤: ほぼ完了

- 実装済み: 選挙 / 複製 / QUIC + mTLS / 並列 RPC / ReadIndex / NoOp / 提案完了 / crash-safe な term・vote・log（`raft/.../runtime`、`transport/netty`、`log`）。
- #26: CI（`.github/workflows/gradle-ci.yml`）3 ジョブ緑。
- #17（PR #69）完了。Snapshot 永続化、単一メッセージ／チャンクでの InstallSnapshot、base-index 前方圧縮、snapshot とログによる再起動復旧を実装。詳細は `docs/architecture.md` のスナップショット節を参照。
  - 従来の単一メッセージ形式・未圧縮ログ・版1スナップショットとの読み取り互換性を維持。

### Phase 3 — MVCC (#21): 複数レプリカは一部実装済み

- PR #70 で単調増加する timestamp の発行、write intent の予約・確定・破棄、版ベースの競合検出、pin と GC safe point に基づく version GC・stale intent GC を実装。
- ローカル checkpoint は未確定の予約版を stale として復元する。Raft では GC 境界を複製し、永続スナップショット v2 で確定履歴・floor・発行済み timestamp 上限・受付記録を復元する。v1 の読み取りにも対応する。[MVCC 契約](mvcc-contract.md)
- 3 レプリカで履歴と floor の一致、リーダー交代、PREPARE の非露出、GC 後の再起動と InstallSnapshot を検証した。
- #21 の残件 (1)(2)(3)(8) は timestamp 衝突回避と順序付け（HLC を含む）、ローカル write intent と版ストアの複製、レプリカ間の GC 境界の調整。

### Phase 4 — Range Sharding (#22): 複数レンジで実装済み

- PR #71 で `RangeMetadata` と epoch による古い要求の拒否、Leader redirect、レンジ単位の scan 分割と結果の結合を実装。
- `RaftGroupManager` が複数レンジの Raft Runtime を管理し、レンジ単位で KV 操作と Raft RPC を振り分ける。
- 残件は Meta Raft と連携したルーティング更新と、レンジ再構成時の切替。

### Phase 5 — Meta Raft (#23): メタデータ複製・昇格・移動計画を実装済み

- PR #72 で専用 Raft Group の bootstrap、node registry と range map の複製、メタデータ上の原子的な split を実装。
- Meta Raft の Learner 追加・catch-up・voter 昇格と、負荷に基づくレプリカ移動計画の算出を実装。
- 残件はデータレンジのレプリカ移動と旧 voter の除外、split 時のデータ移動とルーティング切替、merge。

### Phase 6 — Transaction (#24): 固定レンジ構成で実装済み

- `KvTransactionCoordinator` に `MultiRaftRouter`、各レンジの Leader ランタイム、記録専用の Raft Leader ランタイムを渡す。すべてのグループで版付き `StorageEngineStateMachine` と永続 Raft ログを使用する。
- `begin()` で取得したハンドルは `read(key)`、`write(key, value)`、`commit()`、`abort()` を公開する。削除は `write(key, null)`。空でないキーは最大 1 KiB、アクセスはレンジごとに 64 キーまで、エンコードした確定計画は全体で最大 1 MiB。
- PR #73 で単一レンジの atomic batch と、複数レンジの 2PC・永続トランザクション記録・クラッシュ復旧を実装。読み取りだけの参加者も prepare し、全参加者の同意を記録してから commit を適用する。記録用グループが確定処理を直列化し、各レンジの版を検証する。
- `commit()` が `false` を返した取引は新しく開始できる。結果不明時は同じハンドルで再試行するか、Leader を解決して `recover()` と `record(id)` を呼ぶ。準備が完了した取引は復旧で commit し、適用済みバッチは再適用しない。確定処理の開始前に失われたハンドルは `abort(id)` で破棄できる。
- Strict Serializability の運用範囲は、同じ記録用グループを共有し、固定レンジ構成で全クライアント操作をこの API に通す場合。古い読み取り版を MVCC GC が回収した場合は、取引を破棄して再試行する。開始時に全レンジの quorum が必要で、確定中の参加者が応答不能なら復旧可能になるまで新規開始を待つ。
- 残件はレンジ再構成との連携、確定処理の並列化、トランザクション記録の GC。

### Phase 7 — Hardening (#25): 第一歩＋第二歩完了

- 実装済み: メトリクス基盤（`raft/.../metrics` の `FortisMetrics` / InMemory / NoOp、Applier・Txn フック）、backup / restore ドリル基盤（`raft/.../backup` の `RaftBackup` 外枠 codec、破損時は例外、メモリ版 5 件・ファイル版 2 件のテスト）。
- 残件: LSM、本番セキュリティ（認証・認可・証明書ローテーション）、tracing、本格 Chaos（failure injection、partition test）、benchmark、運用手順。

## #20 本文との差分注記

- #20 本文の Phase チェックボックスは計画時点のものであり、本書では転記していない。
- Phase 2 の #17 は Snapshot 永続化・チャンク転送・ログ圧縮・再起動復旧まで完了。#26 は CI 3 ジョブが成功。
- Phase 3〜6 は単一ノード MVCC、複数レンジのルーティングと Multi-Raft、Meta Raft と移動計画、固定レンジ構成のトランザクションと復旧まで実装済み。
- Phase 7 はメトリクス基盤と backup / restore ドリル基盤まで完了。
- #20 本文の「PR #8 レビュー後の優先修正」「現在地」等の時系列メモは本書に転記しない。最新状態は本書のフェーズ別状態を正とする。

## 未決定事項（#20 本文に基づく残件）

- Key / Value サイズ上限と大容量 Value の扱い
- Public Protocol（QUIC / gRPC / HTTP など）
- LSM 実装方針（独自 / 既存 Engine）
- Timestamp・Commit Ordering・Isolation
- Deadlock / lock recovery
- データレンジのレプリカ移動・旧 voter 除外の手順
- Meta Raft の quorum 配置・障害時の運用
- Snapshot のストリーミング処理・差分転送
- Range Split の consistent cut / routing 切替
- API Authentication / Authorization

## 優先順位（現状に合わせた次の順序案）

1. #21 の残件。timestamp・commit ordering、ローカル intent と版ストアの複製、レプリカ間の GC 境界の調整。
2. #22・#23 の残件。Meta Raft とルーティングの連携、レプリカの実データ移動と旧 voter の除外、split 時のルーティング切替。
3. #24 の残件。レンジ再構成との連携、確定処理の並列化、トランザクション記録の GC。
4. #25 の残件。本番セキュリティ、障害試験、性能測定、運用手順。安全性に関わる作業は並行して進める。
5. 未決定事項を順次確定する。Public Protocol、認証・認可、レプリカ移動手順を先行する。
