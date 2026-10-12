# Fortis アーキテクチャ

本書は issue #19 の設計定義を文書化したものである。
Fortis は **Raft + MVCC + Range Sharding + Transaction を備えた、強整合な分散 Key-Value Store** である。
親 umbrella は #20。

## 1. 概要

- データモデルは Key-Value（key / value はバイナリ）。Value の形式（JSON / Protobuf 等）は Fortis 本体で強制しない。
- 合意は shard ごとの Raft Group が担う。1 ノードは複数 Raft Group をホストできる。
- 通常の単一キー操作は linearizable。トランザクションの目標は Strict Serializability。
- フォルトモデルは crash fault / network partition。BFT は対象外。可用性は CP（多数派喪失時は強整合操作を停止）。

## 2. 非目標（SQL はスコープ外）

Fortis 本体では以下を実装しない。必要なら Fortis の KV API を利用する別プロジェクト／上位 layer とする。

- SQL parser / schema・table 抽象 / query planner・optimizer / JOIN / 関係実行エンジン / SQL 互換 layer・プロトコル

## 3. レイヤー構成

#19 の定義順序:

```text
Raft
↓
Snapshot / Recovery
↓
MVCC
↓
Range Sharding
↓
Meta Raft
↓
Replica Migration / Rebalance
↓
Transaction
↓
Operations / Performance
```

## 4. コンポーネント責務

| 層 | 実装（現状） | 責務 |
| --- | --- | --- |
| Raft | `raft/.../runtime`（`RaftRuntime`）、`leader`・`follower`・`candidate`、`transport`（`RaftTransport`、InMemory/Local）、`transport/netty`（QUIC + mTLS、JVM）、`log`（`FileRaftLog` / `MemoryRaftLog`）、`node` | 選出・複製・commit・適用。書き込みは leader の `propose`（majority commit + local apply 後に成功）。読みは `readIndex` / `linearizableRead` で quorum 確認後に local read |
| Snapshot / Recovery | `raft/.../snapshot`（`InstallSnapshotHandler`、`RaftSnapshotApplier`）、`log` / `persistence`（crash-safe な term・vote・log） | 遅延 follower の catch-up、再起動復旧。現状は単一メッセージ受信のみ。チャンク分割転送・スナップショット自体の永続化・`RaftTimer` 以外の Runtime 配線は将来課題 |
| MVCC | `storage-engine/.../mvcc`（`VersionedFortisStorageEngine`、`MemoryMvccStorageEngine`、`MvccSnapshot`） | 版付き KV、スナップショット読み、tombstone、版ベース競合検証（`versionOf`）、pin（`retain` / `retainLatest` / `release`）、GC（`compact`） |
| Range Sharding | `raft/.../sharding`（`RangeTable`、`KeyRange`、`MultiRaftRouter`） | 静的レンジ構成のルーティング計算。**ルーティング専用でストレージ I/O を行わない**。構成変更時は再構築。Runtime 配線・epoch・range scan は将来課題 |
| Meta | `raft/.../meta`（`MetaStore`、`ClusterMembership`、`RangePlacement`、`RebalancePlanner`） | 構成・配置の版付き保持と移動計画の算出。現状はインメモリ。専用 Raft Group による複製・実データ移動は将来課題 |
| Migration / Rebalance | `RebalancePlanner`（`diff` / `planEvenDistribution`） | 移動指示の算出のみ。Learner 追加→catch-up→membership 変更→旧 voter 除外の手順、実データ移動は将来課題 |
| Transaction | `raft/.../txn`（`TxnCoordinator`） | 下記 §7 の方針。現状は非本番シミュレーション（§7 参照） |
| Operations | `raft/.../metrics`（`FortisMetrics`、`RaftMetrics`、`InMemoryMetrics`、`NoOpMetrics`） | §8 の要点 |

基盤として `io`（`Fs` / `Disks` / WAL 用 I/O）と `storage-engine`（Memory / WAL / Disk、`FortisStorageEngine`）を使う。Raft Log と State Machine 用ストレージは分離する。

## 5. データモデルと一貫性

- 単一キー操作: leader 経由の Raft 書き込みと `linearizableRead` により linearizable。
- MVCC の版（`version`）は **Raft log index を想定した単調増加値でありグループローカル**。グループ間のグローバル順序は仮定しない。
- 読みはスナップショット分離: `getAt(key, readVersion)` は `readVersion` 以下の最新エントリを返す。未来版の指定時は最新値を返す。
- 削除は tombstone（`value=null` の履歴）として扱う。`compact(upToVersion)` は各キーの最新1件を残し、pin 留め中の版を破壊する場合は拒否する。
- `MvccSnapshot` は読み取り時点（版）のみを表す。pin 留めは `retain` 系と `release` で明示管理する。

## 6. Public API

KV 操作を中心とする。現状と計画を区別する。

現状（実装済み）:

```text
FortisStorageEngine: get(key) / put(key, value) / delete(key)
RaftCommand: Put / Delete（executeAt で版付き適用）
RaftRuntime: propose / linearizableRead / readIndex
VersionedFortisStorageEngine: putAt / deleteAt / getAt / snapshot 系
TxnCoordinator: begin / get / put / delete / commit / abort / recover（非本番）
```

計画（#19 の方針）:

```text
get(key) / put(key, value) / delete(key)
batch(...) / transaction(...)
```

必要に応じて compare-and-set、range scan、conditional write など KV 向け primitive を追加する。
Client timeout は未確定結果になり得るため、Request ID / Idempotency Key と retry semantics は別途定義する。

## 7. トランザクション方針

Transaction は SQL 実行のためではなく、**複数 key / 複数 shard にまたがる更新の atomicity** のために提供する。

- 単一 shard: Raft atomic batch。
- 複数 shard: `MVCC + 2PC + Transaction Record`。スナップショット分離で読み、prepare（全参加者）→ 全 OK で commit、1 つでも NG で abort。競合検出は値比較ではなく版（`versionOf`）で行う（ABA 対策）。

現状の暫定点（`TxnCoordinator`）:

- 記録はインメモリのみで、確定判断の Raft ログへの耐久化はない。
- 参加者への適用は逐次であり、途中失敗・クラッシュでは一部グループのみ可視の部分的適用が起こり得る。
- `recover` の presumed-abort も適用済みグループを巻き戻せない。
- 本番利用には耐久化された確定判断、冪等な参加者回復、原子可視性が必要。これらは #24 の範囲。

## 8. 運用・監視の要点

- メトリクス（`RaftMetrics`）: `applied_entries` / `proposals_submitted` / `read_index_requests` / `txn_committed`・`txn_aborted` / `snapshots_taken` / `compactions` / `current_term` / `commit_index` / `last_applied`。
- 検証対象: ノード停止、leader 喪失、多数派喪失、QUIC 切断、partial write、snapshot 途中失敗、coordinator 停止、meta 停止。
- 破損した永続状態を empty state に暗黙変換しない。
- 性能向上は safety / recovery を優先してから測定・最適化する。

## 9. 将来課題と関連 issue

- #17: Snapshot / restart recovery（直近優先、Raft 基盤を固める。#19 はブロッカーではない）
- #18: Raft runtime / transport / consistency / recovery foundation
- #20: 全体設計・ロードマップ（親 umbrella）
- #21: MVCC versioned KV と snapshot read
- #22: Range sharding と multi-Raft group routing
- #23: Meta Raft、membership、range placement、rebalance
- #24: KV transaction と cross-shard 2PC recovery
- #25: Storage・observability・security・operations の hardening
- #26: Kotlin Multiplatform CI（Closed）
