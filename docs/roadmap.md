# Fortis ロードマップ

本書は issue #20（全体追跡用 umbrella）の現状追跡である。
設計定義は `docs/architecture.md` を参照。本書はフェーズ・状態・残件に特化し、設計の重複記述は避ける。

> 注記: #20 本文は計画時点の記述であり、チェックボックス状態も計画当時のものである。
> 本書が feature/raft ブランチ上の現状を正とする。#20 本文との差分は「#20 本文との差分注記」にまとめる。

## 目的と対応表

| Phase | Issue | 役割 | 状態 |
| --- | --- | --- | --- |
| 1 | — | Storage 基盤 | 完了 |
| 2 | #18 / #17 / #26 | Raft 基盤 / Snapshot 回復 / CI | ほぼ完了（#17 は最小実装） |
| 3 | #21 | MVCC・版付き KV・snapshot read | 部分（最小実装） |
| 4 | #22 | Range Sharding・Multi-Raft・Routing | 部分（最小実装） |
| 5 | #23 | Meta Raft・Membership・配置・Rebalance | 部分（最小実装） |
| 6 | #24 | KV Transaction・Cross-Shard 2PC・Recovery | 部分（最小実装） |
| 7 | #25 | LSM・可観測性・セキュリティ・運用 | 未着手（第一歩のみ） |

#19 は SQL 非対応の方針定義であり、実装フェーズとは分けて扱う。

## フェーズ別状態

### Phase 1 — Storage 基盤: 完了

- Memory / File I/O / WAL / Disk、Raft durable log は利用可能。

### Phase 2 — Raft 基盤: ほぼ完了

- 実装済み: 選挙 / 複製 / QUIC + mTLS / 並列 RPC / ReadIndex / NoOp / 提案完了 / crash-safe な term・vote・log（`raft/.../runtime`、`transport/netty`、`log`）。
- #26: CI（`.github/workflows/gradle-ci.yml`）3 ジョブ緑。
- #17 最小実装（`raft/.../snapshot`）: 単一メッセージ InstallSnapshot のみ。
  - 残件: チャンク分割転送、スナップショット自体の永続化、Runtime 配線、restart 復旧の完成。

### Phase 3 — MVCC (#21): 部分

- 実装済み（`storage-engine/.../mvcc`）: 版付き KV、snapshot read、tombstone、retain / release によるピン留め、compact 時のピン留め保護。
- 残件: HLC / commit ordering、write intent、conflict detection の完成、GC safe-point の厳密化。

### Phase 4 — Range Sharding (#22): 部分

- 実装済み（`raft/.../sharding`）: ルーティング計算専用（`RangeTable`、`MultiRaftRouter`）。ストレージ I/O を行わない。
- 残件: epoch、古い epoch 検出・再ルーティング、range scan、Runtime 配線、実 Multi-Raft。

### Phase 5 — Cluster metadata (#23): 部分

- 実装済み（`raft/.../meta`）: インメモリ Meta（`MetaStore`、`ClusterMembership`、`RangePlacement`、`RebalancePlanner` による移動計画算出のみ）。
- 残件: 専用 Raft Group による複製、実データ移動、Learner 追加→catch-up→membership 変更の手順、Split / Merge。

### Phase 6 — Transaction (#24): 部分

- 実装済み（`raft/.../txn`）: 非本番シミュレーション（`TxnCoordinator` の begin / get / put / delete / commit / abort / recover）。
- 残件: durable decision record、冪等な参加者回復、原子可視性、Strict Serializability 検証。現状は一部グループのみ可視の部分的適用が起こり得る。

### Phase 7 — Hardening (#25): 未着手

- 実装済み（`raft/.../metrics`）: メトリクス基盤のみ（`FortisMetrics`、`RaftMetrics` 等）。
- 残件: LSM、本番セキュリティ（認証・認可・証明書ローテーション）、tracing、backup / restore、failure injection、partition test、benchmark、運用手順。

## #20 本文との差分注記

- #20 本文の Phase チェックボックスは計画時点のものであり、本書では転記していない。
- 差分の要点: Phase 2 の Snapshot 項目は「未実装」ではなく「最小実装（単一メッセージ）」が正。
  Phase 3〜6 は本文上未着手に見えるが、現状はいずれも最小実装（暫定）が存在し「部分」とする。
  #26 は本文上未完了だが現状は 3 ジョブ緑である。
- #20 本文の「PR #8 レビュー後の優先修正」「現在地」等の時系列メモは本書に転記しない。最新状態は本書のフェーズ別状態を正とする。

## 未決定事項（#20 本文から転記）

- Key / Value サイズ上限と大容量 Value の扱い
- Public Protocol（QUIC / gRPC / HTTP など）
- LSM 実装方針（独自 / 既存 Engine）
- Timestamp・Commit Ordering・Isolation
- Deadlock / lock recovery
- Raft Membership Change 手順
- Meta Raft bootstrap / recovery / quorum 配置
- Streaming / Incremental Snapshot
- Range Split の consistent cut / routing 切替
- API Authentication / Authorization

## 優先順位（現状に合わせた次の順序案）

1. #17: Snapshot の durable 化・チャンク転送・Runtime 配線・restart 復旧の完成（Raft 基盤の締め）
2. #21: MVCC の intent / conflict / ordering / GC 厳密化
3. #22: epoch・再ルーティング・range scan・Runtime 配線
4. #23: Meta 複製・実データ移動・membership 変更手順
5. #24: durable decision・回復・原子可視性・Strict Serializability 検証
6. #25: セキュリティ・backup・障害試験・性能測定（安全性に関わるものは先行して並走）
7. 未決定事項の順次確定（Protocol、認証・認可、Membership 手順を先行）
