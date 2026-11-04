# 条件付き Atomic Batch / Request Receipt 契約（Issue #44 最小スライス）

対象: 単一 Raft Group 内のみ。Issue #44 の「単一 Group の条件付き batch・受付記録・結果照会」の最小実装。

## 1. 契約

### 条件付き batch（`RaftCommand.ConditionalBatch`）

- 要求 `ConditionalBatchRequest(principal, requestId, conditions, mutations)`。
- 条件: `KvCondition.Absent(key)`（キーが不在。削除済みも不在）、`KvCondition.RevisionEquals(key, revision)`。
- 変更: `KvMutation.Put(key, value)` / `KvMutation.Delete(key)`。
- 1 要求内のキー（条件同士・変更同士）は一意。変更は 1 件以上必須。キー 1..1024 バイト、値 1 MiB 以下、各リスト 64 件以下。
- revision は「そのキーに最後に書き込んだ Raft log index」（削除も書き込み扱い）。未書き込みは 0。
- 適用（State Machine Apply、Raft log 順序で決定的）:
  1. 同一 `(principal, requestId)` の受付記録があれば再実行しない（3 節）。
  2. 全条件を適用時点の確定状態で評価する。
  3. 全条件が成立すれば全変更を同じ log index の版で適用し、`Applied(raftIndex)` を記録する。
  4. 1 つでも不成立なら変更は一切行わず、`Rejected(failedCondition, observedRevision)` を記録する。
- 同一 log index 内の条件評価・変更・受付記録は状態機械の適用直列化の内側で行われ、線形化可能な読み取り（`linearizableRead`）には途中状態を見せない。

### 受付記録（Request Receipt）

- キーは `(principal, requestId)`。保存内容は正規形 `requestDigest`、結果（APPLIED/REJECTED）、log index。
- `requestDigest` は条件と変更の正規エンコードそのもの（ハッシュではない）。同一性は厳密比較される。
- 同一 `requestId` + 同一 digest の再送: 再実行せず保存結果を返す（replay）。
- 同一 `requestId` + 異なる digest: `DigestMismatch`。保存結果は返さず、記録も変更しない。

### Result Lookup（`ConditionalBatchClient.lookupResult`）

- `(principal, requestId, requestDigest)` で受付記録を線形化可能に照会する。実行はしない。
- 結果:
  - `Found(receipt)`: 保存結果（APPLIED/REJECTED）。
  - `DigestMismatch`: 同一 requestId の別内容が記録されている。
  - `ReceiptExpiredOrUnknown`: 受付記録が無い。**未実行の証明ではない**。未適用・退役・未知を区別しない。呼び出し側は「未実行」と解釈してはならない。
- `ConditionalBatchClient.submit` の結果:
  - `Stored(receipt)` / `DigestMismatch` / `OutcomeUnknown`。
  - タイムアウト・Leader 変化・Quorum 喪失・停止で確定結果を確認できない場合は `OutcomeUnknown`。呼び出し側は異なる requestId で再実行してはならず、同じ要求を再送して照合する。

### 跨グループ

- `MultiRaftRouter.routeCommand` は全キーが同一グループに属する場合のみ受け付け、それ以外は `UnsupportedCrossGroupBatchException` で fail closed する。

## 2. 永続化・スナップショット・再起動

- 受付記録は状態機械の一部として保持される（`StorageEngineStateMachine.receipts`）。
- Raft log の符号化（`RaftLogCodec`）と RPC 符号化（`RaftRpcCodec`）は `ConditionalBatch` を command type 4 として扱う。バックアップ（`RaftBackupProcedures` / `RaftBackupCodec`）は log 経由で含まれ、復元後の再生で同じ受付記録になる。
- スナップショット: `StorageEngineStateMachine.snapshotData()` は MVCC 履歴（`VersionedFortisStorageEngine.exportHistory()`、tombstone 含む）と受付記録を符号化する。`restoreSnapshot()` は空の状態機械にのみ復元する。
- 再起動: 永続化された log を新しい状態機械へ再適用すると、同じ状態と受付記録になる（決定的）。同じ log を同じ状態機械へ再適用しても受付記録により状態は変わらない。

## 3. 未実装範囲（本番利用可とは主張しない）

- 跨 Group の原子性（#24 の durable 2PC 完成まで fail closed）。
- 受付記録の GC / retention。現状は永続で退役しないが、退役導入時は `ReceiptExpiredOrUnknown` の契約を維持すること。
- 公開 SDK（#33）・AuthN/AuthZ・Leader Redirect（#34）。`principal` は呼び出し側が渡す値で、認証済みであることは保証しない。
- Grant Epoch / revocation fence との連携（recl 側の要件）。外部 side effect（LLM HTTP 送信等）は batch に含まれない。
- `scanAtRevision`、`CapabilityManifest`、`requestResultLookupV1` 等の capability 広告。
- 3 voter 実プロセス・QUIC・クラッシュ注入による E2E 実証。テストは `LocalRaftTransport` によるインメモリ 3 ノードと log 再生まで。
- ディスク（WAL）上での apply の原子性。現行 WAL にはバッチ書き込みがないため、ディスク永続 storage 上で mutation と受付記録を単一の原子単位にはしていない。受付記録の耐久性は Raft log とスナップショットに依存する。
- スナップショットの Raft 層への接続（lastApplied の復元、log 圧縮との連携、#17/#42/#43）。
- #46（`RaftCommand` の可変 `ByteArray` の防御コピー）は本スライスでは対応しない。`ConditionalBatchRequest` 自体は防御コピーする。
- MVCC の compact 状態（compactedFloor）はスナップショットに含まれない。

## 4. テスト

- `raft/src/commonTest/kotlin/net/kigawa/fortis/raft/batch/`
  - `ConditionalBatchRequestTest`: 要求の検証、防御コピー、正規 digest、ワイヤ・ログ・RPC・バックアップの往復。
  - `ConditionalBatchStateMachineTest`: 条件成功/失敗の原子性、revision、replay、digest 不一致、principal 独立、CAS の唯一の勝者、スナップショット→復元→再送、再起動時の再生の冪等性。
  - `ConditionalBatchClusterTest`: 3 ノード（LocalRaftTransport）での複製、結果照会、Leader 交代後の再送（再提案されない）、確定不能時の `OutcomeUnknown`、ログ再生。
