# Revision 固定 Prefix Scan 契約（Issue #45 MVP）

対象: 単一 Raft Group 内のみ。`RevisionScanClient.scanAtRevision` の最小スライス。
本番利用可とは主張しない。認可・Snapshot lease・Watch 連携は未実装（3 節）。

## 1. API

- `ScanRequest(prefix, readRevision?, cursor?, pageSize)`
  - `prefix`: 0..1024 バイト。空なら全キーが対象。
  - `pageSize`: 1..1000。
  - `readRevision`: null なら線形化可能読み取りで現在の適用済み最大 revision を確定する。指定時はその revision で読む（適用済み・compact 未満であること）。`cursor` と併用する場合は cursor の revision と一致が必要。
  - `cursor`: 前ページの `ScanResult.Page.nextCursor`。null なら先頭ページ。
- `ScanResult`
  - `Page(entries, revision, nextCursor)`: `nextCursor == null` のときのみ列挙が完了している。
  - `RevisionCompacted(floor)`: 固定 revision が compact で失われた。
  - `RevisionUnavailable(revision, latest)`: 要求 revision がまだ適用されていない。
  - `InvalidCursor(reason)`: cursor の形式・チェックサム・prefix・revision が不正。
  - `Unavailable(reason)`: 線形化可能読み取りを確定できない（`NOT_LEADER` / `LEADERSHIP_LOST` / `STOPPED` / `TIMEOUT`）。再試行可能。
  - `Unsupported(reason)`: 状態機械が `StorageEngineStateMachine` でない。

## 2. 契約

- 順序: キーはバイト列の符号なし辞書順（接頭辞は短い方が小さい）。
- 固定 revision: 初回は `linearizableRead` の中で revision を確定し、同じ読み取りで 1 ページ目を返す。以後の適用は必ず確定 revision より大きい版でのみ書き込むため、後続の put/delete は結果に見えない。
- tombstone: 読み取り revision で最新の版が削除マーカーなら、そのキーはページに含めない。
- 完全性: 続きがある場合は必ず `nextCursor` を返す。`nextCursor == null` は列挙完了を意味する。
- 重複・欠落なし: 同じ cursor 系列は、固定 revision で prefix 内かつ直前ページの最終キーより大きい可視キーを順に返す。
- MVCC compact:
  - `compact(u)` で `u <= revision` の場合、固定 revision の結果は変わらない。
  - `compact(u)` で `u > revision` の後は `RevisionCompacted(floor)` を返し、部分ページは返さない。
  - スキャンは版を pin せず GC を止めない。失効は明示エラーで扱う（Snapshot lease は未実装）。
  - 1 ページの評価は単一の排他区間で行うため、ページ内で compact が混ざることはない。
- cursor: `version | revision | prefix | lastKey | FNV-1a 64 checksum` の不透明バイト列。
  - 形式・チェックサム不一致、別 prefix、lastKey が prefix 外、revision 不一致は `InvalidCursor`。
  - チェックサムは破損検出のみ。**真正性（改ざん耐性・認可）は保証しない**。認可は #34 の範囲。
- Leader 交代: revision は commit 済みの状態を指すため、新 Leader でも線形化バリア後に同じ結果を返す。読み取りを確定できなければ `Unavailable` で、部分結果は返さない（fail closed）。
- 空キーは有効なキーとして扱い、cursor の境界にもなる。

## 3. 未実装範囲（本番利用可とは主張しない）

- namespace/tenant の AuthN/AuthZ、server-derived namespace（#34）。現状 `ScanRequest` に namespace はなく、prefix の認可は呼び出し側の責務。
- cursor の MAC による真正性、cursor の TTL、snapshot lease と Renew/Close API。
- MVCC の GC 保護（pin による compact 抑止）。現状は `RevisionCompacted` による明示エラーで代替する。
- `groupId` / `placementEpoch` の返却、複数 Range・複数 Group にまたがる scan（非対応。fail closed の扱いも未定義）。
- Watch（#32）による再走査トリガ、Client SDK（#33）、`scanAtRevisionV1` の capability 広告。
- **MVCC の compact 下限（`compactedFloor`）は snapshot に含まれない。** snapshot 復元後の新 Leader は下限を 0 とみなすため、compact 済みの古い revision の cursor を拒否できない。現在 Raft 層は compact を呼ばないため実害はないが、compact を導入する前に解決が必要。
- メモリ MVCC 実装はページごとに全キーを整列する（O(n log n)）。性能・メモリ予算・レート制限は未定義。
- 3 voter 実プロセス・QUIC・クラッシュ注入による E2E 検証は未実施。

## 4. テスト

- `storage-engine/src/commonTest/.../mvcc/MvccPrefixScanTest.kt`
  - 可視キーの順序、prefix 境界、任意の limit でのページング（欠落・重複なし）、tombstone、固定版の不変性、compact 後の版、不正引数。
- `raft/src/commonTest/.../scan/RevisionScanClientTest.kt`（単一ノード `RaftRuntime`）
  - pageSize 1/7/256 での全件列挙、1 ページ目の後の更新・削除・追加に対する固定 revision の不変性、tombstone、最終ページの完了表示、compact 超過での明示エラー、compact 以下での継続、cursor 改ざん・切り詰め・prefix 流用・revision 不一致の拒否、未適用 revision、非 Leader の `Unavailable`、空キーと符号なし順。
