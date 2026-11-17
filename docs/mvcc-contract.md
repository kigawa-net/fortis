# 単一 node の MVCC 契約（#21）

この checkout の version store は `storage-engine/.../mvcc/MemoryMvccStorageEngine`。
既存の `VersionedFortisStorageEngine` のメソッドは維持し、新しい操作は具象クラスに追加する。

## Timestamp と write intent

- `nextTimestamp()` はエンジンの Mutex 内で単調 counter を進める。既存の `put` / `delete`、`beginWrite` は同じ counter を使い、`putAt` / `deleteAt` の明示版と compaction floor も追い越す。abort や競合でも発行済み版は再利用しない。`Long.MAX_VALUE` 到達後の発行は例外となる。
- `beginWrite(key, value, reads)` は即座に版を予約する。`value=null` は削除。commit するまで値・履歴・`latestVersion()` に公開しない。入力と取得したバイト配列の変更は内部状態に影響しない。
- `commit(intent)` と `abort(intent)` は生成元のエンジンで一度だけ実行できる。abort と競合した予約は stale となり、GC 対象になる。
- `readVersioned(key)` は値と版を原子的に読む。これを `reads` に渡すと、commit と同じ排他区間で各キーの版を検証する。変更時は `READ_CONFLICT` として書き込みを公開しない。値が元に戻る ABA、未存在キーの挿入・削除、tombstone も版で区別する。読み取り依存のない blind write は空の `reads` を使える。
- write の後勝ちは **予約 timestamp の順序** で決める。後の予約が確定した場合、古い予約は `SUPERSEDED` になる。取得済み snapshot への後付け変更を防ぐため、別キーでも公開済み最大版以下の予約は確定できない。必要なら新しい intent で再試行する。
- 明示版 API は従来どおり順不同の履歴再生を許す。snapshot の一貫性が必要な運用では、Raft 適用順など呼び出し側で版を順序付け、ローカル counter と Raft log index を同時に書き込み源として使わない。

## Pin と GC

- `snapshot()` は版の取得だけ。GC 中も読み取りを保持するには `retainLatest()` で取得と pin を原子的に行うか、`retain(version)` を使う。同じ版の複数 pin は参照数で管理し、取得した回数だけ `release` する。
- `gcSafePoint()` は active pin の最小版を返す。pin がない場合は発行済み最大版（floor を含む）の次版、上限時は `Long.MAX_VALUE`。この境界より低い版を GC できる。
- `collectGarbage()` は同じ排他区間で safe point を計算し、適用済み最大版までの履歴を compact し、境界未満の stale 予約を削除する。返り値は削除した予約数。active intent は stale として回収しない。
- 既存の `compact` と同じく、各キーの境界以前の最新値を後続版の読み取り用に残す。GC 後の `get` は変わらず、floor 未満の `getAt` は例外、`scanPrefixAt` は `Compacted` を返す。floor 自体の読み取りは可能。
- pin の既存契約に従い、floor 以下の版は新規 retain できない。pin なしで最新版まで GC した後は、次の版を書き込むまで `retainLatest()` も失敗する。

## Snapshot と crash 回復

- `checkpoint()` は全履歴、適用済み最大版、発行済み counter、compaction floor、未確定予約の版を原子的に保存する。`restoreCheckpoint()` は空のエンジンへ復元する。これはメモリ上の状態転送 API であり、ディスク永続化は呼び出し側の責務。
- 復旧時は旧プロセスの pin を持ち越さず、未確定予約はすべて stale として GC できる。未確定値は復元も公開もせず、counter は予約版を超えて継続する。checkpoint の配列を変更しても保存内容は変わらない。
- 既存の Raft `StorageEngineStateMachine.snapshotData()` / `restoreSnapshot()` は確定履歴を再生し、キーの版と timestamp counter を再構築できることを統合テストで確認する。この従来形式にはローカルの予約 counter・未確定 intent・floor は含まれない。これらも保持するローカル復旧には checkpoint API を使う。

## 後課題

複数 replica 間の timestamp 衝突回避、HLC／グローバル commit ordering、Raft 経由のローカル intent・version store の同期、および複数 replica の MVCC 検証は今回の対象外。現在の counter の保証範囲は単一エンジン（単一 node の version store）と、その checkpoint からの復旧である。
