package net.kigawa.fortis.storage.engine.mvcc

import net.kigawa.fortis.storage.engine.FortisStorageEngine

// MVCC 版 KV。version は Raft log index を想定する単調増加値。
interface VersionedFortisStorageEngine : FortisStorageEngine {
    // 指定 version で書き込む。tombstone は value=null の履歴として扱う。
    suspend fun putAt(key: ByteArray, value: ByteArray, version: Long)

    // 指定 version で削除する。version 直前で値が見えていた場合 true。
    suspend fun deleteAt(key: ByteArray, version: Long): Boolean

    // readVersion 以下の最新エントリを返す。
    // readVersion が適用済み最大版より大きい場合は最新値を返す（未来読みは最新状態を見る）。
    suspend fun getAt(key: ByteArray, readVersion: Long): ByteArray?

    // key に影響した最新 version。履歴なしは 0。
    // トランザクションの版ベース競合検証に使う（値比較では ABA を検出できないため）。
    suspend fun versionOf(key: ByteArray): Long

    // 適用済み最大 version。未適用時は 0。
    suspend fun latestVersion(): Long

    // 現在の最新版を指すスナップショットを返す。ピン留めはしない。
    suspend fun snapshot(): MvccSnapshot

    // 現在の最新版をスナップショット取得と同時にピン留めする。
    // snapshot()→retain() の途中で compact が入る隙間を作らない。
    suspend fun retainLatest(): MvccSnapshot

    // 指定 version をピン留めし、compact から保護する。
    // 返したスナップショットは使い終わったら release で解放すること。
    // version は 1 以上かつ適用済み最大版以下であること。
    suspend fun retain(version: Long): MvccSnapshot

    // retain で取得したスナップショットを解放する。未保持の解放は失敗する。
    suspend fun release(snapshot: MvccSnapshot)

    // 保持中の全履歴を版昇順で返す。スナップショット取得用。compact で削除済みの版は含まない。
    suspend fun exportHistory(): List<MvccHistoryRecord>

    // readVersion 時点で prefix に一致し startAfter より大きい可視キーを、バイト辞書順に最大 limit 件返す。
    // startAfter=null は先頭から。tombstone は除外する。
    // 1 回の呼び出しは単一の排他区間で評価されるため、compact が途中に入って結果が混ざることはない。
    // readVersion が compact で失われていれば Compacted を返す（readVersion >= floor なら版は保持されている）。
    // readVersion は 0 以上かつ latestVersion() 以下であること。
    suspend fun scanPrefixAt(
        prefix: ByteArray,
        readVersion: Long,
        startAfter: ByteArray?,
        limit: Int,
    ): MvccScanResult

    // 各キーについて upToVersion 以下の履歴のうち最新1件だけ残す。
    // 最新読みの結果は変わらない。
    // ピン留め中の最小 version 以上の履歴を破壊する場合は IllegalArgumentException。
    suspend fun compact(upToVersion: Long)
}
