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

    // 適用済み最大 version。未適用時は 0。
    suspend fun latestVersion(): Long

    // 現在の最新版を指すスナップショットを返す。
    suspend fun snapshot(): MvccSnapshot

    // 各キーについて upToVersion 以下の履歴のうち最新1件だけ残す。
    // 最新読みの結果は変わらない。
    suspend fun compact(upToVersion: Long)
}
