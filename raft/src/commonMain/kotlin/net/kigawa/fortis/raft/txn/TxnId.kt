package net.kigawa.fortis.raft.txn

/** トランザクション識別子。UUID が使えない環境向けの決定的な文字列表現。 */
data class TxnId(val value: String)

/**
 * カウンタ式の [TxnId] 生成器。
 *
 * 単調に増加する番号へ接頭辞を付けて発行する。スレッド安全性は想定せず、
 * [TxnCoordinator] 経由の逐次利用を前提とする。
 */
class TxnIdGenerator(private val prefix: String = "txn") {
    private var next: Long = 0L

    /** 次の識別子を発行する。 */
    fun nextId(): TxnId {
        next += 1
        return TxnId("$prefix-$next")
    }
}
