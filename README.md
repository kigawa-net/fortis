# Fortis

Fortis は、Raft・MVCC・Range Sharding・Transaction を備えた強整合な分散 Key-Value Store を目指す Kotlin Multiplatform プロジェクトです。SQL 層は対象外で、データモデルはバイナリの Key-Value です。

> 開発中です。各フェーズの実装状況は [docs/roadmap.md](docs/roadmap.md) を参照してください。

## モジュール構成

`settings.gradle.kts` で定義されているモジュールは次の 3 つです。

| モジュール | 役割 | ターゲット |
| --- | --- | --- |
| `io` | ファイル I/O 抽象（`fs`）とディスク操作（`disk`）。JVM / Linux / macOS 向けの実装を持つ | jvm, linuxX64, macosArm64 |
| `storage-engine` | ストレージエンジン。Memory / WAL / Disk 実装と MVCC（`mvcc`）を持つ。`io` に依存 | jvm, linuxX64, macosArm64 |
| `raft` | Raft 実装（選挙・ログ複製・スナップショット・永続化）と、sharding / meta / txn / scan / batch 等の上位機能。JVM では Netty QUIC をトランスポートに使う。`io` と `storage-engine` に依存 | jvm, linuxX64, macosArm64 |

ルート直下の `src/nativeMain` は、Linux / macOS 向けネイティブ実行ファイル用のエントリポイント（`Main.kt`）です。

## 前提条件

- JDK 25（CI の `.github/workflows/gradle-ci.yml` で `java-version: "25"` を使用）
- Gradle は同梱の Wrapper（`./gradlew`、Gradle 9.7.1）を使用
- Kotlin 2.3.20（Multiplatform プラグイン）
- Linux / macOS のネイティブテストは、それぞれ対応するホスト OS が必要

## ビルド・テスト

CI で実行されているタスクと同じものです。

```sh
# JVM テスト（io / storage-engine / raft）
./gradlew :io:jvmTest :storage-engine:jvmTest :raft:jvmTest

# Linux ネイティブ（raft）
./gradlew :raft:compileTestKotlinLinuxX64 :raft:linuxX64Test

# macOS ネイティブ（raft、macOS 上で実行）
./gradlew :raft:macosArm64Test
```

## ドキュメント

- [docs/architecture.md](docs/architecture.md) — アーキテクチャと設計方針
- [docs/roadmap.md](docs/roadmap.md) — フェーズ別の実装状況と残件
- [docs/conditional-batch-contract.md](docs/conditional-batch-contract.md) — 条件付きバッチの契約
- [docs/revision-scan-contract.md](docs/revision-scan-contract.md) — revision 固定 prefix scan の契約
