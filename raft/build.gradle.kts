plugins {
    kotlin("multiplatform") version "2.3.20"
}

kotlin {
    jvm()
    macosArm64()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
            implementation(project(":io"))
            implementation(project(":storage-engine"))
        }

        jvmMain.dependencies {
            implementation("io.netty:netty-codec-classes-quic:4.2.18.Final")
            runtimeOnly("io.netty:netty-codec-native-quic:4.2.18.Final:linux-x86_64")
            runtimeOnly("io.netty:netty-codec-native-quic:4.2.18.Final:osx-aarch_64")
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.1")
        }
    }
}
