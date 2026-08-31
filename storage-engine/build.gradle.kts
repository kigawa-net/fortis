plugins {
    kotlin("multiplatform")
}

kotlin {
    jvm()
    macosArm64()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
        }
    }
}
