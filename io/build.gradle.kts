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
        }
    }
}
