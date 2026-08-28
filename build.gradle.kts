plugins {
    kotlin("multiplatform") version "2.1.0"
}

repositories {
    mavenCentral()
}

kotlin {
    macosX64("macos") {
        binaries {
            executable()
        }
    }
    linuxX64("linux") {
        binaries {
            executable()
        }
    }

    sourceSets {
        val commonMain by getting
        val nativeMain by creating {
            dependsOn(commonMain)
            kotlin.srcDirs("src/nativeMain/kotlin")
        }
        val macosMain by getting {
            dependsOn(nativeMain)
        }
        val linuxMain by getting {
            dependsOn(nativeMain)
        }
    }
}
