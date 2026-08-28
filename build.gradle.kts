plugins {
    kotlin("multiplatform") version "2.3.20"
}

kotlin {
    macosArm64("macos") {
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
        val commonMain = getByName("commonMain")
        val nativeMain = create("nativeMain") {
            dependsOn(commonMain)
            kotlin.srcDirs("src/nativeMain/kotlin")
        }
        val macosMain = getByName("macosMain") {
            dependsOn(nativeMain)
        }
        val linuxMain = getByName("linuxMain") {
            dependsOn(nativeMain)
        }
    }
}
