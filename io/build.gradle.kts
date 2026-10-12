plugins {
    kotlin("multiplatform") version "2.3.20"
}

kotlin {
    jvm()
    macosArm64()
    linuxX64()
    
    sourceSets {
        val nativeMain = create("nativeMain") {
            dependsOn(commonMain.get())
        }
        macosArm64Main.get().dependsOn(nativeMain)
        linuxX64Main.get().dependsOn(nativeMain)

        commonMain.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
        }
    }
}
