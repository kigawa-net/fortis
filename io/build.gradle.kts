plugins {
    kotlin("multiplatform") version "2.3.20"
}

kotlin {
    jvm()
    macosArm64()
    linuxX64()
    
    sourceSets {
        val commonMain = getByName("commonMain")
    }
}
