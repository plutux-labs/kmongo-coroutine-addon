plugins {
    `maven-publish`
    kotlin("jvm") version "1.3.11"
}

group = "com.plutux"
version = "1.1-SNAPSHOT"

repositories {
    jcenter()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.litote.kmongo:kmongo-coroutine:3.9.+")
}

publishing {
    publications {
        create<MavenPublication>("default") {
            from(components["java"])
        }
    }
    repositories {
        maven {
            url = uri("https://oss.sonatype.org/content/repositories/snapshots")
            credentials {
                username = System.getenv("SONATYPE_USER")
                password = System.getenv("SONATYPE_PASSWORD")
            }
        }
    }
}