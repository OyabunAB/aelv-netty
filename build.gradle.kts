import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.mavenPublish)
    alias(libs.plugins.signing)
    alias(libs.plugins.dokka)
    alias(libs.plugins.nexusPublish)
}

version = System.getenv("VERSION") ?: "0.0.0-SNAPSHOT"

val isPublishable = !version.toString().endsWith("-SNAPSHOT")
val isRelease     = Regex("""^\d+\.\d+\.\d+$""").matches(version.toString())
val signingKey: String?     = System.getenv("GPG_SIGNING_KEY")
val signingPassword: String? = System.getenv("GPG_SIGNING_PASSWORD")

dependencies {
    api(libs.bundles.api)
    implementation(libs.bundles.core)
    testImplementation(libs.bundles.test)
}

kotlin {
    jvmToolchain(libs.versions.jvm.get().toInt())
}

val jvmTargetVersion = JvmTarget.fromTarget(libs.versions.jvm.get())
tasks.compileKotlin { compilerOptions { jvmTarget.set(jvmTargetVersion) } }
tasks.compileTestKotlin { compilerOptions { jvmTarget.set(jvmTargetVersion) } }

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("started", "passed", "skipped", "failed")
        showStandardStreams = true
    }
}

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    archiveClassifier = "sources"
    from(sourceSets.main.map { it.allSource })
}

val javadocJar = tasks.register<Jar>("javadocJar") {
    dependsOn(tasks.dokkaGenerate)
    archiveClassifier = "javadoc"
    from(layout.buildDirectory.dir("dokka/html"))
}

if (isPublishable && signingKey != null) {
    signing {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications)
    }
}

if (isRelease) {
    nexusPublishing {
        repositories {
            sonatype {
                username.set(System.getenv("OSSRH_USERNAME"))
                password.set(System.getenv("OSSRH_PASSWORD"))
            }
        }
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/OyabunAB/aelv-netty")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(sourcesJar)
            artifact(javadocJar)
            pom {
                name = "aelv-netty"
                description = "Netty transport bridge for aelv — reactive streams over Netty channels without Reactor."
                url = "https://github.com/OyabunAB/aelv-netty"
                licenses {
                    license {
                        name = "Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/OyabunAB/aelv-netty.git"
                    developerConnection = "scm:git:ssh://github.com:OyabunAB/aelv-netty.git"
                    url = "https://github.com/OyabunAB/aelv-netty"
                }
                developers {
                    developer {
                        id = "dansun"
                        name = "Daniel Sundberg"
                        email = "daniel.sundberg@oyabun.se"
                    }
                }
            }
        }
    }
}
