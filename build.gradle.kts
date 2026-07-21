import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.mavenPublish)
    alias(libs.plugins.signing)
    alias(libs.plugins.dokka)
    alias(libs.plugins.nexusPublish)
    alias(libs.plugins.jmh)
}

version = System.getenv("VERSION") ?: "0.0.0-SNAPSHOT"

val isPublishable = !version.toString().endsWith("-SNAPSHOT")
val isRelease     = Regex("""^\d+\.\d+\.\d+$""").matches(version.toString())
val signingKey: String?     = System.getenv("GPG_SIGNING_KEY")
val signingPassword: String? = System.getenv("GPG_SIGNING_PASSWORD")

dependencies {
    api(libs.bundles.api)
    implementation(libs.bundles.core)
    compileOnly("io.netty:netty-transport-native-epoll:${libs.versions.netty.get()}:linux-x86_64")
    compileOnly("io.netty:netty-transport-native-kqueue:${libs.versions.netty.get()}:osx-x86_64")
    testImplementation(libs.bundles.test)
    jmh(libs.jmh.annprocess)
}

kotlin {
    jvmToolchain(libs.versions.jvm.get().toInt())
}

val jvmTargetVersion = JvmTarget.fromTarget(libs.versions.jvm.get())
tasks.compileKotlin { compilerOptions { jvmTarget.set(jvmTargetVersion) } }
tasks.compileTestKotlin { compilerOptions { jvmTarget.set(jvmTargetVersion) } }

tasks.named("compileJmhKotlin") {
    (this as org.jetbrains.kotlin.gradle.tasks.KotlinCompile)
        .compilerOptions { jvmTarget.set(jvmTargetVersion) }
}

jmh {
    warmupIterations.set(3)
    warmup.set("5s")
    iterations.set(5)
    timeOnIteration.set("10s")
    fork.set(1)
    resultFormat.set("JSON")
    resultsFile.set(project.file("${project.layout.buildDirectory.get()}/reports/jmh/results.json"))
    jvmArgsAppend.add("-Dorg.slf4j.simpleLogger.log.org.openjdk.jmh=WARN")
}

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
