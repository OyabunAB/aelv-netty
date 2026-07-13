rootProject.name = "aelv-netty"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        mavenLocal()
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/OyabunAB/*")
            credentials {
                username = settings.providers.gradleProperty("gpr.user").orNull ?: System.getenv("GITHUB_ACTOR")
                password = settings.providers.gradleProperty("gpr.key").orNull ?: System.getenv("GITHUB_TOKEN") ?: System.getenv("GHA_READ_PACKAGES_TOKEN")
            }
        }
    }
}
