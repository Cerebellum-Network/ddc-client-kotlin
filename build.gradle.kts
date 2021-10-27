import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.32"

    id("com.avast.gradle.docker-compose") version "0.14.3"

    maven
}

group = "com.github.cerebellum-network"

repositories {
    mavenLocal()
    mavenCentral()
}

val vertxVersion = "4.0.3"
val smallryeMutinyVertx = "2.9.0"
dependencies {
    // Vert.x
    implementation("io.vertx:vertx-lang-kotlin:$vertxVersion")
    implementation("io.vertx:vertx-web-client:$vertxVersion")

    // Smallrye
    implementation("io.smallrye.reactive:smallrye-mutiny-vertx-core:$smallryeMutinyVertx")
    implementation("io.smallrye.reactive:smallrye-mutiny-vertx-web-client:$smallryeMutinyVertx")

    // Crypto
    implementation("com.google.crypto.tink:tink:1.5.0")

    // Slf4j
    implementation("org.slf4j:slf4j-api:1.7.30")

    // JSON
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.9.8")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")
    testImplementation("com.google.crypto.tink:tink:1.5.0")
    testImplementation("ch.qos.logback:logback-classic:1.1.7")
    testImplementation("org.mockito.kotlin:mockito-kotlin:4.0.0")
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions {
            jvmTarget = JavaVersion.VERSION_11.toString()
            javaParameters = true
        }
    }

    withType<Test> {
        dependsOn(composeUp)
        finalizedBy(composeDown)
        useJUnitPlatform()
    }
}

dockerCompose {
    useComposeFiles = listOf("docker-compose/docker-compose.yml")
}
