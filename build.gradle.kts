plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
    alias(libs.plugins.graalvm.native)
    alias(libs.plugins.openapi.generator)
    id("com.diffplug.spotless") version "6.25.0"
    id("jacoco")
}

group = "com.sporty.betting"
version = "1.0.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

spotless {
    java {
        googleJavaFormat()
        target("src/**/*.java")
    }
}

jacoco {
    toolVersion = "0.8.11"
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

dependencies {
    // Spring Boot
    implementation(libs.spring.boot.starter.web)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.spring.boot.starter.validation)
    implementation(libs.spring.boot.starter.data.redis)

    // Kafka
    implementation(libs.spring.kafka)
    implementation("org.apache.kafka:kafka-streams")

    // RocketMQ (exclude lz4-java to resolve capability conflict with Kafka 4.x)
    implementation(libs.rocketmq.spring.boot.starter) {
        exclude(group = "org.lz4", module = "lz4-java")
    }

    // OpenAPI
    implementation(libs.springdoc.openapi.starter.webmvc.ui)

    // Jackson
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)

    // Lombok
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    // Test
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.spring.boot.starter.webmvc.test)
    testImplementation("org.springframework.boot:spring-boot-restclient")
    testImplementation(libs.spring.kafka.test)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit.jupiter)

    testImplementation(libs.awaitility)
    testImplementation(libs.mockito.core)

    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)
}

val mockitoAgent = configurations.create("mockitoAgent")

dependencies {
    mockitoAgent(libs.mockito.core) { isTransitive = false }
}

// OpenAPI Generator — contract-first code generation
openApiGenerate {
    generatorName.set("spring")
    inputSpec.set("$projectDir/src/main/resources/openapi/api.yaml")
    outputDir.set("${layout.buildDirectory.get()}/generated/openapi")
    apiPackage.set("com.sporty.betting.api")
    modelPackage.set("com.sporty.betting.api.dto")
    configOptions.set(mapOf(
        "interfaceOnly" to "true",
        "useSpringBoot3" to "true",
        "useTags" to "true",
        "openApiNullable" to "false",
        "skipDefaultInterface" to "false",
        "documentationProvider" to "springdoc",
        "generatedConstructorWithRequiredArgs" to "false"
    ))
}

sourceSets {
    main {
        java {
            srcDir("${layout.buildDirectory.get()}/generated/openapi/src/main/java")
        }
    }
}

tasks.named("compileJava") {
    dependsOn("openApiGenerate")
}

tasks.withType<Test> {
    useJUnitPlatform()
    jvmArgs("-javaagent:${mockitoAgent.asPath}")
}
