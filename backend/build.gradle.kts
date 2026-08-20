plugins {
    java
    id("org.springframework.boot") version "4.1.0"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.realtime"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-websocket")
    implementation("org.springframework.kafka:spring-kafka")

    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    implementation("com.zaxxer:HikariCP")
    implementation("com.clickhouse:clickhouse-jdbc:0.8.6:all") {
        // Kafka 4 uses the source-compatible at.yawk LZ4 fork.
        exclude(group = "org.lz4", module = "lz4-java")
    }
    runtimeOnly("at.yawk.lz4:lz4-java:1.10.1")
    runtimeOnly("com.mysql:mysql-connector-j")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testCompileOnly("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")
}

tasks.withType<Test> {
    useJUnitPlatform()
    // Allow opt-in real JSON consistency regressions without making fixtures mandatory in CI.
    System.getProperty("mbo.json")?.let { systemProperty("mbo.json", it) }
    System.getProperty("atas.json")?.let { systemProperty("atas.json", it) }
}

tasks.register<JavaExec>("officialOrderBookAudit") {
    group = "verification"
    description = "Runs the Java MboBookEngine official MBP-10/TBBO audit line protocol"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.realtime.marketdata.orderbook.audit.OfficialOrderBookAuditMain")
    standardInput = System.`in`
}
