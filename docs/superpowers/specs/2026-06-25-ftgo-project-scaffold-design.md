# FTGO Project Scaffold — Design Spec

**Date:** 2026-06-25  
**Status:** Approved  
**Scope:** Multi-module Gradle project scaffold for my-food-to-go-app with 6 Spring Boot stub services and local infrastructure

---

## Context

Sanjay is building the FTGO application alongside reading *Microservices Patterns* by Chris Richardson (Ch. 1–2 complete). This scaffold establishes the project skeleton before Ch. 3 (interprocess communication). All implementation details beyond structure are intentionally deferred to the relevant chapters.

---

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Hexagonal layers | Java packages (not sub-projects) | Matches reference impl; no extra Gradle complexity |
| Shared common module | None yet | YAGNI — extract when Ch. 4 sagas force shared event types |
| Dependency set | Minimal (web, jpa, mysql, test) | Eventuate Tram added in Ch. 3 when messaging is introduced |
| MySQL topology | Single instance, one schema per service | Lower local resource usage; matches Richardson's own Compose setup |
| Gradle layout | Flat multi-module | Simplest; matches reference implementation structure |

---

## Project Layout

```
my-food-to-go-app/
├── build.gradle
├── settings.gradle
├── gradlew
├── gradlew.bat
├── gradle/wrapper/
│   ├── gradle-wrapper.jar
│   └── gradle-wrapper.properties
├── compose.yml
├── infrastructure/
│   └── mysql/
│       └── init.sql          ← creates all 6 schemas on first boot
├── CONTEXT.md
├── docs/
│   └── superpowers/
│       └── specs/
│           └── 2026-06-25-ftgo-project-scaffold-design.md
├── ftgo-consumer-service/
├── ftgo-order-service/
├── ftgo-kitchen-service/
├── ftgo-accounting-service/
├── ftgo-restaurant-service/
└── ftgo-delivery-service/
```

---

## Per-Service Module Layout

Each service is identical in structure. Example using `ftgo-order-service`:

```
ftgo-order-service/
├── build.gradle
└── src/
    ├── main/
    │   ├── java/com/sanjay/ftgo/order/
    │   │   ├── FtgoOrderServiceApplication.java
    │   │   ├── api/
    │   │   │   └── .gitkeep
    │   │   ├── domain/
    │   │   │   └── .gitkeep
    │   │   └── infrastructure/
    │   │       └── .gitkeep
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/sanjay/ftgo/order/
            └── FtgoOrderServiceApplicationTests.java
```

### Base package convention

`com.sanjay.ftgo.<servicename>` where `<servicename>` is the single-word domain name:

| Module | Base package |
|--------|-------------|
| ftgo-consumer-service | `com.sanjay.ftgo.consumer` |
| ftgo-order-service | `com.sanjay.ftgo.order` |
| ftgo-kitchen-service | `com.sanjay.ftgo.kitchen` |
| ftgo-accounting-service | `com.sanjay.ftgo.accounting` |
| ftgo-restaurant-service | `com.sanjay.ftgo.restaurant` |
| ftgo-delivery-service | `com.sanjay.ftgo.delivery` |

### Package responsibilities (stub — empty, will fill per chapter)

- `api/` — inbound adapters: REST controllers, messaging listeners
- `domain/` — aggregates, domain services, ports (interfaces)
- `infrastructure/` — outbound adapters: JPA repositories, Kafka publishers

---

## Gradle Configuration

### Versions

| Tool | Version |
|------|---------|
| Java | 21 |
| Spring Boot | 3.5.3 |
| Spring Dependency Management | 1.1.7 |
| Gradle Wrapper | 8.14.2 |

### Root `build.gradle`

```groovy
plugins {
    id 'org.springframework.boot' version '3.5.3' apply false
    id 'io.spring.dependency-management' version '1.1.7' apply false
    id 'java' apply false
}

subprojects {
    apply plugin: 'java'
    apply plugin: 'org.springframework.boot'
    apply plugin: 'io.spring.dependency-management'

    group = 'com.sanjay.ftgo'
    version = '0.0.1-SNAPSHOT'

    java {
        sourceCompatibility = JavaVersion.VERSION_21
    }

    repositories {
        mavenCentral()
    }

    dependencies {
        implementation 'org.springframework.boot:spring-boot-starter-web'
        implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
        runtimeOnly    'com.mysql:mysql-connector-j'
        testImplementation 'org.springframework.boot:spring-boot-starter-test'
    }

    test {
        useJUnitPlatform()
    }
}
```

### `settings.gradle`

```groovy
rootProject.name = 'my-food-to-go-app'

include 'ftgo-consumer-service'
include 'ftgo-order-service'
include 'ftgo-kitchen-service'
include 'ftgo-accounting-service'
include 'ftgo-restaurant-service'
include 'ftgo-delivery-service'
```

### Per-service `build.gradle`

Each service `build.gradle` is intentionally minimal — all shared config comes from root:

```groovy
// No additional config needed at stub stage
// Service-specific dependencies added per chapter
```

---

## Service Ports

| Service | Port | MySQL Schema |
|---------|------|-------------|
| ftgo-consumer-service | 8081 | `ftgo_consumer` |
| ftgo-order-service | 8082 | `ftgo_order` |
| ftgo-kitchen-service | 8083 | `ftgo_kitchen` |
| ftgo-accounting-service | 8084 | `ftgo_accounting` |
| ftgo-restaurant-service | 8085 | `ftgo_restaurant` |
| ftgo-delivery-service | 8086 | `ftgo_delivery` |

---

## Docker Compose / Infrastructure

`compose.yml` defines three containers:

| Container | Image | Port |
|-----------|-------|------|
| `mysql` | `mysql:8.4` | 3306 |
| `zookeeper` | `confluentinc/cp-zookeeper:7.9` | 2181 |
| `kafka` | `confluentinc/cp-kafka:7.9` | 9092 |

MySQL credentials: `ftgo` / `ftgo`

`infrastructure/mysql/init.sql` creates all schemas on first boot:

```sql
CREATE DATABASE IF NOT EXISTS ftgo_consumer;
CREATE DATABASE IF NOT EXISTS ftgo_order;
CREATE DATABASE IF NOT EXISTS ftgo_kitchen;
CREATE DATABASE IF NOT EXISTS ftgo_accounting;
CREATE DATABASE IF NOT EXISTS ftgo_restaurant;
CREATE DATABASE IF NOT EXISTS ftgo_delivery;
```

---

## Stub File Content

### `application.yml` (example: ftgo-order-service)

```yaml
spring:
  application:
    name: ftgo-order-service
  datasource:
    url: jdbc:mysql://localhost:3306/ftgo_order
    username: ftgo
    password: ftgo
  jpa:
    hibernate:
      ddl-auto: update

server:
  port: 8082
```

### `FtgoOrderServiceApplication.java`

```java
package com.sanjay.ftgo.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FtgoOrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(FtgoOrderServiceApplication.class, args);
    }
}
```

### `FtgoOrderServiceApplicationTests.java`

```java
package com.sanjay.ftgo.order;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class FtgoOrderServiceApplicationTests {
    @Test
    void contextLoads() {}
}
```

---

## Out of Scope (deferred to future chapters)

- Eventuate Tram / messaging wiring (Ch. 3)
- Saga orchestration (Ch. 4)
- Domain model / aggregate implementation (Ch. 5)
- Shared common module (extract when Ch. 4 requires shared event types)
- Kubernetes / production deployment (Ch. 12)
- ftgo-api-gateway (Ch. 8)
