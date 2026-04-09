# Data Protection Extension - Spring Boot Scala Sample

Sample Spring Boot application written in **Scala** demonstrating the Axon Data Protection extension usage with field-level encryption for GDPR compliance in event-sourced applications.

This sample is a **Scala port** of the Java-based `data-protection-spring-boot-sample`, showcasing how to use Axon Framework 5.x with Scala while maintaining GDPR compliance through automatic field-level encryption.

## 📋 Table of Contents

- [Overview](#overview)
- [Scala Integration](#scala-integration)
- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Running the Application](#running-the-application)
- [API Endpoints](#api-endpoints)
- [GDPR Compliance](#gdpr-compliance)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Scala-Specific Notes](#scala-specific-notes)

## 🎯 Overview

This sample demonstrates a **gift card management system** built with:
- **Scala 2.13** for domain logic, commands, events, and projections
- **Axon Framework 5.x** for CQRS/Event Sourcing
- **Spring Boot 3.3** for application framework
- **Data Protection Extension** for automatic field-level encryption
- **PostgreSQL** for event store and projection database
- **Flyway** for database migrations

## 🔧 Scala Integration

This sample shows best practices for integrating Scala with Axon Framework:

### Scala Case Classes
- **Commands & Events**: Pure Scala case classes with `@BeanProperty` annotations
- **Query Models**: Scala case classes for immutable data transfer objects
- **Domain Model**: Scala class with Axon annotations for event sourcing

### Mixed Java/Scala Architecture
- **Scala (15 files)**:
  - Domain model (`GiftCard`)
  - Commands (`IssueGiftCardCommand`, `RedeemGiftCardCommand`)
  - Events (`GiftCardIssuedEvent`, `GiftCardRedeemedEvent`, `Person`, `Address`)
  - Projections (`GiftCardProjection`, `GiftCardSummary`, `GiftCardSummaryList`)
  - Queries (`FindGiftCardQuery`, `FindAllGiftCardsQuery`)
  - Controllers (`DataProtectionController`, `KeyManagementController`)
  - Application main class (`DataProtectionScalaSampleApplication`)

- **Java (7 files - necessary for framework compatibility)**:
  - JPA entities (`GiftCardEntity`, `GiftCardRepository`) - JPA annotations require Java
  - Spring configuration (`DataProtectionConfiguration`, `WebConfig`, `GiftPersonalDataGroup`) - @Configuration works best in Java
  - Main controller (`GiftCardController`) - Complex Spring WebFlux SSE implementation
  - Service (`EventProcessorResetService`) - Deep Axon Framework integration

- **Why?**: Spring `@Configuration`, JPA entities, and complex Spring WebFlux SSE streams work more reliably with Java, while domain logic, commands, events, and projections benefit from Scala's conciseness and immutability

### Key Annotations
```scala
@BeanProperty  // Required for Jackson serialization
@EventTag      // Axon event tagging
@DataSubjectId // GDPR data subject identifier
@PersonalData  // Field-level encryption marker
```

## ✨ Features

### Core Functionality
- ✅ **Issue Gift Cards** with initial balance and encrypted personal data
- ✅ **Redeem Gift Cards** in partial amounts
- ✅ **Query Gift Cards** by ID or retrieve all cards
- ✅ **Real-time Updates** via Server-Sent Events (SSE)

### GDPR Compliance
- 🔐 **Automatic Encryption**: Personal data encrypted before event storage
- 🔓 **Automatic Decryption**: Events decrypted when loaded from event store
- 🗑️ **Cryptographic Erasure**: Delete encryption keys to forget data subject
- 🔑 **Key Management**: JPA-based persistent key storage
- 📊 **Audit Trail**: Complete event history with encrypted personal data

## 🏗️ Architecture

### CQRS/Event Sourcing Pattern

```
┌─────────────┐
│   Command   │ (Scala case classes)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Aggregate  │ (Scala class)
│  (GiftCard) │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Event    │ (Scala case classes with @PersonalData)
│  Encrypted  │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│    Event Store      │
│   (PostgreSQL)      │
│ Encrypted at rest   │
└──────┬──────────────┘
       │
       ▼
┌──────────────────┐
│  Event Handler   │ (Scala projection)
│  (Projection)    │
│  Receives        │
│  Decrypted Data  │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  Query Database  │
│   (PostgreSQL)   │
│  Plain text data │
└──────────────────┘
```

### Encryption Flow

```
1. Command arrives with plain text personal data
2. GiftCard aggregate emits GiftCardIssuedEvent
3. FieldEncryptingConverter intercepts event before storage
4. Personal data fields (@PersonalData) are encrypted
5. Encrypted event stored in event store
6. When event is replayed/loaded:
   - Converter automatically decrypts @PersonalData fields
   - Event handlers receive plain text data
   - Projection database can store plain text for querying
```

## 📦 Requirements

- **Java 21** or higher
- **Scala 2.13.15** (managed by Maven)
- **Maven 3.9+**
- **Docker & Docker Compose** (for PostgreSQL)

## 🚀 Running the Application

### 1. Start PostgreSQL

```bash
cd examples/data-protection-spring-boot-scala-sample
docker-compose up -d
```

### 2. Build the Application

```bash
mvn clean install
```

### 3. Run the Application

```bash
mvn spring-boot:run
```

The application will start on `http://localhost:8080`

### 4. Access the UI

Open `http://localhost:8080` in your browser to access the gift card management interface.

## 🔌 API Endpoints

### Gift Card Operations

#### Issue a new gift card
```bash
POST /api/giftcards/issue
Content-Type: application/json

{
  "amount": 100.00,
  "username": "john.doe",
  "ownerName": "John Doe",
  "ownerAddressLine1": "123 Main St",
  "ownerAddressLine2": "Apt 4B",
  "ownerCountry": "USA",
  "dateOfBirth": "1990-01-15",
  "randomNumber": 42
}
```

#### Redeem from a gift card
```bash
POST /api/giftcards/{giftCardId}/redeem
Content-Type: application/json

{
  "amount": 25.00
}
```

#### Get gift card by ID
```bash
GET /api/giftcards/{giftCardId}
```

#### Get all gift cards
```bash
GET /api/giftcards
```

#### Stream all gift cards (SSE)
```bash
GET /api/giftcards/stream
Accept: text/event-stream
```

### GDPR Operations

#### Forget a data subject (cryptographic erasure)
```bash
DELETE /api/data-protection/forget/{dataSubjectId}
```

This deletes the encryption key for the data subject, making all their encrypted personal data permanently unrecoverable.

#### Get all encryption keys
```bash
GET /api/data-protection/keys
```

## 🔐 GDPR Compliance

### Personal Data Annotations

The sample demonstrates three types of personal data encryption:

> **Important for Scala**: All data protection annotations must use the `@field` meta-annotation to target the field instead of the constructor parameter. This is required because Scala case class parameters are constructor parameters by default, not fields.

#### 1. Field-Level Encryption (`@PersonalData`)
```scala
import io.axoniq.framework.extension.dataprotection.sample.giftcard.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.api.PersonalData
import scala.annotation.meta.field

case class GiftCardIssuedEvent(
  @(PersonalData @field)(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
  username: String
)
```

**Scala-specific syntax**: `@(PersonalData @field)` applies the `@field` meta-annotation to ensure the annotation targets the generated field, not the constructor parameter.

#### 2. Deep Encryption (`@DeepPersonalData`)
```scala
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData
import scala.annotation.meta.field

case class GiftCardIssuedEvent(
  @(DeepPersonalData @field)
  owner: Person  // Person contains nested @PersonalData fields
)
```

#### 3. Serialized Encryption (`@SerializedPersonalData`)
```scala
import io.axoniq.framework.extension.dataprotection.sample.giftcard.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.api.SerializedPersonalData
import scala.annotation.meta.field
import java.time.LocalDate

case class GiftCardIssuedEvent(
  @(SerializedPersonalData @field)(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
  dateOfBirth: LocalDate
)
```

### Data Subject Identifier

```scala
import io.axoniq.framework.extension.dataprotection.sample.giftcard.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.api.DataSubjectId
import org.axonframework.eventsourcing.annotation.EventTag
import scala.annotation.meta.field

case class GiftCardIssuedEvent(
  @EventTag
  @(DataSubjectId @field)(
    group = GiftPersonalDataGroup.GROUP_NAME,
    prefix = GiftPersonalDataGroup.GROUP_PREFIX
  )
  giftCardId: String
)
```

The `giftCardId` serves as the **data subject identifier**. All personal data in events with this ID can be cryptographically erased by deleting the corresponding encryption key.

## 🛠️ Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Scala | 2.13.15 |
| Language | Java | 21 |
| Framework | Spring Boot | 3.3.5 |
| CQRS/ES | Axon Framework | 5.0.0 |
| Data Protection | Axon Data Protection Extension | 5.0.0-SNAPSHOT |
| Database | PostgreSQL | Latest |
| Migration | Flyway | Latest |
| Build | Maven | 3.9+ |
| Scala Build | scala-maven-plugin | 4.9.2 |
| JSON | Jackson + jackson-module-scala | 2.18.2 |

## 📁 Project Structure

```
src/
├── main/
│   ├── scala/                                    # Scala source files
│   │   └── io/axoniq/dataprotection/sample/
│   │       ├── DataProtectionScalaSampleApplication.scala  # Main class
│   │       └── giftcard/
│   │           ├── command/                      # Command models (Scala)
│   │           │   ├── IssueGiftCardCommand.scala
│   │           │   └── RedeemGiftCardCommand.scala
│   │           ├── domain/                       # Aggregate (Scala)
│   │           │   └── GiftCard.scala
│   │           ├── event/                        # Event models (Scala)
│   │           │   ├── Address.scala
│   │           │   ├── Person.scala
│   │           │   ├── GiftCardIssuedEvent.scala
│   │           │   └── GiftCardRedeemedEvent.scala
│   │           └── query/                        # Query models (Scala)
│   │               ├── FindGiftCardQuery.scala
│   │               ├── FindAllGiftCardsQuery.scala
│   │               ├── GiftCardSummary.scala
│   │               ├── GiftCardSummaryList.scala
│   │               └── GiftCardProjection.scala
│   ├── java/                                     # Java source files
│   │   └── io/axoniq/dataprotection/sample/
│   │       ├── api/                              # REST API (Java)
│   │       │   └── KeyManagementController.java
│   │       ├── config/                           # Spring Config (Java)
│   │       │   ├── DataProtectionConfiguration.java
│   │       │   ├── GiftPersonalDataGroup.java
│   │       │   ├── WebConfig.java
│   │       │   └── EventProcessorResetService.java
│   │       └── giftcard/
│   │           ├── query/                        # JPA entities (Java)
│   │           │   ├── GiftCardEntity.java
│   │           │   └── GiftCardRepository.java
│   │           └── web/                          # REST controllers (Java)
│   │               ├── GiftCardController.java
│   │               └── DataProtectionController.java
│   └── resources/
│       ├── application.properties                # Application config
│       ├── db/migration/                         # Flyway migrations
│       └── static/                               # Web UI
└── test/
    └── scala/                                    # Scala tests
```

## 🎓 Scala-Specific Notes

### Case Classes vs Java Records

Scala case classes are used instead of Java records, providing:
- **Immutability** by default
- **Pattern matching** support
- **Copy constructors** for creating modified instances
- **Automatic `equals`, `hashCode`, `toString`**

### @BeanProperty Annotation

All case class parameters must use `@BeanProperty` to work with Jackson:

```scala
case class IssueGiftCardCommand(
  @BeanProperty giftCardId: String,
  @BeanProperty amount: BigDecimal
)
```

This generates Java-style getters/setters required by Jackson serialization.

### Java Interoperability

When calling Java code from Scala:

```scala
// Java Optional → Scala Option
import scala.jdk.OptionConverters._
repository.findById(id).toScala

// Java List → Scala List
import scala.jdk.CollectionConverters._
repository.findAll().asScala.toList
```

### Mixed Compilation

Maven compiles Java and Scala together:
1. Scala code can reference Java code
2. Java code can reference Scala code (with some limitations)
3. `scala-maven-plugin` handles the compilation order

## 🔍 Key Differences from Java Sample

| Aspect | Java Version | Scala Version |
|--------|-------------|---------------|
| Commands/Events | Java records | ✅ **Scala case classes** |
| Domain Model | Java class | ✅ **Scala class** |
| Projection | Java class | ✅ **Scala class** |
| Queries | Java records | ✅ **Scala case classes** |
| Data Controllers | Java | ✅ **Scala** (DataProtection, KeyManagement) |
| Main Controller | Java | Java (complex WebFlux SSE) |
| Config | Java | Java (@Configuration compatibility) |
| JPA Entities | Java | Java (JPA annotation requirement) |
| Immutability | Manual (records) | Built-in (case classes) |
| Pattern Matching | Switch expressions | Native pattern matching |
| Collections | Java Collections | Scala Collections with Java interop |

## 📚 Learning Resources

- [Axon Framework Documentation](https://docs.axoniq.io)
- [Axon Data Protection Extension Guide](https://docs.axoniq.io/extensions/data-protection)
- [Scala Documentation](https://docs.scala-lang.org/)
- [Spring Boot with Scala](https://spring.io/guides)
- [Jackson Scala Module](https://github.com/FasterXML/jackson-module-scala)

## 🤝 Related Samples

- `data-protection-spring-boot-sample` - Java version of this sample
- `data-protection-core` - Core data protection library

## 📝 License

Copyright (c) 2010-2025 AxonIQ B.V.

Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS, Version September 2025.
