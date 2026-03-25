# Data Protection Extension - Spring Boot Kotlin Sample

Sample Spring Boot application written in **Kotlin** demonstrating the Axon Data Protection extension usage with field-level encryption for GDPR compliance in event-sourced applications.

This sample is a **Kotlin port** of the Java-based `data-protection-spring-boot-sample`, showcasing how to use Axon Framework 5.x with Kotlin while maintaining GDPR compliance through automatic field-level encryption.

## Table of Contents

- [Overview](#overview)
- [Kotlin Integration](#kotlin-integration)
- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Running the Application](#running-the-application)
- [API Endpoints](#api-endpoints)
- [GDPR Compliance](#gdpr-compliance)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Kotlin-Specific Notes](#kotlin-specific-notes)
- [Key Differences from Java/Scala Samples](#key-differences-from-javascala-samples)

## Overview

This sample demonstrates a **gift card management system** built with:
- **Kotlin 2.1.0** for all application code
- **Axon Framework 5.x** for CQRS/Event Sourcing
- **Spring Boot 3.3** for application framework
- **Data Protection Extension** for automatic field-level encryption
- **PostgreSQL** for event store and projection database
- **Flyway** for database migrations

## Kotlin Integration

This sample shows best practices for integrating Kotlin with Axon Framework:

### Pure Kotlin Implementation
- **100% Kotlin** - All 20 Kotlin files, no Java files needed!
- **Data Classes** for immutable commands, events, and queries
- **Regular Classes** for JPA entities (with mutable properties)
- **Object Declarations** for singleton configuration constants
- **Named Parameters** for clear, self-documenting code
- **Null Safety** with Kotlin's type system (`String?` for nullable fields)
- **Extension Functions** and idiomatic Kotlin patterns

### Simplest Annotation Syntax

Kotlin has the **simplest syntax** for Data Protection annotations compared to Java and Scala:

**Kotlin** (cleanest - NO `@field` needed!):
```kotlin
data class GiftCardIssuedEvent(
    @EventTag
    @DataSubjectId(group = "gift", prefix = "gift-")
    val giftCardId: String,

    @PersonalData(group = "gift", replacement = "<removed>")
    val username: String,

    @DeepPersonalData
    val owner: Person
)
```

**Scala** (requires `@field` meta-annotation):
```scala
case class GiftCardIssuedEvent(
  @(DataSubjectId @field)(group = "gift", prefix = "gift-")  // Verbose!
  giftCardId: String,

  @(PersonalData @field)(group = "gift", replacement = "<removed>")
  username: String
)
```

**Java** (verbose with explicit constructors):
```java
public record GiftCardIssuedEvent(
    @EventTag
    @DataSubjectId(group = "gift", prefix = "gift-")
    String giftCardId,

    @PersonalData(group = "gift", replacement = "<removed>")
    String username
) {}
```

**Why is Kotlin simpler?** Annotations on data class properties automatically target fields, not constructor parameters - no meta-annotations needed!

## Features

### Core Functionality
- Issue Gift Cards with initial balance and encrypted personal data
- Redeem Gift Cards in partial amounts
- Query Gift Cards by ID or retrieve all cards
- Real-time Updates via Server-Sent Events (SSE)

### GDPR Compliance
- Automatic Encryption: Personal data encrypted before event storage
- Automatic Decryption: Events decrypted when loaded from event store
- Cryptographic Erasure: Delete encryption keys to forget data subject
- Key Management: JPA-based persistent key storage
- Audit Trail: Complete event history with encrypted personal data

## Architecture

### CQRS/Event Sourcing Pattern

```
┌─────────────┐
│   Command   │ (Kotlin data class)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Entity  │ (Kotlin class)
│  (GiftCard) │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Event    │ (Kotlin data class with @PersonalData)
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
│  Event Handler   │ (Kotlin projection)
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
2. GiftCard entity emits GiftCardIssuedEvent
3. FieldEncryptingConverter intercepts event before storage
4. Personal data fields (@PersonalData) are encrypted
5. Encrypted event stored in event store
6. When event is replayed/loaded:
   - Converter automatically decrypts @PersonalData fields
   - Event handlers receive plain text data
   - Projection database can store plain text for querying
```

## Requirements

- **Java 21** or higher
- **Kotlin 2.1.0** (managed by Maven)
- **Maven 3.9+**
- **Docker & Docker Compose** (for PostgreSQL)

## Running the Application

### 1. Start PostgreSQL

```bash
cd examples/data-protection-spring-boot-kotlin-sample
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

## API Endpoints

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

## GDPR Compliance

### Personal Data Annotations

The sample demonstrates three types of personal data encryption with Kotlin's clean, simple syntax:

> **Kotlin Advantage**: Unlike Scala, Kotlin does **NOT** require `@field` meta-annotation on data class properties! Annotations automatically target the backing field, making the syntax cleaner and more concise.

#### 1. Field-Level Encryption (`@PersonalData`)

```kotlin
import io.axoniq.framework.extension.dataprotection.sample.giftcard.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.api.PersonalData

data class GiftCardIssuedEvent(
    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    val username: String  // Simple! No @field needed!
)
```

**Kotlin simplicity**: Annotations on data class properties automatically apply to the field, not the constructor parameter.

#### 2. Deep Encryption (`@DeepPersonalData`)

```kotlin
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData

data class GiftCardIssuedEvent(
    @DeepPersonalData
    val owner: Person  // Person contains nested @PersonalData fields
)
```

Deep encryption recursively encrypts all `@PersonalData` fields within nested objects.

#### 3. Serialized Encryption (`@SerializedPersonalData`)

```kotlin
import io.axoniq.framework.extension.dataprotection.api.SerializedPersonalData
import java.time.LocalDate

data class GiftCardIssuedEvent(
    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    val dateOfBirth: LocalDate  // Clean syntax!
)
```

Serialized encryption converts the entire field to bytes before encryption, preserving complex types.

### Data Subject Identifier

```kotlin
import io.axoniq.framework.extension.dataprotection.sample.giftcard.GiftPersonalDataGroup
import io.axoniq.framework.extension.dataprotection.api.DataSubjectId
import org.axonframework.eventsourcing.annotation.EventTag

data class GiftCardIssuedEvent(
    @EventTag
    @DataSubjectId(
        group = GiftPersonalDataGroup.GROUP_NAME,
        prefix = GiftPersonalDataGroup.GROUP_PREFIX
    )
    val giftCardId: String  // No @field meta-annotation needed!
)
```

The `giftCardId` serves as the **data subject identifier**. All personal data in events with this ID can be cryptographically erased by deleting the corresponding encryption key.

### Complete Example

Here's a complete event showing all encryption types in clean Kotlin syntax:

```kotlin
data class GiftCardIssuedEvent(
    // Data subject identifier
    @EventTag
    @DataSubjectId(
        group = GiftPersonalDataGroup.GROUP_NAME,
        prefix = GiftPersonalDataGroup.GROUP_PREFIX
    )
    val giftCardId: String,

    // Regular field (not encrypted)
    val amount: BigDecimal,

    // Field-level encryption
    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    val username: String,

    // Deep encryption (nested object)
    @DeepPersonalData
    val owner: Person,

    // Serialized encryption
    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    val dateOfBirth: LocalDate,

    // Encrypted field pair (framework populates this)
    val dateOfBirthEncrypted: ByteArray?,

    // Serialized encryption for primitive
    @SerializedPersonalData(group = GiftPersonalDataGroup.GROUP_NAME, replacement = "<removed>")
    val randomNumber: Int,

    // Encrypted field pair (framework populates this)
    val randomNumberEncrypted: String?
)
```

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Kotlin | 2.1.0 |
| Java | Java | 21 |
| Framework | Spring Boot | 3.3.5 |
| CQRS/ES | Axon Framework | 5.0.0 |
| Data Protection | Axon Data Protection Extension | 5.0.0-SNAPSHOT |
| Database | PostgreSQL | Latest |
| Migration | Flyway | Latest |
| Build | Maven | 3.9+ |
| Kotlin Build | kotlin-maven-plugin | 2.1.0 |
| JSON | Jackson + jackson-module-kotlin | Latest |

## Project Structure

```
src/
├── main/
│   ├── kotlin/                                      # All Kotlin source files (100%)
│   │   └── io/axoniq/dataprotection/sample/
│   │       ├── DataProtectionKotlinSampleApplication.kt  # Main class
│   │       ├── config/                              # Configuration (Kotlin)
│   │       │   ├── DataProtectionConfiguration.kt   # Spring Config
│   │       │   ├── GiftPersonalDataGroup.kt         # Constants (object)
│   │       │   └── WebConfig.kt                     # Web Config
│   │       └── giftcard/
│   │           ├── command/                         # Command models (Kotlin)
│   │           │   ├── IssueGiftCardCommand.kt      # data class
│   │           │   └── RedeemGiftCardCommand.kt     # data class
│   │           ├── domain/                          # Aggregate (Kotlin)
│   │           │   └── GiftCard.kt                  # class
│   │           ├── event/                           # Event models (Kotlin)
│   │           │   ├── Address.kt                   # data class
│   │           │   ├── Person.kt                    # data class
│   │           │   ├── GiftCardIssuedEvent.kt       # data class
│   │           │   └── GiftCardRedeemedEvent.kt     # data class
│   │           ├── query/                           # Query models (Kotlin)
│   │           │   ├── FindGiftCardQuery.kt         # data class
│   │           │   ├── FindAllGiftCardsQuery.kt     # object
│   │           │   ├── GiftCardSummary.kt           # data class
│   │           │   ├── GiftCardSummaryList.kt       # data class
│   │           │   ├── GiftCardProjection.kt        # class
│   │           │   ├── GiftCardEntity.kt            # class (JPA)
│   │           │   └── GiftCardRepository.kt        # interface
│   │           └── web/                             # REST controllers (Kotlin)
│   │               ├── GiftCardController.kt        # class
│   │               └── DataProtectionController.kt  # class
│   └── resources/
│       ├── application.properties                   # Application config
│       ├── db/migration/                            # Flyway migrations
│       └── static/                                  # Web UI
└── test/
    └── kotlin/                                      # Kotlin tests

20 Kotlin files total - 100% Kotlin, 0 Java files!
```

## Kotlin-Specific Notes

### Data Classes vs Java Records

Kotlin data classes provide more features than Java records:
- **Immutability** by default (when using `val`)
- **Pattern matching** via destructuring
- **Copy method** for creating modified instances
- **Automatic `equals`, `hashCode`, `toString`**
- **Component functions** for destructuring (`component1()`, `component2()`, etc.)

```kotlin
// Immutable data class (like Java record)
data class IssueGiftCardCommand(
    val giftCardId: String,
    val amount: BigDecimal
)

// Usage
val cmd = IssueGiftCardCommand("123", BigDecimal("100"))
val modified = cmd.copy(amount = BigDecimal("200"))  // Easy copying!
val (id, amount) = cmd  // Destructuring!
```

### JPA Entities - Mutable Classes

JPA entities use regular classes with `var` properties (mutable):

```kotlin
@Entity
@Table(name = "gift_card")
class GiftCardEntity() {
    @Id
    var giftCardId: String? = null  // Mutable with var

    var remainingValue: BigDecimal? = null

    @PersonalData(group = GiftPersonalDataGroup.GROUP_NAME)
    var username: String? = null  // Nullable for JPA

    // Secondary constructor for initialization
    constructor(
        giftCardId: String,
        remainingValue: BigDecimal,
        username: String
    ) : this() {
        this.giftCardId = giftCardId
        this.remainingValue = remainingValue
        this.username = username
    }
}
```

**Why?** JPA requires:
- No-arg constructor (primary constructor)
- Mutable properties (`var` not `val`)
- Nullable types for lazy initialization

### Object Declarations for Singletons

Kotlin's `object` keyword creates singletons elegantly:

```kotlin
object GiftPersonalDataGroup {
    const val GROUP_NAME = "gift"
    const val GROUP_PREFIX = "gift-"
}

// Usage - no .INSTANCE needed!
@DataSubjectId(
    group = GiftPersonalDataGroup.GROUP_NAME,  // Direct access!
    prefix = GiftPersonalDataGroup.GROUP_PREFIX
)
```

**Java equivalent** would be verbose:
```java
public final class GiftPersonalDataGroup {
    private GiftPersonalDataGroup() {}
    public static final String GROUP_NAME = "gift";
    public static final String GROUP_PREFIX = "gift-";
}
```

### Named Parameters

Kotlin's named parameters make code self-documenting:

```kotlin
GiftCardIssuedEvent.create(
    giftCardId = uuid,
    amount = BigDecimal("100.00"),
    username = "john.doe",
    owner = Person(
        name = "John Doe",
        address = Address(
            line1 = "123 Main St",
            line2 = "Apt 4B",
            country = "USA"
        )
    ),
    dateOfBirth = LocalDate.of(1990, 1, 15),
    randomNumber = 42
)
```

### Extension Functions

Kotlin allows extending existing classes:

```kotlin
fun BigDecimal.isPositive(): Boolean = this > BigDecimal.ZERO

// Usage in validation
require(command.amount.isPositive()) {
    "Gift card amount must be positive"
}
```

### Null Safety

Kotlin's type system distinguishes nullable and non-nullable types:

```kotlin
// Non-nullable - guaranteed not null
val giftCardId: String = "123"

// Nullable - can be null
val username: String? = null

// Safe call operator
val length = username?.length  // Returns null if username is null

// Elvis operator
val displayName = username ?: "Unknown"  // Default value if null

// Not-null assertion (use sparingly!)
val upperName = username!!.uppercase()  // Throws if null
```

### Validation with `require()`

Kotlin's `require()` function is cleaner than Java's manual checks:

```kotlin
// Kotlin - concise and clear
require(command.amount > BigDecimal.ZERO) {
    "Gift card amount must be positive"
}

require(command.amount <= remainingValue) {
    "Insufficient funds"
}

// Java equivalent - verbose
if (command.amount().compareTo(BigDecimal.ZERO) <= 0) {
    throw new IllegalArgumentException("Gift card amount must be positive");
}
```

### No Need for `@BeanProperty`

Unlike Scala, Kotlin doesn't need `@BeanProperty`:

**Kotlin** (automatic):
```kotlin
data class IssueGiftCardCommand(
    val giftCardId: String,  // Automatically generates getGiftCardId()
    val amount: BigDecimal   // Automatically generates getAmount()
)
```

**Scala** (manual):
```scala
case class IssueGiftCardCommand(
  @BeanProperty giftCardId: String,  // Required for Jackson!
  @BeanProperty amount: BigDecimal
)
```

Kotlin data classes automatically generate Java-style getters for interoperability!

## Key Differences from Java/Scala Samples

| Aspect | Java Version | Scala Version | Kotlin Version |
|--------|-------------|---------------|----------------|
| **Commands/Events** | Java records | Scala case classes with `@BeanProperty` | **Kotlin data classes** |
| **Annotation Syntax** | Standard | Requires `@(Annotation @field)` | **Cleanest - no `@field` needed!** |
| **Domain Model** | Java class | Scala class | **Kotlin class** |
| **Projection** | Java class | Scala class | **Kotlin class** |
| **Queries** | Java records | Scala case classes | **Kotlin data classes** |
| **Controllers** | Java | Mixed Scala/Java | **100% Kotlin** |
| **Config** | Java | Java | **100% Kotlin** |
| **JPA Entities** | Java | Java | **100% Kotlin** |
| **Singletons** | Static class | `object` | **`object` (native support)** |
| **Immutability** | `final` fields | `val` | **`val` (default for data classes)** |
| **Null Safety** | `@Nullable` annotations | `Option[T]` | **Native `T?` syntax** |
| **Validation** | Manual `if` checks | Manual checks | **`require()` with lambdas** |
| **Java Interop** | Native | Requires converters | **Seamless (native)** |
| **File Count** | ~15 Java files | 15 Scala + 7 Java | **20 Kotlin files (0 Java!)** |

### Why Kotlin is the Best Choice for Axon + Data Protection

1. **Simplest Annotation Syntax**: No `@field` meta-annotations needed (unlike Scala)
2. **100% Pure Implementation**: No Java files needed (unlike Scala sample)
3. **Seamless Java Interop**: Works perfectly with Axon Framework (Java-based)
4. **Modern Language Features**: Data classes, null safety, extension functions
5. **Spring Boot Native Support**: First-class Kotlin support in Spring
6. **Clean and Concise**: Less boilerplate than Java, cleaner than Scala
7. **Industry Momentum**: Growing adoption in enterprise and Spring ecosystem

## Learning Resources

- [Axon Framework Documentation](https://docs.axoniq.io)
- [Axon Data Protection Extension Guide](https://docs.axoniq.io/extensions/data-protection)
- [Kotlin Documentation](https://kotlinlang.org/docs/home.html)
- [Spring Boot with Kotlin](https://spring.io/guides/tutorials/spring-boot-kotlin/)
- [Jackson Kotlin Module](https://github.com/FasterXML/jackson-module-kotlin)

## Related Samples

- `data-protection-spring-boot-sample` - Java version of this sample
- `data-protection-spring-boot-scala-sample` - Scala version of this sample
- `data-protection-core` - Core data protection library

## License

Copyright (c) 2010-2025 AxonIQ B.V.

Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS, Version September 2025.
