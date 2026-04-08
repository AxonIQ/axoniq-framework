# Scala Migration Guide

This document explains the Scala migration from the Java Spring Boot sample.

## 🎯 Migration Goals

1. ✅ Convert core domain logic to Scala
2. ✅ Use Scala case classes for Commands, Events, and Queries
3. ✅ Maintain compatibility with Axon Framework 5.x
4. ✅ Keep Java where necessary for framework integration
5. ✅ Ensure build works with mixed Java/Scala compilation

## 📦 What Was Converted to Scala (15 files)

### Domain Model & CQRS
```
src/main/scala/io/axoniq/dataprotection/sample/giftcard/
├── domain/
│   └── GiftCard.scala ........................... EventSourced aggregate
├── command/
│   ├── IssueGiftCardCommand.scala ............... Command case class
│   └── RedeemGiftCardCommand.scala .............. Command case class
└── event/
    ├── GiftCardIssuedEvent.scala ................ Event case class
    ├── GiftCardRedeemedEvent.scala .............. Event case class
    ├── Person.scala ............................. Nested encrypted data
    └── Address.scala ............................ Nested encrypted data
```

### Query Model
```
src/main/scala/io/axoniq/dataprotection/sample/giftcard/query/
├── GiftCardProjection.scala ..................... Event handler & query handler
├── GiftCardSummary.scala ........................ Read model DTO
├── GiftCardSummaryList.scala .................... Collection wrapper
├── FindGiftCardQuery.scala ...................... Query case class
└── FindAllGiftCardsQuery.scala .................. Query case class
```

### Controllers
```
src/main/scala/io/axoniq/dataprotection/sample/
├── giftcard/web/
│   └── DataProtectionController.scala ........... GDPR operations API
└── api/
    └── KeyManagementController.scala ............ Encryption key management
```

### Application
```
src/main/scala/io/axoniq/dataprotection/sample/
└── DataProtectionScalaSampleApplication.scala ... Main Spring Boot app
```

## ⚙️ What Remained in Java (7 files)

### JPA Persistence Layer
```
src/main/java/io/axoniq/dataprotection/sample/giftcard/query/
├── GiftCardEntity.java .......................... JPA entity (@Entity)
└── GiftCardRepository.java ...................... Spring Data JPA repository
```
**Why Java?** JPA annotations (`@Entity`, `@Table`, `@Column`) work best with Java classes. Scala case classes have issues with JPA proxying and lazy loading.

### Spring Configuration
```
src/main/java/io/axoniq/dataprotection/sample/config/
├── DataProtectionConfiguration.java ............. Spring @Configuration
├── GiftPersonalDataGroup.java ................... Constants for annotations
├── WebConfig.java ............................... CORS configuration
└── EventProcessorResetService.java .............. Axon processor management
```
**Why Java?** Spring `@Configuration` classes with complex `@Bean` methods work more reliably in Java. Also, Scala objects cannot be used as annotation parameters (compile-time constant requirement).

### Main Controller
```
src/main/java/io/axoniq/dataprotection/sample/giftcard/web/
└── GiftCardController.java ...................... REST API with SSE streams
```
**Why Java?** Complex Spring WebFlux Server-Sent Events (SSE) implementation with subscription queries. The Java version is well-tested and SSE with Reactor in Scala adds unnecessary complexity.

## 🔑 Key Scala Features Used

### 1. Case Classes for Immutability
```scala
case class IssueGiftCardCommand(
  @BeanProperty
  @TargetEntityId
  giftCardId: String,

  @BeanProperty
  amount: BigDecimal,

  @BeanProperty
  username: String
)
```

### 2. @BeanProperty for Java Interop
Required for Jackson serialization and Spring framework compatibility:
```scala
@BeanProperty  // Generates getGiftCardId() and setGiftCardId()
giftCardId: String
```

### 3. Scala Collections with Java Interop
```scala
import scala.jdk.CollectionConverters._

val scalaList: List[GiftCardSummary] = ...
val javaList: java.util.List[GiftCardSummary] = scalaList.asJava
```

### 4. Java Predicate for Lambda Compatibility
```scala
import java.util.function.Predicate

queryUpdateEmitter.emit(
  classOf[FindGiftCardQuery],
  new Predicate[FindGiftCardQuery] {
    override def test(query: FindGiftCardQuery): Boolean =
      query.giftCardId == event.giftCardId
  },
  giftCard
)
```

### 5. Future/CompletableFuture Conversion
```scala
import scala.jdk.FutureConverters._

val futureResult: Future[ResponseEntity[String]] = Future { ... }
futureResult.asJava.toCompletableFuture
```

## 🚧 Challenges & Solutions

### Challenge 1: Annotation Parameters Must Be Constants
**Problem:** Scala cannot use Java static final fields as annotation parameters.

```scala
// ❌ Does NOT compile
@DataSubjectId(group = GiftPersonalDataGroup.GROUP_NAME)

// ✅ Works - hardcoded values
@DataSubjectId(group = "gift")
```

**Solution:** Hardcode annotation string values directly.

### Challenge 2: Mixed Compilation Order
**Problem:** Java code needs Scala classes (queries, commands) and vice versa.

**Solution:** Configure Maven to compile Scala first, then Java:
```xml
<execution>
    <id>scala-compile-first</id>
    <phase>process-resources</phase>
    <goals>
        <goal>compile</goal>
    </goals>
</execution>
```

### Challenge 3: JPA Entity Compatibility
**Problem:** Scala case classes don't work well as JPA entities.

**Solution:** Keep JPA entities in Java. Scala classes can use them via repositories.

### Challenge 4: Spring Configuration Complexity
**Problem:** Spring `@Configuration` with `@Bean` methods and complex initialization.

**Solution:** Keep configuration classes in Java. Scala code can inject these beans.

## 📊 Conversion Statistics

| Category | Files | Lines of Code | Language |
|----------|-------|---------------|----------|
| **Domain & CQRS** | 7 | ~400 | Scala ✅ |
| **Query Model** | 5 | ~350 | Scala ✅ |
| **Controllers** | 2 | ~150 | Scala ✅ |
| **Application** | 1 | ~50 | Scala ✅ |
| **JPA Layer** | 2 | ~200 | Java ⚙️ |
| **Configuration** | 4 | ~400 | Java ⚙️ |
| **Main Controller** | 1 | ~350 | Java ⚙️ |
| **TOTAL** | **22** | **~1900** | **68% Scala** |

## 🎓 Lessons Learned

1. **Scala Shines in Domain Logic**: Case classes for commands, events, and DTOs are concise and type-safe
2. **Java Still Needed for Framework Integration**: JPA, Spring Configuration, complex WebFlux
3. **Mixed Compilation Works Well**: Maven `scala-maven-plugin` handles it smoothly
4. **@BeanProperty is Essential**: Required for Jackson and Spring compatibility
5. **Annotation Limitations**: Scala cannot use dynamic values in annotations

## 🏗️ Build Configuration

### Maven Plugin Setup
```xml
<plugin>
    <groupId>net.alchim31.maven</groupId>
    <artifactId>scala-maven-plugin</artifactId>
    <version>4.9.2</version>
    <executions>
        <execution>
            <id>scala-compile-first</id>
            <phase>process-resources</phase>
            <goals>
                <goal>add-source</goal>
                <goal>compile</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

### Dependencies
```xml
<dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-library</artifactId>
    <version>2.13.15</version>
</dependency>

<dependency>
    <groupId>com.fasterxml.jackson.module</groupId>
    <artifactId>jackson-module-scala_2.13</artifactId>
</dependency>
```

## ✅ Verification

Build the project:
```bash
mvn clean package -pl examples/data-protection-spring-boot-scala-sample -am
```

Expected output:
```
[INFO] BUILD SUCCESS
```

Run the application:
```bash
cd examples/data-protection-spring-boot-scala-sample
docker-compose up -d
mvn spring-boot:run
```

Access at: `http://localhost:8080`

## 🔗 Related Documentation

- [README.md](README.md) - Full sample documentation
- [Axon Framework Scala Guide](https://docs.axoniq.io)
- [Jackson Scala Module](https://github.com/FasterXML/jackson-module-scala)
- [Scala Maven Plugin](https://github.com/davidB/scala-maven-plugin)

---

**Created:** 2025-12-12
**Axon Framework Version:** 5.0.0
**Scala Version:** 2.13.15
**Spring Boot Version:** 3.3.5
