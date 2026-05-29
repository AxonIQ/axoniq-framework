# Demo Plan: University Message Transformation

**Companion to**: [spec.md](spec.md), [plan.md](plan.md), [tasks.md](tasks.md)
**Demo target repo**: `AxonFramework` (cross-repo from this `axoniq-framework` branch)
**Demo module path**: `AxonFramework/examples/university-message-transformation/`
**Last revised**: 2026-05-29

---

## 0. Goal & success criteria

Deliver a runnable, fully tested AF5 example application in the `AxonFramework` repo that
demonstrates every feature delivered by `axoniq-message-transformation` on this branch.
The demo must read as production-shape code, not a toy: vertical slices, DDD building
blocks, CQRS, event sourcing, DCB, proper invariant enforcement, idempotent seeding.

**Success criteria**:

- Every delivered FR (14 of them, listed in section 1) demonstrated visibly in
  `main()` console output AND verified by JUnit.
- Architecture passes the "would a senior engineer ship this?" bar.
- A reader can copy the structure verbatim to start their own bounded context with
  versioned events.
- Builds cold (`./mvnw -Pexamples clean verify`) with `axon.server.enabled=false`
  default. Same command builds against a running Axon Server when toggled.
- Idempotent: re-running `main()` against persistent Axon Server keeps the catalog
  stable (no duplicate seed events).

---

## 1. Scope

### In scope (5.2.0 MUST + SHOULD delivered on this branch)

| FR     | What the demo proves                                              |
|--------|-------------------------------------------------------------------|
| FR-001 | 1:1 structural transform with payload mapper                      |
| FR-004 | Programmatic registration; chain locks at `.build()`              |
| FR-005 | Concrete `from(MessageType)` AND `from(Predicate<MessageType>)`   |
| FR-006 | Determinism + thread-safety contract                              |
| FR-007 | Multi-hop fixed-point iteration (v1 -> v2 -> v3)                  |
| FR-009 | Both `Class<T>` and `TypeReference<T>` overloads                  |
| FR-010 | Envelope preservation (token, sequence, tags)                     |
| FR-011 | Lazy deserialization on the non-matching path                     |
| FR-012 | Read-context consistency (entity-load / DCB / tracking processor) |
| FR-013 | Chain-build DEBUG log entry                                       |
| FR-016 | Unversioned legacy events treated as `0.0.1`                      |
| FR-017 | Plain-JUnit transformation testability                            |
| FR-018 | Output-identity check                                             |
| FR-021 | Decoration-ordering invariant                                     |

### Out of scope (user-confirmed off-table)

- US2 rename ("not supported yet" per user)
- US3 split / US4 drop / US5 multi-hop hardening / US6 conflict detection beyond
  FR-018 / US7 per-transformer hooks
- US8 commands / US9 queries / snapshots
- Spring Boot variant (follow-on)
- Antora reference-guide pages (T034) -- deferred to follow-up
- AF4 -> AF5 migration cookbook -- skipped per user decision

---

## 2. Domain story & bounded context

**Bounded context**: `CourseCatalog` -- a university's published-course catalog with
student enrolments and operational announcements.

### Ubiquitous language

| Term            | Meaning                                                                |
|-----------------|------------------------------------------------------------------------|
| Course          | A published offering of study. Identified by `CourseId`.               |
| Capacity range  | Min/max enrolment a course accepts. Value object: `CapacityRange`.     |
| Student         | A person registered with the catalog. Identified by `StudentId`.       |
| Enrolment       | The fact a student is bound to a course.                               |
| Announcement    | A free-text operational notice broadcast to subscribers.               |

### Three years of evolution (the reason the chain exists)

| When            | Schema change                                                | Transform                                  |
|-----------------|--------------------------------------------------------------|--------------------------------------------|
| Y1              | `CoursePublished` had `capacity: int`                        | v1 -> v2: split into `minCapacity`/`maxCapacity` |
| Y2              | Overbooking incident; capacity became a band                 | v2 -> v3: wrap into `CapacityRange` value object |
| Y3              | Privacy review collapses `firstName`+`lastName` to `fullName`| v1 -> v2 on `StudentRegistered`            |
| Beta            | Welcome messages had no version                              | predicate match on `0.x` -> v1             |
| Pre-versioning  | A `SystemAnnouncement` slipped through without a revision    | `0.0.1` -> v1 uplift                       |

Today the application code is built against the year-3 model. It does not care that the
event store is full of older shapes. Five transformations bridge the gap.

---

## 3. Architecture -- real business-application patterns

| AF5 standard / DDD principle           | How the demo applies it                                                                                  |
|----------------------------------------|----------------------------------------------------------------------------------------------------------|
| Vertical slices / Screaming Architecture | `coursecatalog/<events|transformations|write|read|automation|seed>` -- each folder is a business capability, not a layer |
| CQRS                                   | `write/` produces events; `read/` projects them; queries served by dedicated `QueryHandlingModule`       |
| Event sourcing                         | Aggregate state reconstructed from events on every command (no row state)                                |
| DCB (Dynamic Consistency Boundary)     | `@EventCriteriaBuilder` on each `State` declares multi-tag scope; no fixed aggregate root                |
| DDD building blocks                    | Entities (`State`), Value Objects (`CapacityRange`), Domain Events (`events/`), Repositories             |
| Invariant enforcement                  | Pure `assertXxx(state)` methods inside command handlers (mirrors `SubscribeStudentToCourseCommandHandler`) |
| Idempotency                            | Marker event guards `LegacyEventSeeder`; commands tolerate "already-done" by returning empty event lists |
| Anti-Corruption Layer                  | The transformation chain IS the ACL between historic on-disk shapes and current domain model             |
| Process manager / automation           | `OverbookingNotifier` reacts to `CoursePublished` v3 and emits announcements                             |
| Composition over inheritance           | Each transformation is a standalone class with `static EventTransformer build()`; no transformer base    |
| Hexagonal-style I/O isolation          | Tracking projection writes to a `CatalogViewRepository` interface; in-memory adapter ships               |
| Declarative > annotation-heavy         | Programmatic chain composition; annotations only where AF5 expects them                                  |

---

## 4. Module layout

```
AxonFramework/examples/university-message-transformation/                    NEW
|-- pom.xml
|-- README.md
|-- docker-compose.yaml
|-- docs/images/CourseCatalog_VersionEvolution.png
`-- src/
    |-- main/
    |   |-- resources/
    |   |   |-- application.properties                                       axon.server.enabled=false
    |   |   `-- logback.xml
    |   `-- java/org/axonframework/examples/demo/coursecatalog/
    |       |-- package-info.java                                            @NullMarked
    |       |-- CourseCatalogApplication.java                                main() + .configurer(...)
    |       |-- ConfigurationProperties.java
    |       |
    |       |-- catalog/                                                     feature package
    |       |   |-- package-info.java
    |       |   |-- CourseCatalogModuleConfiguration.java                    aggregates per-slice configs
    |       |   |-- CourseCatalogMessageNames.java                           name + revision constants
    |       |   |-- CourseCatalogTags.java                                   tag-key constants
    |       |   |-- Ids.java
    |       |   |
    |       |   |-- events/                                                  current shape only
    |       |   |   |-- CoursePublished.java                                 v3 - CapacityRange (+ @EventTag)
    |       |   |   |-- CourseCapacityChanged.java                           v1               (+ @EventTag)
    |       |   |   |-- StudentRegistered.java                               v2 - fullName    (+ @EventTag)
    |       |   |   |-- StudentEnrolledInCourse.java                         v1               (+ @EventTag)
    |       |   |   |-- RegistrationClosed.java                              v1 - pass-through anchor (+ @EventTag)
    |       |   |   |-- SystemAnnouncement.java                              v1 - lifted from 0.0.1 (+ @EventTag)
    |       |   |   |-- WelcomeMessageSent.java                              v1 - lifted from 0.x   (+ @EventTag)
    |       |   |   `-- CatalogSeeded.java                                   marker event (+ @EventTag)
    |       |   |
    |       |   |-- values/                                                  value objects
    |       |   |   `-- CapacityRange.java                                   record + invariant in canonical constructor
    |       |   |
    |       |   |-- transformations/                                         one class per transformation
    |       |   |   |-- CoursePublishedV1ToV2.java                           Class<JsonNode>
    |       |   |   |-- CoursePublishedV2ToV3.java
    |       |   |   |-- StudentRegisteredV1ToV2.java                         TypeReference<Map<String,Object>>
    |       |   |   |-- SystemAnnouncementLegacyUplift.java                  from((name, "0.0.1"))
    |       |   |   |-- WelcomeMessageBetaCleanup.java                       from(Predicate<MessageType>)
    |       |   |   `-- CourseCatalogTransformations.java                    chain composition site
    |       |   |
    |       |   |-- write/
    |       |   |   |-- publishcourse/                                       single-tag DCB
    |       |   |   |   |-- PublishCourse.java                               command
    |       |   |   |   |-- PublishCourseCommandHandler.java                 @CommandHandler + inner State
    |       |   |   |   `-- PublishCourseConfiguration.java
    |       |   |   |-- updatecoursecapacity/                                reads v1/v2 lifted via chain
    |       |   |   |   |-- UpdateCourseCapacity.java
    |       |   |   |   |-- UpdateCourseCapacityCommandHandler.java
    |       |   |   |   `-- UpdateCourseCapacityConfiguration.java
    |       |   |   `-- enrollstudent/                                       multi-tag DCB
    |       |   |       |-- EnrollStudent.java
    |       |   |       |-- EnrollStudentCommandHandler.java                 @EventCriteriaBuilder composite
    |       |   |       |-- EnrolmentId.java                                  composite id record
    |       |   |       `-- EnrollStudentConfiguration.java
    |       |   |
    |       |   |-- read/
    |       |   |   `-- catalogview/
    |       |   |       |-- CatalogViewReadModel.java                       record per catalog row
    |       |   |       |-- CourseCatalogView.java                          query result aggregate
    |       |   |       |-- CatalogViewRepository.java                      interface
    |       |   |       |-- InMemoryCatalogViewRepository.java              adapter
    |       |   |       |-- CatalogViewProjection.java                      @EventHandler methods
    |       |   |       |-- GetCourseCatalogView.java                       query
    |       |   |       |-- GetCourseCatalogViewQueryHandler.java
    |       |   |       `-- CatalogViewConfiguration.java                    PooledStreamingEventProcessorModule + QueryHandlingModule
    |       |   |
    |       |   |-- automation/
    |       |   |   `-- overbookingnotifier/                                process manager
    |       |   |       |-- OverbookingNotifier.java                        @EventHandler reacts to v3 CoursePublished
    |       |   |       `-- OverbookingNotifierConfiguration.java
    |       |   |
    |       |   `-- seed/
    |       |       |-- LegacyEventSeeder.java                              idempotent via marker event
    |       |       `-- LegacyEventSeedConfiguration.java                   lifecycle hook
    |       |
    |       `-- shared/
    |           `-- ids/
    |               |-- CatalogId.java
    |               |-- CourseId.java
    |               `-- StudentId.java
    |
    `-- test/
        |-- resources/
        |   |-- logback-test.xml                                            DEBUG for chain-build log capture
        |   `-- transformations/                                            golden JSON files
        |       |-- coursepublished/{v1,v2,v3}.json
        |       |-- studentregistered/{v1,v2}.json
        |       |-- systemannouncement/{unversioned,v1}.json
        |       `-- welcomemessagesent/{v0_5,v0_7,v0_9,v1}.json
        `-- java/org/axonframework/examples/demo/coursecatalog/
            |-- CourseCatalogApplicationTest.java                            abstract base
            |-- MainSmokeTest.java                                           concrete - runs main()
            `-- catalog/
                |-- CourseCatalogAxonTestFixture.java                        per-slice AxonTestFixtureProvider classes
                |-- testing/                                                 holixon-inspired helpers
                |   |-- TransformationTester.java
                |   |-- ChainTester.java
                |   `-- JsonAssertions.java
                |-- transformations/                                         L1 unit tests
                |   |-- CoursePublishedV1ToV2Test.java
                |   |-- CoursePublishedV2ToV3Test.java
                |   |-- StudentRegisteredV1ToV2Test.java
                |   |-- SystemAnnouncementLegacyUpliftTest.java
                |   `-- WelcomeMessageBetaCleanupTest.java
                |-- chain/                                                   L2 tests
                |   |-- MultiHopChainTesterTest.java
                |   |-- ChainLockingTest.java
                |   |-- ChainConcurrencyTest.java
                |   |-- OutputIdentityCheckTest.java
                |   |-- DecorationOrderTest.java
                |   `-- ChainBuildLogTest.java
                |-- write/                                                   per-slice BDD fixtures
                |   |-- publishcourse/PublishCourseAxonFixtureTest.java
                |   |-- updatecoursecapacity/UpdateCourseCapacityAxonFixtureTest.java
                |   `-- enrollstudent/EnrollStudentAxonFixtureTest.java
                |-- read/catalogview/CatalogViewProjectionAxonFixtureTest.java
                |-- automation/overbookingnotifier/OverbookingNotifierAxonFixtureTest.java
                `-- chain_integration/
                    |-- NonMatchingPassThroughAxonFixtureTest.java
                    |-- PredicateMatchingAxonFixtureTest.java
                    |-- LegacyUnversionedEventAxonFixtureTest.java
                    |-- ReadContextConsistencyAxonFixtureTest.java
                    `-- EnvelopePreservationAxonFixtureTest.java
```

---

## 5. Slice anatomies

### 5.1 Write slice -- `updatecoursecapacity` (the headline DCB + chain interaction)

```java
record UpdateCourseCapacity(CourseId courseId, CapacityRange newRange) {}

class UpdateCourseCapacityCommandHandler {
    @CommandHandler
    void handle(UpdateCourseCapacity cmd, @InjectEntity State state, EventAppender appender) {
        appender.append(decide(cmd, state));
    }

    private List<? extends Event> decide(UpdateCourseCapacity cmd, State state) {
        assertCoursePublished(state);
        assertNewRangeContainsCurrentEnrolments(state, cmd);
        if (state.range.equals(cmd.newRange())) return List.of();   // idempotency
        return List.of(new CourseCapacityChanged(cmd.courseId(), cmd.newRange()));
    }

    @EventSourcedEntity(tagKey = CourseCatalogTags.COURSE_ID)
    static class State {
        private boolean published = false;
        private CapacityRange range;
        private int currentEnrolments = 0;

        @EntityCreator State() {}

        @EventSourcingHandler void evolve(CoursePublished e)         { published = true; range = e.range(); }
        @EventSourcingHandler void evolve(CourseCapacityChanged e)   { range = e.newRange(); }
        @EventSourcingHandler void evolve(StudentEnrolledInCourse e) { currentEnrolments++; }
    }
}
```

`evolve(CoursePublished e)` receives v3 instances even when the disk holds v1 -- the chain
lifts the JSON, the converter binds it to the `CoursePublished` record, the state evolves.
That is the entire ACL story in one method.

### 5.2 Write slice -- `enrollstudent` (multi-tag DCB)

Same shape as `SubscribeStudentToCourseCommandHandler` in `university-demo`: composite
`EnrolmentId(courseId, studentId)`, `@EventCriteriaBuilder` returning `EventCriteria.either(...)`
of two tag-scoped queries. Demonstrates multi-stream DCB reads flow through the chain.

### 5.3 Read slice -- `catalogview`

`CatalogViewProjection` annotated with `@EventHandler` methods, one per current-shape
event. Reads through a `PooledStreamingEventProcessorModule`. The projector only knows
about v3 / current shapes -- the chain handles all uplift transparently.

`GetCourseCatalogView` query + handler returns the full read model.

### 5.4 Automation slice -- `overbookingnotifier`

`@EventHandler void on(CoursePublished published)` -- when published capacity is unusually
wide (e.g. `range.max - range.min > 20`), emit a `SystemAnnouncement` via `EventAppender`
warning the admins.

Proves automations see chain-transformed v3 events even when the original was a v1 disk
event from years ago.

### 5.5 Value object -- `CapacityRange`

```java
public record CapacityRange(int min, int max) {
    public CapacityRange {
        if (min < 0 || max < min) throw new IllegalArgumentException(...);
    }
}
```

Domain invariants enforced in the canonical constructor. Standard DDD value-object practice.

### 5.6 Transformation -- example

```java
public final class CoursePublishedV1ToV2 {
    private CoursePublishedV1ToV2() {}
    public static EventTransformer build() {
        return EventTransformation
            .from(new MessageType(COURSE_PUBLISHED_NAME, "1.0.0"))
            .to(  new MessageType(COURSE_PUBLISHED_NAME, "2.0.0"))
            .transform(JsonNode.class, CoursePublishedV1ToV2::map);
    }
    private static JsonNode map(JsonNode v1, @Nullable ProcessingContext ctx) {
        int cap = v1.get("capacity").asInt();
        ObjectNode v2 = JsonNodeFactory.instance.objectNode();
        v2.set("courseId", v1.get("courseId"));
        v2.set("name",     v1.get("name"));
        v2.put("minCapacity", cap);
        v2.put("maxCapacity", cap);
        return v2;
    }
}
```

### 5.7 Chain composition site

```java
public final class CourseCatalogTransformations {
    private CourseCatalogTransformations() {}
    public static EventTransformerChain chain() {
        return EventTransformerChain.builder()
            .register(SystemAnnouncementLegacyUplift.build())   // 0.0.1 -> 1.0.0
            .register(CoursePublishedV1ToV2.build())            // v1 -> v2
            .register(CoursePublishedV2ToV3.build())            // v2 -> v3 (chains)
            .register(StudentRegisteredV1ToV2.build())          // TypeReference<Map>
            .register(WelcomeMessageBetaCleanup.build())        // predicate-from semver 0.x
            .build();
    }
}
```

No inline mappers in the registration site. Every transformation is a standalone class
that a user can copy.

---

## 6. Configuration aggregator

`CourseCatalogModuleConfiguration.configure(...)` follows `FacultyModuleConfiguration`'s
shape exactly:

```java
public static EventSourcingConfigurer configure(EventSourcingConfigurer configurer) {
    // Cross-cutting
    configurer = CourseCatalogTransformations.register(configurer);   // register the chain

    // Write
    configurer = PublishCourseConfiguration.configure(configurer);
    configurer = UpdateCourseCapacityConfiguration.configure(configurer);
    configurer = EnrollStudentConfiguration.configure(configurer);

    // Read
    configurer = CatalogViewConfiguration.configure(configurer);

    // Automation
    configurer = OverbookingNotifierConfiguration.configure(configurer);

    // Seed (idempotent, lifecycle-hooked)
    configurer = LegacyEventSeedConfiguration.configure(configurer);

    return configurer;
}
```

---

## 7. Historic event seeding (idempotent)

Uses the delivered `GenericEventMessage(MessageType, payload)` constructor. No historic
Java record classes -- the seeder builds `JsonNode` payloads and appends them with
explicit `MessageType`.

**Idempotency guard**: a `CatalogSeeded` marker event written at the end of seeding.
Subsequent runs see the marker and skip.

**Tag attachment**: the seeder needs to attach tags explicitly because `@EventTag`
annotations only fire for typed events. Mechanism to be confirmed in pre-flight (see
section 12).

### Seeded events (18 total)

| Type                    | Versions                | Count       | Demonstrates                          |
|-------------------------|-------------------------|-------------|---------------------------------------|
| CoursePublished         | 1.0.0, 2.0.0, 3.0.0     | 3 + 2 + 2   | Multi-hop chain + pass-through        |
| StudentRegistered       | 1.0.0                   | 4           | TypeReference overload                |
| WelcomeMessageSent      | 0.5, 0.7, 0.9           | 1 each      | Predicate-based from match            |
| SystemAnnouncement      | unversioned             | 1           | FR-016 legacy default to 0.0.1        |
| RegistrationClosed      | 1.0.0                   | 3           | Non-matching pass-through             |

---

## 8. Three read contexts in `main()`

```
 1. configurer.start()
    -> DEBUG line names all 5 transformers                             (FR-013)
 2. LegacyEventSeeder runs idempotently via start lifecycle hook       (idempotent)
 3. Await CourseCatalogProjection catch-up (Awaitility)
 4. Print CourseCatalogView -- every row in v3 / current shape         (FR-012 path A: tracking processor)
 5. Dispatch UpdateCourseCapacity on a v1-seeded course
    -> aggregate state replay via transaction().source()               (FR-012 path B: entity-load)
 6. Dispatch EnrollStudent with course + student ids
    -> DCB SourcingCondition read by tag                               (FR-012 path C: DCB)
 7. Reprint CourseCatalogView -- live commands on top of historic-lifted state
 8. configuration.shutdown()
```

Same v3 shape, three different read paths, visible to the eye in console output.

---

## 9. Testing strategy -- four layers

| Layer | Purpose                                         | Style                              | Examples                                                                 |
|-------|-------------------------------------------------|------------------------------------|--------------------------------------------------------------------------|
| L1    | Each transformation in isolation                | Plain JUnit, golden JSON, no AF wiring | `CoursePublishedV1ToV2Test` via `TransformationTester`                |
| L2    | Chain assembly without an event store           | `ChainTester` golden files         | `MultiHopChainTesterTest`, `ChainLockingTest`, `OutputIdentityCheckTest` |
| L3    | Per-slice via AxonTestFixture                   | given/when/then BDD                | `UpdateCourseCapacityAxonFixtureTest`, `CatalogViewProjectionAxonFixtureTest` |
| L4    | End-to-end smoke                                | abstract base + concrete `MainSmokeTest` | `CourseCatalogApplicationTest`                                     |

### holixon-inspired test helpers (in demo test sources)

```java
TransformationTester.forTransformation(CoursePublishedV1ToV2.build())
    .usingConverter(JacksonConverter.defaultJackson())
    .given().messageType(COURSE_PUBLISHED_NAME, "1.0.0")
            .payloadFromResource("/transformations/coursepublished/v1.json")
    .whenTransformed()
    .then().messageType(COURSE_PUBLISHED_NAME, "2.0.0")
            .payloadEqualsResource("/transformations/coursepublished/v2.json");
```

`ChainTester` adds `.andIntermediateHops(...)` for proving fixed-point iteration to a
reader. Helpers live in demo test sources so they double as teaching material; a
follow-up issue tracks lifting them into a public test-jar.

### Per-slice test scenarios

Each `*AxonFixtureTest` covers a fixed scenario matrix.

**`UpdateCourseCapacityAxonFixtureTest`**:

| Scenario                       | Given                                              | When                                  | Then                       |
|--------------------------------|----------------------------------------------------|---------------------------------------|----------------------------|
| Happy path on v3-seeded course | v3 `CoursePublished`                               | `UpdateCourseCapacity` with new range | `CourseCapacityChanged`    |
| Chain integration              | v1 `CoursePublished` (single capacity)             | `UpdateCourseCapacity` with new range | `CourseCapacityChanged`    |
| Idempotency                    | v3 `CoursePublished` (2,5)                          | `UpdateCourseCapacity` to (2,5)        | empty event list           |
| Invariant: course must exist   | nothing                                            | `UpdateCourseCapacity`                | exception                  |
| Invariant: range contains enrolments | `CoursePublished(2,5)` + 4 `StudentEnrolledInCourse` | `UpdateCourseCapacity` to (1,3)       | exception                  |
| DCB scope: other course events ignored | course A published + course B enrolments | `UpdateCourseCapacity` on course A    | invariant unaffected by B  |

Equivalent matrices spelled out per slice for `PublishCourseAxonFixtureTest`,
`EnrollStudentAxonFixtureTest`, `CatalogViewProjectionAxonFixtureTest`, and
`OverbookingNotifierAxonFixtureTest` during implementation.

### Test conventions (per `AxonFramework/CLAUDE.md`)

- JUnit 5; AssertJ over Hamcrest; Awaitility for async
- `// given /`, `// when /`, `// then` markers (spaces required)
- `@Nested` for logical grouping
- No `@DisplayName` -- method names self-explanatory
- No mocks -- `MessagesRecordingConfigurationEnhancer` + `RecordingComponentsRegistry`
  from `axon-test`; `Mockito.spy()` only when behaviour-based assertion is impossible
- Test class names describe behaviour, never spec IDs
- One concept per test; tests independent of each other

---

## 10. FR coverage matrix

| FR              | Visible in `main()`              | Verified by                                                                  |
|-----------------|----------------------------------|------------------------------------------------------------------------------|
| FR-001          | every CoursePublished hop        | `CoursePublishedV1ToV2Test` (L1)                                             |
| FR-004          | n/a                              | `ChainLockingTest`                                                           |
| FR-005 concrete | every transform                  | per-transformation tests + slice tests                                       |
| FR-005 predicate| 3 WelcomeMessageSent rows        | `WelcomeMessageBetaCleanupTest`, `PredicateMatchingAxonFixtureTest`          |
| FR-006          | n/a                              | `ChainConcurrencyTest`                                                       |
| FR-007 multi-hop| v1 -> v3 in catalog view         | `MultiHopChainTesterTest`, `UpdateCourseCapacityAxonFixtureTest`             |
| FR-009 Class<T> | 4 of 5 transformations           | per-transformation tests                                                     |
| FR-009 TypeRef  | StudentRegistered row in view    | `StudentRegisteredV1ToV2Test`                                                |
| FR-010          | aggregate sequence numbers       | `EnvelopePreservationAxonFixtureTest`                                        |
| FR-011          | RegistrationClosed rows          | `NonMatchingPassThroughAxonFixtureTest`                                      |
| FR-012          | sections 4, 5, 6 of main         | `ReadContextConsistencyAxonFixtureTest`, slice tests                         |
| FR-013          | step 1 DEBUG line                | `ChainBuildLogTest`                                                          |
| FR-016          | SystemAnnouncement row in view   | `SystemAnnouncementLegacyUpliftTest`, `LegacyUnversionedEventAxonFixtureTest`|
| FR-017          | n/a                              | every L1 test runs with only a `Converter`                                   |
| FR-018          | n/a                              | `OutputIdentityCheckTest`                                                    |
| FR-021          | n/a                              | `DecorationOrderTest`                                                        |

The Axon Server dashboard at http://localhost:8024 visually proves append-only +
read-time-only across all 18 seeded events.

---

## 11. Configuration & infrastructure

### `application.properties`

```properties
axon.server.enabled=false   # CI-portable; flip to true for Axon Server + UI
```

### `docker-compose.yaml`

Verbatim copy of `university-demo`'s -- `axoniq/axonserver:2025.2.7`, ports 8024 (UI)
+ 8124 (gRPC), `STANDALONE_DCB=true`, `DEVMODE_ENABLED=true`.

### `logback.xml`

Based on `university-java`'s template, with:

```xml
<logger name="io.axoniq.framework.messaging.transformation" level="DEBUG"/>
<logger name="io.grpc.netty" level="INFO"/>
<logger name="org.axonframework.axonserver.connector" level="INFO"/>
<logger name="io.axoniq.axonserver.connector" level="INFO"/>
```

### `logback-test.xml`

Required for the `ChainBuildLogTest` to capture the DEBUG line via a `ListAppender`.
Same logger levels as production logback, with the chain logger pinned to DEBUG.

### `pom.xml`

Mirrors `university-demo`'s dependency set + adds:

```xml
<dependency>
  <groupId>io.axoniq.framework</groupId>
  <artifactId>axoniq-message-transformation</artifactId>
</dependency>
```

Test scope: `axon-test`, `axoniq-testcontainer`, JUnit 5, AssertJ, Awaitility,
`log4j-slf4j2-impl`.

### Configuration enhancer wiring

`CourseCatalogModuleConfiguration` registers `EventTransformerChain` as a component;
`EventTransformationConfigurationEnhancer` (ServiceLoader-discovered) auto-installs
`TransformingEventStore`.

---

## 12. Pre-flight verifications (run before T-0)

| # | Check                                                                                  | Plan-B if negative                                       |
|---|----------------------------------------------------------------------------------------|----------------------------------------------------------|
| 1 | `axoniq-message-transformation` declared in `axoniq-framework-bom/pom.xml`              | Add it (covers tasks.md T002)                            |
| 2 | `@Event(name, revision)` annotation exists in AF5                                       | Register custom `CourseCatalogMessageTypeResolver`       |
| 3 | Default Jackson `MessageConverter` auto-registration                                    | Register one explicitly in module configuration         |
| 4 | `EventAppender` / `TagResolver` API for appending raw `JsonNode` with explicit tags     | Use typed historic record classes with `@EventTag`       |
| 5 | `MessageStream.flatMap` not required for 1:1 (re-verify by reading `EventTransformerChain.transformOne`) | n/a -- 5.2.0 ships only 1:1                |

If any item comes back negative, the plan adjusts before coding starts.

---

## 13. Code-conventions checklist

Every shipped file passes:

- Apache 2.0 license header (matches existing `AxonFramework` files)
- ASCII only (no curly quotes, em-dashes, ellipsis) -- per `AxonFramework/CLAUDE.md`
- LF line endings, UTF-8, 4-space indent, 120-char line limit
- `package-info.java` with `@org.jspecify.annotations.NullMarked` per package
- `@Nullable` only at documented boundaries (`ProcessingContext`)
- Fragment-style Javadoc (`@param` / `@return` lowercase, no period)
- `{@link}` for all type/method references; `@since 5.2.0`; `@author` on every public class
- No FR-XXX / US-XXX / T0XX / phase / "5.3+" / "deferred" references in Javadoc, test
  class names, or filenames -- per `.claude/rules/javadoc-no-spec-references.md`
- `@Internal` annotation on extracted helpers
- No wildcard imports (threshold 99)
- Records for immutable data; sealed types where polymorphism is closed
- No `CompletableFuture.join()` / `.get()` without timeout -- per
  `.claude/rules/completablefuture-blocking.md`
- `TypeReference` (not raw `Class`) for any generic `Type`-taking API -- per
  `.claude/rules/type-safety.md`
- `Objects.requireNonNull(...)` on every public method's reference parameters

---

## 14. Documentation

### `README.md` sections

1. Story (one paragraph -- three years of evolution).
2. Run in-memory (`-Daxon.server.enabled=false`; default).
3. Run with Axon Server (`docker compose up`) + dashboard at http://localhost:8024.
4. What the dashboard proves -- stored versions on disk vs what the app observes
   (visual proof of append-only + read-time-only).
5. The five transformations -- one-paragraph each linking to source file.
6. Cookbook -- "Add your own transformation in 6 steps".
7. Testing your transformation -- `TransformationTester` walkthrough with one full example.
8. Architecture map -- pointer to bounded-context + DCB + CQRS + ACL choices.
9. Out-of-scope list -- rename, split, drop, commands, queries, snapshots.
10. Cross-repo links -- spec, issue, BOM artifact.

### Cross-references with existing AF5 reference guide

README links to:

- `messaging-concepts/pages/anatomy-message.adoc` -- MessageType / version semantics
- `queries/pages/query-handlers.adoc` -- read-side query handler pattern
- `testing/pages/basic-testing.adoc` -- `AxonTestFixture` BDD
- `messaging-concepts/pages/processing-context.adoc` -- `ProcessingContext` in transformations

Dedicated `docs/reference-guide/modules/message-transformation/` pages are deferred to a
follow-up issue per user decision.

---

## 15. Implementation order

Each row is a coherent commit; the codebase compiles and tests green at every step.

| #  | Group                                                                    | Commit subject                                                       |
|----|--------------------------------------------------------------------------|----------------------------------------------------------------------|
| 1  | Pre-flight: BOM check + AF5 annotation greps + `MessageStream.flatMap`   | `137: pre-flight checks for university-message-transformation demo`  |
| 2  | Maven skeleton, license headers, `package-info.java`, properties, logback| `137: scaffold university-message-transformation module`             |
| 3  | Shared `ids/`                                                            | `137: course-catalog shared ids`                                     |
| 4  | `values/CapacityRange`, `events/*` (with `@EventTag`), name/tag constants | `137: course-catalog domain primitives`                              |
| 5  | Five transformation classes                                              | `137: course-catalog transformations`                                |
| 6  | `TransformationTester`, `ChainTester`, `JsonAssertions`, golden JSONs    | `137: holixon-style transformation testing helpers`                  |
| 7  | Per-transformation L1 unit tests                                         | `137: per-transformation unit tests`                                 |
| 8  | `CourseCatalogTransformations` chain composition + module configuration  | `137: chain composition and module configuration`                    |
| 9  | `write/publishcourse/` slice + test                                      | `137: publish course write slice`                                    |
| 10 | `write/updatecoursecapacity/` slice + test (chain ACL behaviour)         | `137: update course capacity write slice`                            |
| 11 | `write/enrollstudent/` slice + test (multi-tag DCB)                      | `137: enroll student write slice`                                    |
| 12 | `read/catalogview/` slice + test                                         | `137: catalog view read slice`                                       |
| 13 | `automation/overbookingnotifier/` slice + test                           | `137: overbooking notifier automation`                               |
| 14 | `seed/LegacyEventSeeder` + idempotency marker + test                    | `137: idempotent legacy event seeder`                                |
| 15 | `CourseCatalogApplication.main()` + `ConfigurationProperties`            | `137: course catalog application entry point`                        |
| 16 | `CourseCatalogAxonTestFixture`, L2/L3 chain integration tests           | `137: chain integration tests`                                       |
| 17 | Abstract `CourseCatalogApplicationTest` + concrete `MainSmokeTest`       | `137: end-to-end smoke test`                                         |
| 18 | `docker-compose.yaml`, `docs/images/`, `README.md`                       | `137: infrastructure files, dashboard wiring, README`                |
| 19 | Add module to `examples/pom.xml`; full `./mvnw -Pexamples clean verify`  | `137: include module in examples build`                              |

A `TaskList` is created at step 1 covering each row; `TaskUpdate(in_progress -> completed)`
per row so progress is observable.

---

## 16. Verification & acceptance

```bash
# Cold (default, no infra)
./mvnw -Pexamples -pl examples/university-message-transformation -am clean verify

# With Axon Server + dashboard
docker compose -f examples/university-message-transformation/docker-compose.yaml up -d
./mvnw -Pexamples -pl examples/university-message-transformation -am \
    -Daxon.server.enabled=true clean verify
open http://localhost:8024
java -cp ... org.axonframework.examples.demo.coursecatalog.CourseCatalogApplication
```

**Acceptance bars**:

- All L1 + L2 + L3 + L4 tests green in both modes
- `CourseCatalogApplication.main(new String[0])` runs to completion, prints v3 shapes for
  every seeded historic event, exits 0
- Idempotent: re-running `main()` against persistent Axon Server keeps the catalog
  stable (no duplicate seed events)
- Dashboard at http://localhost:8024 shows seeded events in their stored
  (untransformed) shape
- All existing example modules still green (`./mvnw -Pexamples clean verify` from repo root)
- Conventions checklist (section 13) passes for every new file
- Architecture passes the senior-engineer "would I ship this?" review

---

## 17. Follow-ups (filed when demo lands)

Tracked under [tasks.md T044](tasks.md):

1. Lift `TransformationTester` + `ChainTester` from demo test sources into a public
   `axoniq-message-transformation:test-jar` so end users get them as a dependency.
2. Antora reference-guide module at `docs/reference-guide/modules/message-transformation/`
   (tasks.md T034) -- covers structural transformation, predicate matching, organizing
   transformations.
3. AF4 -> AF5 transformer migration cookbook.
4. Spring Boot variant of the demo mirroring `university-java-springboot-3` /
   `university-java-springboot-4`.

---
