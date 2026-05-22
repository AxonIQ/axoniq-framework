# Public API contract (user-facing)

**Stability**: 5.2.0 GA for the events surface; commands and queries factories in 5.3+.

**Module**: `axoniq-framework/messaging/axoniq-message-transformation/`

The user-facing API consists of three things:
1. The **`MessageTransformerChain.builder()`** for registering transformations and locking the chain.
2. The per-type **factories** (`EventTransformation`, plus `CommandTransformation` and `QueryTransformation` in 5.3+) that produce `MessageTransformer` instances without the user writing SPI code.
3. The **registration pattern** in Axon configuration (a `ConfigurationEnhancer` does the wiring; the user only registers the chain instance).

See also: [shared SPI base](spi-base.md), [event SPI](spi-events.md), [command/query SPI](spi-cqrs.md).

---

## Chain registration (entry point)

The user constructs **exactly one** chain per application and registers it with the framework configuration. The framework wires it into the `EventStore` decorator in 5.2.0 and (when commands/queries arrive in 5.3+) the `CommandBus` and `QueryBus` decorators too -- all from the same chain instance.

```java
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import io.axoniq.framework.messaging.transformation.MessageTransformerChain;
import io.axoniq.framework.messaging.transformation.SemverComparator;
import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;

// 1. Build the chain.
MessageTransformerChain chain = MessageTransformerChain.builder()
    .versionOrder(SemverComparator.instance())                  // optional, FR-020
    .register(EventTransformation.from(new MessageType("com.example.CourseCreated", "1.0.0"))
                                 .to(new MessageType("com.example.CourseCreated", "2.0.0"))
                                 .transform(JsonNode.class, v1 -> { /* restructure capacity -> min/max */ }))
    .register(EventTransformation.rename(new MessageType("com.example.CourseOpened", "1.0.0"),
                                          new MessageType("com.example.CourseCreated", "2.0.0")))
    .build();

// 2. Register the chain with the Axon configuration.
//    The EventTransformationConfigurationEnhancer (5.2.0) and the
//    CqrsTransformationConfigurationEnhancer (5.3+) discover it and install the decorators
//    (TransformingEventStore on EventStore; TransformingCommandBus / TransformingQueryBus
//    on the respective buses in 5.3+).
EventSourcingConfigurer.create()
    .componentRegistry(cr -> cr.registerComponent(MessageTransformerChain.class, c -> chain))
    .start();
```

Full end-to-end example covering events + commands + queries is shown below.

**Contract**:
- `.build()` returns an immutable chain; further registration is rejected (FR-004).
- Registration order = chain application order (FR-004).
- If a `VersionComparator` is registered, ordering is enforced at `.build()` (FR-020).
- Conflict classes from FR-008 (duplicate `from`, self-loop, multi-step cycle, version-order violation) are detected and surfaced before any event is processed.

---

## `EventTransformation` factory (5.2.0)

```java
package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.NullMarked;

import java.util.List;
import java.util.function.Function;

/**
 * Factory producing {@link EventTransformer} instances for the user-facing flows
 * described in spec.md US1 through US5. Method names are reserved on this factory
 * from 5.2.0 (Forward-compatibility invariant #4); methods for nice-to-have stories
 * may be added incrementally, never renamed.
 *
 * @since 5.2.0
 */
@NullMarked
public final class EventTransformation {

    /* US1 -- 1:1 structural transformation (FR-001, MUST in 5.2.0) ----------------- */

    /**
     * Begin a 1:1 transformation from the given source identity. Returns a fluent
     * builder requiring a {@code to(...)} call to set the target identity, then a
     * {@code transform(...)} call to supply the payload mapping behaviour.
     */
    public static SingleEventTransformationBuilder from(MessageType source) { /* ... */ }

    public static final class SingleEventTransformationBuilder {

        /** Declare the target identity. */
        public SingleEventTransformationWithTargetBuilder to(MessageType target) { /* ... */ }
    }

    public static final class SingleEventTransformationWithTargetBuilder {

        /**
         * Supply the payload mapping behaviour. The mapper receives the payload converted to
         * the declared input type (typically a structured representation: {@code JsonNode},
         * {@code GenericRecord}, POJO; FR-009) and returns the transformed payload that will
         * be wrapped as the new event. The input and output Java types MAY differ -- e.g.,
         * mapping a {@code CourseCreatedV1} POJO to a {@code CourseCreatedV2} POJO, or
         * reading bytes and producing a {@code JsonNode}. The framework uses the returned
         * object's runtime type to convert back for downstream consumers.
         *
         * @param <T>          the input payload type (what the framework converts the stored payload to)
         * @param <U>          the output payload type (whatever the mapper produces)
         * @param inputType    the type to convert the input payload to before invocation
         * @param payloadMapper maps the input payload to its transformed output
         */
        public <T, U> EventTransformer transform(Class<T> inputType, Function<T, U> payloadMapper) { /* ... */ }
    }

    /* US2 -- pure rename (FR-002, SHOULD in 5.2.0) --------------------------------- */

    /**
     * Pure identity rename without a payload mapping. Equivalent to {@code from(source).to(target)}
     * with an identity payload function; the framework's rename factory sets the
     * output identity itself, so FR-018 (output identity check) is trivially satisfied.
     *
     * @return an {@link EventTransformer} that updates {@code MessageType} only
     */
    public static EventTransformer rename(MessageType source, MessageType target) { /* ... */ }

    /* US3 -- split (FR-003, MAY in 5.2.0; method name reserved per
       Forward-compatibility invariant #4) -------------------------------------- */

    /**
     * 1:N split. Begin a multi-event transformation from the given source identity.
     * Returns a builder requiring a {@code transform(...)} call that produces the
     * list of replacement events.
     */
    public static MultiEventTransformationBuilder split(MessageType source) { /* ... */ }

    public static final class MultiEventTransformationBuilder {

        /**
         * Supply the splitting behaviour. The mapper receives the input payload converted
         * to {@code T} and returns the list of replacement events that take the input
         * event's place in the stream. Returning an empty list also works as a drop, but
         * prefer {@link EventTransformation#drop(MessageType)} for the pure-drop case --
         * it is more direct and does not require a payload type or mapper.
         *
         * @param <T>               the input payload type
         * @param inputType         the type to convert the input payload to before invocation
         * @param replacementMapper maps the input payload to its replacement events
         */
        public <T> EventTransformer transform(Class<T> inputType,
                                              Function<T, List<TransformedEvent>> replacementMapper) { /* ... */ }
    }

    /* US4 -- drop (FR-003, FR-014; MAY in 5.2.0; method name reserved per
       Forward-compatibility invariant #4) -------------------------------------- */

    /**
     * 1:0 drop. The event matching {@code source} is removed from the stream entirely;
     * no payload type, no mapper, no replacement events. The tracking token still
     * advances past the dropped event (FR-014); a tracking processor resumes AFTER it
     * and does not reprocess it.
     * <p>
     * Equivalent in effect to {@code split(source).transform(Object.class, ignored -> List.of())},
     * but more direct: no payload conversion happens (the framework does not call the
     * converter for events matching a drop), keeping FR-011 lazy deserialization intact
     * even when the user did not specify an input type.
     *
     * @param source the identity ({@link MessageType}) of the event to drop
     * @return an {@link EventTransformer} that suppresses matching events
     */
    public static EventTransformer drop(MessageType source) { /* ... */ }
}
```

**Cross-references**: FR-001 to FR-003, FR-009, FR-018, US1 to US5.

---

## `CommandTransformation` and `QueryTransformation` factories (5.3+)

Same shape as `EventTransformation` but only the 1:1 and rename patterns are exposed; `split(...)` and a drop-equivalent are NOT offered on these types (FR-019).

```java
package io.axoniq.framework.messaging.transformation.cqrs;

import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.NullMarked;

import java.util.function.Function;

/**
 * Factory producing {@link CommandTransformer} instances. 1:1 only.
 *
 * @since 5.3+
 */
@NullMarked
public final class CommandTransformation {

    public static SingleCommandTransformationBuilder from(MessageType source) { /* ... */ }
    public static CommandTransformer rename(MessageType source, MessageType target) { /* ... */ }

    public static final class SingleCommandTransformationBuilder {
        public SingleCommandTransformationWithTargetBuilder to(MessageType target) { /* ... */ }
    }
    public static final class SingleCommandTransformationWithTargetBuilder {
        /**
         * Same shape as {@link EventTransformation}'s 1:1 transform.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the type to convert the input command's payload to before invocation
         * @param payloadMapper maps the input payload to its transformed output
         */
        public <T, U> CommandTransformer transform(Class<T> inputType, Function<T, U> payloadMapper) { /* ... */ }
    }
}
```

```java
package io.axoniq.framework.messaging.transformation.cqrs;

import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.NullMarked;

import java.util.function.Function;

/**
 * Factory producing {@link QueryTransformer} instances. 1:1 only.
 *
 * @since 5.3+
 */
@NullMarked
public final class QueryTransformation {

    public static SingleQueryTransformationBuilder from(MessageType source) { /* ... */ }
    public static QueryTransformer rename(MessageType source, MessageType target) { /* ... */ }

    public static final class SingleQueryTransformationBuilder {
        public SingleQueryTransformationWithTargetBuilder to(MessageType target) { /* ... */ }
    }
    public static final class SingleQueryTransformationWithTargetBuilder {
        /**
         * Same shape as {@link EventTransformation}'s 1:1 transform.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the type to convert the input query's payload to before invocation
         * @param payloadMapper maps the input payload to its transformed output
         */
        public <T, U> QueryTransformer transform(Class<T> inputType, Function<T, U> payloadMapper) { /* ... */ }
    }
}
```

**Cross-references**: FR-019, US8, US9.

---

## What the API looks like end-to-end

One chain holds every transformation -- events (US1-US5 in 5.2.0) plus commands (US8) and queries (US9) when delivered in 5.3+. Built once at startup, registered with the Axon configuration, and the framework wires it into all three ingress decorators.

```java
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import io.axoniq.framework.messaging.transformation.MessageTransformerChain;
import io.axoniq.framework.messaging.transformation.Observability;                  // FR-013
import io.axoniq.framework.messaging.transformation.SemverComparator;
import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import io.axoniq.framework.messaging.transformation.events.TransformedEvent;
import io.axoniq.framework.messaging.transformation.cqrs.CommandTransformation;     // 5.3+
import io.axoniq.framework.messaging.transformation.cqrs.QueryTransformation;       // 5.3+

import org.axonframework.messaging.core.MessageType;

import java.util.List;

MessageTransformerChain chain = MessageTransformerChain.builder()
    .versionOrder(SemverComparator.instance())                                  // optional, FR-020
    .observability(Observability.enabled())                                     // default; .disabled() for hot paths, FR-013

    // ---------- Events (5.2.0) ----------

    // US1 -- 1:1 structural transformation (FR-001, MUST in 5.2.0)
    //   v1 had a single `capacity` field, v2 splits into min/max.
    .register(EventTransformation.from(new MessageType("com.example.CourseCreated", "1.0.0"))
                                 .to  (new MessageType("com.example.CourseCreated", "2.0.0"))
                                 .transform(JsonNode.class, v1 -> {
                                     int cap = v1.get("capacity").asInt();
                                     ObjectNode v2 = JsonNodeFactory.instance.objectNode();
                                     v2.put("minCapacity", cap);
                                     v2.put("maxCapacity", cap);
                                     v2.put("name", v1.get("name").asText());
                                     return v2;
                                 }))

    // US2 -- pure rename, payload unchanged (FR-002, SHOULD in 5.2.0)
    //   After a domain refinement, CourseOpened becomes CourseCreated.
    .register(EventTransformation.rename(new MessageType("com.example.CourseOpened",  "1.0.0"),
                                          new MessageType("com.example.CourseCreated", "1.0.0")))

    // US3 -- 1:N split, declared order is delivery order (FR-003, FR-010, MAY in 5.2.0)
    //   StudentEnrolledAndCourseUpdated bundled two facts; split them apart.
    .register(EventTransformation.split(new MessageType("com.example.StudentEnrolledAndCourseUpdated", "1.0.0"))
                                 .transform(JsonNode.class, v1 -> List.of(
                                     TransformedEvent.of(new MessageType("com.example.StudentEnrolled",       "1.0.0"),
                                                         v1.get("studentEnrollment")),
                                     TransformedEvent.of(new MessageType("com.example.CourseCapacityUpdated", "1.0.0"),
                                                         v1.get("courseUpdate"))
                                 )))

    // US4 -- 1:0 drop, tracking token still advances (FR-003, FR-014, MAY in 5.2.0)
    //   SystemHeartbeat was accidentally stored; suppress it cleanly.
    .register(EventTransformation.drop(new MessageType("com.example.SystemHeartbeat", "1.0.0")))

    // US5 -- chaining across versions (FR-007, MAY in 5.2.0)
    //   Combined with US1's v1->v2 above, this v2->v3 hop completes a v1 -> v2 -> v3 chain.
    .register(EventTransformation.from(new MessageType("com.example.CourseCreated", "2.0.0"))
                                 .to  (new MessageType("com.example.CourseCreated", "3.0.0"))
                                 .transform(JsonNode.class, v2 -> {
                                     ObjectNode v3 = JsonNodeFactory.instance.objectNode();
                                     ObjectNode range = v3.putObject("capacityRange");
                                     range.put("min", v2.get("minCapacity").asInt());
                                     range.put("max", v2.get("maxCapacity").asInt());
                                     return v3;
                                 }))

    // ---------- Commands (5.3+) ----------

    // US8 -- 1:1 command transformation (FR-019)
    //   Receiver fills a default `enrollmentReason` before dispatching to the v2 handler.
    .register(CommandTransformation.from(new MessageType("com.example.EnrollStudent", "1.0.0"))
                                   .to  (new MessageType("com.example.EnrollStudent", "2.0.0"))
                                   .transform(JsonNode.class, v1 -> {
                                       ObjectNode v2 = v1.deepCopy();
                                       v2.put("enrollmentReason", "UNKNOWN");
                                       return v2;
                                   }))

    // CommandTransformation does NOT expose split(...) -- 1:N / 1:0 are compile-time forbidden
    // for commands and queries (FR-019). Commands and queries are single-intent.

    // ---------- Queries (5.3+) ----------

    // US9 -- 1:1 query transformation (FR-019)
    //   Receiver fills a default `includeArchived` filter for queries sent by older callers.
    .register(QueryTransformation.from(new MessageType("com.example.FindCoursesByFaculty", "1.0.0"))
                                 .to  (new MessageType("com.example.FindCoursesByFaculty", "2.0.0"))
                                 .transform(JsonNode.class, v1 -> {
                                     ObjectNode v2 = v1.deepCopy();
                                     v2.put("includeArchived", false);
                                     return v2;
                                 }))

    .build();

// Register with the Axon configuration. The ConfigurationEnhancers do the rest.
EventSourcingConfigurer.create()
    .componentRegistry(cr -> cr.registerComponent(MessageTransformerChain.class, c -> chain))
    .start();
```

The configuration enhancers (`EventTransformationConfigurationEnhancer` in 5.2.0, joined by `CqrsTransformationConfigurationEnhancer` in 5.3+) pick up the chain and install:
- `TransformingEventStore` decorator on `EventStore` (events read path; 5.2.0)
- `TransformingCommandBus` decorator on `CommandBus` (5.3+, handler-registration level so both local and remote dispatch are covered)
- `TransformingQueryBus` decorator on `QueryBus` (5.3+, same)

Every entity load, DCB read, tracking-processor read, incoming command, and incoming query now flows through the chain.

## Errors and validation (failure modes)

The user only observes errors when something is misconfigured. The framework surfaces all FR-008 conflict classes before any message is processed (`register(...)` for per-entry errors, `.build()` for cross-entry errors). Runtime exceptions thrown inside a transformation function propagate immediately with full context (FR-015); no silent skip.

```java
// duplicate `from`            -> ChainConfigurationException at register()
// self-loop (from == to)      -> ChainConfigurationException at register()
// multi-step cycle            -> ChainConfigurationException at .build() (with full edge list)
// version-order violation     -> ChainConfigurationException at .build() (only when a VersionComparator is set)
// runtime exception in mapper -> propagated to caller, identifying transformation + event + stream position
```

Sample DEBUG output at chain build (format framework-internal; field set stable, FR-013):

```text
DEBUG  MessageTransformerChain locked with 7 transformations:
DEBUG    [1] com.example.CourseCreated@1.0.0 -> com.example.CourseCreated@2.0.0 (events, 1:1, payload mapper)
DEBUG    [2] com.example.CourseOpened@1.0.0  -> com.example.CourseCreated@1.0.0  (events, rename)
DEBUG    [3] com.example.StudentEnrolledAndCourseUpdated@1.0.0                   (events, split 1:N)
DEBUG    [4] com.example.SystemHeartbeat@1.0.0                                   (events, drop 1:0)
DEBUG    [5] com.example.CourseCreated@2.0.0 -> com.example.CourseCreated@3.0.0  (events, 1:1, payload mapper)
DEBUG    [6] com.example.EnrollStudent@1.0.0 -> com.example.EnrollStudent@2.0.0  (commands, 1:1, payload mapper)
DEBUG    [7] com.example.FindCoursesByFaculty@1.0.0 -> com.example.FindCoursesByFaculty@2.0.0  (queries, 1:1, payload mapper)
```

---

## What is NOT in the public API

- The `MessageTransformer<M>` interface and its specializations are SPI -- advanced users MAY implement them directly but the factories are the supported route.
- `TransformingEventStore`, `TransformingEventStoreTransaction`, `TransformingCommandBus`, `TransformingQueryBus` are `@Internal` -- users do not instantiate them; the `ConfigurationEnhancer` does.
- The append / dispatch / publish paths are not decorated; transformations run at READ / receive only (FR-021).
- Sender-side transformation (new-to-old at the sender, "downcasting" in industry terms) is explicitly out of scope (spec Part C).
- Annotation-based registration is deferred to a future release; programmatic registration via `MessageTransformerChain.builder()` is the only path in 5.2.0 (FR-004, Forward-compatibility invariant #5).
