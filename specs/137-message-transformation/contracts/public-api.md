# Public API contract (user-facing)

**Stability**: 5.2.0 GA for the events surface; commands and queries factories in 5.3+.

**Module**: `axoniq-framework/messaging/axoniq-message-transformation/`

The user-facing API consists of three things:
1. A **typed chain per message type** -- `EventTransformerChain.builder()` (5.2.0), plus `CommandTransformerChain.builder()` / `QueryTransformerChain.builder()` (5.3+). Each is registered as its own component.
2. The per-type **factories** (`EventTransformation`, plus `CommandTransformation` and `QueryTransformation` in 5.3+) that produce `MessageTransformation` instances without the user writing SPI code.
3. The **registration pattern** in Axon configuration (a `ConfigurationEnhancer` per message type does the wiring; the user only registers the typed chain instance(s) as components).

See also: [shared SPI base](spi-base.md), [event SPI](spi-events.md), [commands and queries SPI](spi-commands-queries.md).

---

## Chain registration (entry point)

The user constructs **one chain per message type** -- transformers do not overlap across types, so each chain is independent. In 5.2.0 that's just `EventTransformerChain`; in 5.3+ `CommandTransformerChain` and `QueryTransformerChain` join. Each is registered as its own component; each is picked up by its own `ConfigurationEnhancer` and wired into its own decorator (`TransformingEventStore` / `TransformingCommandBus` / `TransformingQueryBus`).

```java
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;

// 1. Build the events chain.
EventTransformerChain eventChain = EventTransformerChain.builder()
    .register(EventTransformation.from(new MessageType("com.example.CourseCreated", "1.0.0"))
                                 .to(new MessageType("com.example.CourseCreated", "2.0.0"))
                                 .transform(JsonNode.class, (v1, ctx) -> { /* restructure capacity -> min/max */ }))
    .register(EventTransformation.rename(new MessageType("com.example.CourseOpened", "1.0.0"),
                                          new MessageType("com.example.CourseCreated", "2.0.0")))
    .build();

// 2. Register the chain with the Axon configuration.
//    EventTransformationConfigurationEnhancer discovers it and installs TransformingEventStore.
//    For commands / queries (5.3+), the user builds CommandTransformerChain / QueryTransformerChain
//    in parallel and registers each as its own component; their enhancers wire the respective bus decorators.
EventSourcingConfigurer.create()
    .componentRegistry(cr -> cr.registerComponent(EventTransformerChain.class, c -> eventChain))
    .start();
```

Full end-to-end example covering events + commands + queries is shown below.

**Contract** (applies to every typed chain):
- `.build()` returns an immutable chain; further registration is rejected (FR-004).
- Registration order = chain application order (FR-004).
- Version ranges expressed via `from(Predicate<MessageType>)` (FR-005, FR-020).
- Conflict classes from FR-008 (duplicate concrete `from`, self-loop, multi-step cycle on concrete edges) are detected before any message is processed. A defensive runtime safety bound guards against infinite loops from pathological misconfiguration; under normal use it never fires.
- **Each chain is a passive registry.** It does NOT hold a `MessageConverter` reference. The framework's per-type `ConfigurationEnhancer` resolves the registered `MessageConverter` from {@code Configuration} at decorator-registration time and passes it into the matching `Transforming*` decorator (pattern mirrored from `AnnotatedEventSourcedEntityModule` and `EventSourcingConfigurationDefaults`). Users never thread a converter through the builder.

---

## `EventTransformation` factory (5.2.0)

```java
package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Factory producing {@link EventTransformation} instances. Use one of the static methods
 * ({@link #from(MessageType)} / {@link #from(Predicate)},
 * {@link #rename(MessageType, MessageType)}, {@link #split(MessageType)},
 * {@link #drop(MessageType)}) and register the result with
 * {@code EventTransformerChain.builder().register(...)}.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@NullMarked
public final class EventTransformation {

    /* US1 -- 1:1 structural transformation (FR-001, MUST in 5.2.0) ----------------- */

    /**
     * Begin a 1:1 transformation matching the given concrete {@code from} identity by
     * exact equality. Continue with {@code to(...)} then {@code transform(...)}.
     *
     * @param source the {@code from} identity
     * @return a builder awaiting {@code to(...)}
     */
    public static SingleEventTransformationBuilder from(MessageType source) { /* ... */ }

    /**
     * Begin a 1:1 transformation matching any {@link MessageType} for which the supplied
     * predicate returns {@code true}. Use this overload for range / regex / semver matching.
     * Continue with {@code to(...)} then {@code transform(...)}.
     *
     * @param sourcePredicate the matcher
     * @return a builder awaiting {@code to(...)}
     */
    public static SingleEventTransformationBuilder from(Predicate<MessageType> sourcePredicate) { /* ... */ }

    public static final class SingleEventTransformationBuilder {

        /**
         * Declare the {@code to} identity.
         *
         * @param target the {@code to} identity
         * @return a builder awaiting {@code transform(...)}
         */
        public SingleEventTransformationWithTargetBuilder to(MessageType target) { /* ... */ }
    }

    public static final class SingleEventTransformationWithTargetBuilder {

        /**
         * Supply the payload mapping behaviour. The framework invokes
         * {@code MessageConverter.convertPayload(message, inputType)} on the inner event message
         * to obtain a value of {@code inputType} (e.g. {@code JsonNode}, a POJO) before invoking
         * the mapper. The mapper's return value becomes the new payload of the rewritten
         * {@code EventMessage}; downstream handlers convert that payload to their own preferred
         * Java type via their own {@code MessageConverter} call -- the transformation chain
         * itself does NOT convert "back". Input and output Java types MAY differ.
         * <p>
         * The framework verifies that the output payload's resolved {@link MessageType} matches
         * the declared {@code to} when resolution is possible (typed POJO with {@code @Event} /
         * {@code @Message} annotation -- {@code MessageTypeResolver.resolve(Class<?>)} returns
         * a non-empty {@code Optional}). For untyped representations ({@code JsonNode},
         * {@code Map<String, Object>}, raw bytes) the resolver returns {@code Optional.empty()}
         * and the framework trusts the mapper to produce the correct {@code to} identity --
         * see FR-018.
         * <p>
         * The mapper also receives the active {@link ProcessingContext}. The entity-load read
         * path always supplies a non-null context; the tracking-processor read path
         * ({@code EventStore.open(StreamingCondition, @Nullable ProcessingContext)}) MAY supply
         * {@code null} if the caller passed {@code null}. Null-check before calling
         * {@code .get*(...)} on it. Most transformations do not need the context.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the type the stored payload is converted to before invocation
         * @param payloadMapper maps the input payload + (possibly-null) processing context to
         *                      its transformed output
         * @return the resulting {@link EventTransformation}
         */
        public <T, U> EventTransformation transform(Class<T> inputType,
                                                  BiFunction<T, @Nullable ProcessingContext, U> payloadMapper) { /* ... */ }

        /**
         * Generic-type overload. Use this when {@code inputType} carries type parameters
         * (e.g. {@code Map<String, Object>}, {@code List<Foo>}). Per
         * {@code .claude/rules/type-safety.md} and mirroring
         * {@code Configuration.getComponent(TypeReference)}, the factory accepts a
         * {@link org.axonframework.common.TypeReference} so {@code T} is bound at compile
         * time and the lambda parameter type is inferred. Behaviour is identical to the
         * {@code Class<T>} overload; internally the factory calls
         * {@code inputType.getType()} before handing to {@code MessageConverter.convertPayload}.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the {@link org.axonframework.common.TypeReference} the stored
         *                      payload is converted to
         * @param payloadMapper maps the input payload + processing context to its transformed output
         * @return the resulting {@link EventTransformation}
         */
        public <T, U> EventTransformation transform(org.axonframework.common.TypeReference<T> inputType,
                                                  BiFunction<T, @Nullable ProcessingContext, U> payloadMapper) { /* ... */ }
    }

    /* US2 -- pure rename (FR-002, SHOULD in 5.2.0) --------------------------------- */

    /**
     * Pure identity rename: payload passes through unchanged, only the {@link MessageType}
     * is updated.
     *
     * @param source the {@code from} identity
     * @param target the {@code to} identity
     * @return the resulting {@link EventTransformation}
     */
    public static EventTransformation rename(MessageType source, MessageType target) { /* ... */ }

    /* US3 -- split (FR-003, MAY in 5.2.0; method name reserved per
       Forward-compatibility invariant #4) -------------------------------------- */

    /**
     * Begin a 1:N split. The stored payload is converted to {@code inputType} once. Each
     * output declared via {@code producing(...)} derives its payload from that converted
     * input. Finish with {@code build()}. A {@code TypeReference<T>} overload exists for
     * generic input types.
     *
     * @param <T>       input payload type
     * @param source    the {@code from} identity
     * @param inputType the type the stored payload is converted to before invocation
     * @return a builder awaiting one or more {@code producing(...)} declarations
     */
    public static <T> SplitStep<T> split(MessageType source, Class<T> inputType) { /* ... */ }

    public static final class SplitStep<T> {

        /**
         * Declare one produced event, pairing its identity with the mapper deriving its
         * payload. Outputs are emitted in declaration order; each inherits the input
         * event's tracking token and sequence number. The declared identities widen a
         * type-filtering read back to the source. Declaring the same identity twice emits
         * one event per declaration. A context-aware {@code BiFunction} overload exists,
         * receiving the active {@link ProcessingContext} -- non-null on the entity-load
         * read path; possibly-null on the tracking-processor read path (see the 1:1
         * overload's Javadoc for details).
         *
         * @param producedType the produced event's identity
         * @param outputMapper maps the input payload to the produced event's payload
         * @return this builder, for further declarations or {@code build()}
         */
        public SplitStep<T> producing(MessageType producedType, Function<T, ?> outputMapper) { /* ... */ }

        /**
         * Complete the split. A split always emits exactly its declared outputs; to remove
         * an event from the read stream instead, use
         * {@link EventTransformation#drop(MessageType)} -- it skips payload conversion
         * entirely.
         *
         * @return the resulting {@link EventTransformation}
         * @throws IllegalArgumentException if no output was declared
         */
        public EventTransformation build() { /* ... */ }
    }

    /* US4 -- drop (FR-003, FR-014; MAY in 5.2.0; method name reserved per
       Forward-compatibility invariant #4) -------------------------------------- */

    /**
     * 1:0 drop. The matching event is removed from the stream; no payload conversion happens.
     * The tracking token still advances past the dropped event, so a tracking processor
     * resumes after it and does not reprocess it.
     *
     * @param source the {@code from} identity of the event to drop
     * @return an {@link EventTransformation} that suppresses matching events
     */
    public static EventTransformation drop(MessageType source) { /* ... */ }
}
```

**Cross-references**: FR-001 to FR-003, FR-009, FR-018, US1 to US5.

---

## `CommandTransformation` and `QueryTransformation` factories (5.3+)

Same shape as `EventTransformation` but only the 1:1 and rename patterns are exposed; `split(...)` and a drop-equivalent are NOT offered on these types (FR-019).

```java
package io.axoniq.framework.messaging.transformation.commandhandling;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Factory producing {@link CommandTransformation} instances. 1:1 only -- commands are
 * single-intent, so split / drop are not exposed. Mirrors the {@link EventTransformation}
 * shape for the patterns it does support.
 *
 * @author Laura Devriendt
 * @since 5.3+
 */
@NullMarked
public final class CommandTransformation {

    /**
     * Begin a 1:1 transformation. Continue with {@code to(...)} then {@code transform(...)}.
     *
     * @param source the {@code from} identity
     * @return a builder awaiting {@code to(...)}
     */
    public static SingleCommandTransformationBuilder from(MessageType source) { /* ... */ }

    /** Predicate-based source matching; see {@link EventTransformation#from(Predicate)}. */
    public static SingleCommandTransformationBuilder from(Predicate<MessageType> sourcePredicate) { /* ... */ }

    /**
     * Pure identity rename: payload passes through unchanged.
     *
     * @param source the {@code from} identity
     * @param target the {@code to} identity
     * @return the resulting {@link CommandTransformation}
     */
    public static CommandTransformation rename(MessageType source, MessageType target) { /* ... */ }

    public static final class SingleCommandTransformationBuilder {
        /**
         * Declare the {@code to} identity.
         *
         * @param target the {@code to} identity
         * @return a builder awaiting {@code transform(...)}
         */
        public SingleCommandTransformationWithTargetBuilder to(MessageType target) { /* ... */ }
    }
    public static final class SingleCommandTransformationWithTargetBuilder {
        /**
         * Supply the payload mapping. See {@link EventTransformation}'s 1:1 transform for details.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the type the input command's payload is converted to before invocation
         * @param payloadMapper maps the input payload + processing context to its transformed output
         * @return the resulting {@link CommandTransformation}
         */
        public <T, U> CommandTransformation transform(Class<T> inputType,
                                                    BiFunction<T, @Nullable ProcessingContext, U> payloadMapper) { /* ... */ }
    }
}
```

```java
package io.axoniq.framework.messaging.transformation.queryhandling;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Factory producing {@link QueryTransformation} instances. 1:1 only. Subscription-query update
 * streams flowing back to subscribers are NOT transformed -- only the incoming query is.
 *
 * @author Laura Devriendt
 * @since 5.3+
 */
@NullMarked
public final class QueryTransformation {

    /**
     * Begin a 1:1 transformation. Continue with {@code to(...)} then {@code transform(...)}.
     *
     * @param source the {@code from} identity
     * @return a builder awaiting {@code to(...)}
     */
    public static SingleQueryTransformationBuilder from(MessageType source) { /* ... */ }

    /** Predicate-based source matching; see {@link EventTransformation#from(Predicate)}. */
    public static SingleQueryTransformationBuilder from(Predicate<MessageType> sourcePredicate) { /* ... */ }

    /**
     * Pure identity rename: payload passes through unchanged.
     *
     * @param source the {@code from} identity
     * @param target the {@code to} identity
     * @return the resulting {@link QueryTransformation}
     */
    public static QueryTransformation rename(MessageType source, MessageType target) { /* ... */ }

    public static final class SingleQueryTransformationBuilder {
        /**
         * Declare the {@code to} identity.
         *
         * @param target the {@code to} identity
         * @return a builder awaiting {@code transform(...)}
         */
        public SingleQueryTransformationWithTargetBuilder to(MessageType target) { /* ... */ }
    }
    public static final class SingleQueryTransformationWithTargetBuilder {
        /**
         * Supply the payload mapping. See {@link EventTransformation}'s 1:1 transform for details.
         *
         * @param <T>           input payload type
         * @param <U>           output payload type
         * @param inputType     the type the input query's payload is converted to before invocation
         * @param payloadMapper maps the input payload + processing context to its transformed output
         * @return the resulting {@link QueryTransformation}
         */
        public <T, U> QueryTransformation transform(Class<T> inputType,
                                                  BiFunction<T, @Nullable ProcessingContext, U> payloadMapper) { /* ... */ }
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
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import io.axoniq.framework.messaging.transformation.commandhandling.CommandTransformerChain;   // 5.3+
import io.axoniq.framework.messaging.transformation.commandhandling.CommandTransformation;    // 5.3+
import io.axoniq.framework.messaging.transformation.queryhandling.QueryTransformerChain;       // 5.3+
import io.axoniq.framework.messaging.transformation.queryhandling.QueryTransformation;         // 5.3+

import org.axonframework.messaging.core.MessageType;

import java.util.List;

// ---------- Events chain (5.2.0) ----------

EventTransformerChain eventChain = EventTransformerChain.builder()

    // US1 -- 1:1 structural transformation (FR-001, MUST in 5.2.0).
    //   v1 had a single `capacity` field, v2 splits into min/max.
    //   The mapper receives the converted payload + active ProcessingContext (nullable).
    .register(EventTransformation.from(new MessageType("com.example.CourseCreated", "1.0.0"))
                                 .to  (new MessageType("com.example.CourseCreated", "2.0.0"))
                                 .transform(JsonNode.class, (v1, ctx) -> {
                                     int cap = v1.get("capacity").asInt();
                                     ObjectNode v2 = JsonNodeFactory.instance.objectNode();
                                     v2.put("minCapacity", cap);
                                     v2.put("maxCapacity", cap);
                                     v2.put("name", v1.get("name").asText());
                                     return v2;
                                 }))

    // US2 -- pure rename, payload unchanged (FR-002, SHOULD in 5.2.0)
    .register(EventTransformation.rename(new MessageType("com.example.CourseOpened",  "1.0.0"),
                                          new MessageType("com.example.CourseCreated", "1.0.0")))

    // US3 -- 1:N split (FR-003, FR-010, MAY in 5.2.0)
    .register(EventTransformation.split(new MessageType("com.example.StudentEnrolledAndCourseUpdated", "1.0.0"),
                                        JsonNode.class)
                                 .producing(new MessageType("com.example.StudentEnrolled", "1.0.0"),
                                            v1 -> v1.get("studentEnrollment"))
                                 .producing(new MessageType("com.example.CourseCapacityUpdated", "1.0.0"),
                                            v1 -> v1.get("courseUpdate"))
                                 .build())

    // US4 -- 1:0 drop (FR-003, FR-014, MAY in 5.2.0)
    .register(EventTransformation.drop(new MessageType("com.example.SystemHeartbeat", "1.0.0")))

    // US5 -- chaining across versions, predicate-based `from` (FR-007, MAY in 5.2.0)
    //   Note: MessageType.qualifiedName() returns a QualifiedName record (NOT a String);
    //         use .name() to compare to the fully-qualified name string.
    .register(EventTransformation.from(mt -> mt.qualifiedName().name().equals("com.example.CourseCreated")
                                              && mt.version().startsWith("2."))
                                 .to  (new MessageType("com.example.CourseCreated", "3.0.0"))
                                 .transform(JsonNode.class, (v2, ctx) -> {
                                     ObjectNode v3 = JsonNodeFactory.instance.objectNode();
                                     ObjectNode range = v3.putObject("capacityRange");
                                     range.put("min", v2.get("minCapacity").asInt());
                                     range.put("max", v2.get("maxCapacity").asInt());
                                     return v3;
                                 }))

    .build();

// ---------- Command chain (5.3+) ----------

// US8 -- 1:1 command transformation (FR-019). Receiver fills a default `enrollmentReason`.
// CommandTransformation does NOT expose split(...): 1:N / 1:0 are compile-time forbidden
// for commands and queries (FR-019). Commands and queries are single-intent.
CommandTransformerChain commandChain = CommandTransformerChain.builder()
    .register(CommandTransformation.from(new MessageType("com.example.EnrollStudent", "1.0.0"))
                                   .to  (new MessageType("com.example.EnrollStudent", "2.0.0"))
                                   .transform(JsonNode.class, (v1, ctx) -> {
                                       ObjectNode v2 = v1.deepCopy();
                                       v2.put("enrollmentReason", "UNKNOWN");
                                       return v2;
                                   }))
    .build();

// ---------- Query chain (5.3+) ----------

// US9 -- 1:1 query transformation (FR-019). Receiver fills `includeArchived` default.
QueryTransformerChain queryChain = QueryTransformerChain.builder()
    .register(QueryTransformation.from(new MessageType("com.example.FindCoursesByFaculty", "1.0.0"))
                                 .to  (new MessageType("com.example.FindCoursesByFaculty", "2.0.0"))
                                 .transform(JsonNode.class, (v1, ctx) -> {
                                     ObjectNode v2 = v1.deepCopy();
                                     v2.put("includeArchived", false);
                                     return v2;
                                 }))
    .build();

// Register each chain as its own component. The matching ConfigurationEnhancers wire
// each into its own decorator (TransformingEventStore / TransformingCommandBus /
// TransformingQueryBus).
EventSourcingConfigurer.create()
    .componentRegistry(cr -> cr
        .registerComponent(EventTransformerChain.class,   c -> eventChain)
        .registerComponent(CommandTransformerChain.class, c -> commandChain)     // 5.3+
        .registerComponent(QueryTransformerChain.class,   c -> queryChain))      // 5.3+
    .start();
```

The configuration enhancers (`EventTransformationConfigurationEnhancer` in 5.2.0; the per-sub-package `CommandTransformationConfigurationEnhancer` + `QueryTransformationConfigurationEnhancer` in 5.3+) pick up the chain and install:
- `TransformingEventStore` decorator on `EventStore` (events read path; 5.2.0)
- `TransformingCommandBus` decorator on `CommandBus` (5.3+, on the bus -- not on a `CommandBusConnector` -- so transformation is available even without distributed messaging)
- `TransformingQueryBus` decorator on `QueryBus` (5.3+, same -- on the bus)

Every entity load, DCB read, tracking-processor read, incoming command, and incoming query now flows through the chain.

## Errors and validation (failure modes)

The user only observes errors when something is misconfigured. The framework surfaces all FR-008 conflict classes before any message is processed (`register(...)` for per-entry errors, `.build()` for cross-entry errors). Runtime exceptions thrown inside a transformation function propagate immediately with full context (FR-015); no silent skip.

```java
// duplicate concrete `from`     -> ChainConfigurationException at register()
// self-loop (concrete from==to) -> ChainConfigurationException at register()
// multi-step cycle              -> ChainConfigurationException at .build() (concrete-edge graph, with full edge list)
// runtime exception in mapper   -> propagated to caller, identifying transformation + event + stream position
```

A defensive runtime safety bound prevents an infinite loop from pathological predicate
misconfiguration; under normal use it never fires.

---

## What is NOT in the public API

- The `MessageTransformation<M>` interface and its specializations are SPI -- advanced users MAY implement them directly but the factories are the supported route.
- `TransformingEventStore`, `TransformingEventStoreTransaction`, `TransformingCommandBus`, `TransformingQueryBus` are `@Internal` -- users do not instantiate them; the `ConfigurationEnhancer` does.
- The append / dispatch / publish paths are not decorated; transformations run at READ / receive only (FR-021).
- Sender-side transformation (new-to-old at the sender, "downcasting" in industry terms) is explicitly out of scope (spec Part C).
- Annotation-based registration is deferred to a future release; programmatic registration via `EventTransformerChain.builder()` (and the future `CommandTransformerChain.builder()` / `QueryTransformerChain.builder()`) is the only path in 5.2.0 (FR-004, Forward-compatibility invariant #5).
