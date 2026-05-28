# SPI events contract

**Stability**: 5.2.0 GA.

**Module**: `axoniq-framework/messaging/axoniq-message-transformation/`
**Package**: `io.axoniq.framework.messaging.transformation.events`

Event-specific specialization of the [shared SPI base](spi-base.md). Ships with the integration types that decorate the `EventStore` at runtime. Covers US1-US5. User-facing factory methods and end-to-end usage examples are in [public-api.md](public-api.md); chain-level concerns are in [spi-base.md](spi-base.md).

## `EventTransformer`

`MessageTransformer<EventMessage>` specialization for events.

```java
package io.axoniq.framework.messaging.transformation.events;

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Event-specific {@link MessageTransformer}. Use the {@code EventTransformation} factory
 * rather than implementing directly. See {@link MessageTransformer} for the transformation
 * contract; this interface narrows the message type to {@link EventMessage}.
 * <p>
 * A transformer rewrites only the {@link EventMessage} itself -- its
 * {@link org.axonframework.messaging.core.MessageType}, payload, and metadata. Properties
 * that live alongside the event in the read stream -- the tags resolved at append time
 * and any tracking-position or sequence information carried on the
 * {@link MessageStream.Entry} context -- are not part of the message and are not
 * modified by the chain. For 1:N splits, every output is delivered against the same
 * stream position as the input.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@NullMarked
public interface EventTransformer extends MessageTransformer<EventMessage> {

    @Override
    MessageStream<? extends EventMessage> transform(EventMessage message, @Nullable ProcessingContext context);
}
```

**Event-specific contract** (in addition to base contract in [spi-base.md](spi-base.md)):
- **Envelope preservation** (FR-010): entity type, entity identifier, tracking token, and sequence number MUST be carried unchanged from input to output. Framework overrides any attempt to modify them. For 1:N splits, every replacement inherits the input's envelope (no renumbering -- all N share the input's tracking token + sequence number). Metadata MAY be modified via the message-level entry point.
- **Snapshot pass-through (entity-load path only)**: `SnapshotEventMessage` entries (subtype of `EventMessage`, lives at `eventsourcing/eventstore/SnapshotEventMessage.java`) are prepended into the entity-load stream by `SnapshotCapableEventStorageEngine` when `SourcingStrategy.Snapshot` is active (verified at `SnapshotCapableEventStorageEngine.java:82-107`). That stream reaches the chain via `transaction(...).source(...)`, so FR-005 unknown-`MessageType` pass-through covers it. **Tracking-processor reads** (`open(StreamingCondition, ...)`) do NOT carry snapshots -- only post-snapshot events. A future snapshot transformation API can opt in for entity-loads with no wiring change here; firing on streaming reads will need its own hook regardless (Forward-compatibility invariant #6).

**Cross-references**: FR-001 to FR-003, FR-010, US1 to US5.

---

## `TransformedEvent`

Output type for 1:N split mappers. The framework wraps each into an `EventMessage`, preserving the input event's envelope per FR-010.

```java
package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.NullMarked;

/**
 * One output of a 1:N split mapper. The framework wraps each into an
 * {@link org.axonframework.messaging.eventhandling.EventMessage}; all N outputs inherit
 * the input event's envelope (tracking token, sequence number, entity identity).
 *
 * @param type    identity of the replacement event
 * @param payload new payload; its runtime type drives downstream conversion
 * @author Laura Devriendt
 * @since 5.2.0
 */
@NullMarked
public record TransformedEvent(MessageType type, Object payload) {

    /**
     * Convenience factory.
     *
     * @param type    identity of the replacement event
     * @param payload new payload
     * @return a {@link TransformedEvent}
     */
    public static TransformedEvent of(MessageType type, Object payload) {
        return new TransformedEvent(type, payload);
    }
}
```

**Cross-references**: FR-003, FR-009, FR-010, US3.

---

## `EventTransformerChain`

Public, immutable chain of `EventTransformer` instances. Built once at startup via `EventTransformerChain.builder()` and locked at `.build()`. Registered with the framework configuration as a component; the `EventTransformationConfigurationEnhancer` (see below) installs the decorator.

Behaviour is shared across all typed chains (events / commands / queries) -- see [spi-base.md](spi-base.md) "Chains are per message type" for the rationale. Specifically: startup-only registration (FR-004), fixed-point iteration with last-match-wins (FR-007), `QualifiedName`-keyed map + predicate list hybrid lookup (FR-011), conflict detection (FR-008), defensive runtime safety bound. Same contract clauses, typed to `EventMessage`.

```java
package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.NullMarked;

/**
 * Immutable chain of {@link EventTransformer} instances applied at event read time.
 * Built once at startup via {@link Builder} and locked on {@link Builder#build()}.
 * Register the chain with the Axon configuration as an
 * {@code EventTransformerChain.class}-typed component; the framework installs the
 * read-side decorators automatically.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@NullMarked
public final class EventTransformerChain {

    /**
     * Apply the chain to the given stream. Fixed-point iteration, last match wins; see
     * {@link io.axoniq.framework.messaging.transformation.MessageTransformer} for the
     * per-element shape. Non-matching elements pass through unchanged in constant time.
     *
     * @param stream the input stream of events
     * @return the transformed stream
     */
    public MessageStream<? extends EventMessage> transform(MessageStream<EventMessage> stream) { /* ... */ }

    public static Builder builder() { /* ... */ }

    /** Fluent builder; registration order = application order. */
    public static final class Builder {
        public Builder register(EventTransformer transformer) { /* ... */ }
        public EventTransformerChain build() { /* ... */ }
    }
}
```

**Future additive overload (5.3+, not in 5.2.0)**: a single-message entry point `MessageStream.Single<? extends EventMessage> transform(EventMessage message, @Nullable ProcessingContext context)` is reserved. Events wire via `EventStore.transaction().source(...)` and `EventStore.open(...)` -- both return `MessageStream`, so the stream-in / stream-out method above fits the decorator wiring exactly. Commands and queries (5.3+) wire at `CommandBus.subscribe(...)` / `QueryBus.subscribe(...)` time, which is single-message-in -- those chains will gain the single-message overload as a pure additive Java overload (no SPI break, forward-compat invariant #4 reserves the name).

**Cross-references**: FR-004, FR-005, FR-007, FR-008, FR-011, FR-013, US1, US5, US6, US7.

---

## `TransformingEventStore` (integration type, internal)

Decorator on `EventStore` registered by `EventTransformationConfigurationEnhancer`. Borrows the structural pattern from `InterceptingEventStore` (decorator on `EventStore` + wrapping `EventStoreTransaction` cached per `ProcessingContext`), but **deliberately deviates** in one respect: `InterceptingEventStore.open(...)` delegates straight through (`InterceptingEventStore.java:153-155`), whereas `TransformingEventStore.open(...)` MUST wrap and pipe the inner stream through the chain, because FR-012 requires transformation to fire on tracking-processor reads in addition to entity loads and DCB reads.

```java
package io.axoniq.framework.messaging.transformation.events;

import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * {@link EventStore} decorator that applies a {@link EventTransformerChain} to every read
 * path (entity loads, DCB reads, tracking-processor reads). Installed automatically by
 * {@code EventTransformationConfigurationEnhancer}; not constructed by users.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@NullMarked
@Internal
public final class TransformingEventStore implements EventStore {

    /**
     * Decoration order: outer (later) than {@code InterceptingEventStore} (which uses
     * {@code MIN_VALUE + 50}). The {@code +1000} offset leaves headroom for users or
     * framework to slot other decorators in between.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 1000;

    /**
     * @param delegate  the inner {@link EventStore} to wrap
     * @param chain     the application's {@link EventTransformerChain} (passive registry)
     * @param converter the active {@link MessageConverter} used to convert payloads to each
     *                  matched transformer's declared {@code inputType} before invoking the
     *                  mapper. Resolved by {@code EventTransformationConfigurationEnhancer}
     *                  from {@code Configuration.getComponent(MessageConverter.class)} at
     *                  decorator-registration time; users do NOT construct this class.
     */
    public TransformingEventStore(EventStore delegate,
                                   EventTransformerChain chain,
                                   MessageConverter converter) { /* ... */ }

    /**
     * Returns a {@link TransformingEventStoreTransaction} cached per
     * {@link ProcessingContext}; {@code source(...)} pipes through the chain.
     */
    @Override
    public EventStoreTransaction transaction(ProcessingContext processingContext) { /* ... */ }

    /** Returns the inner stream piped through the chain (tracking-processor reads). */
    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition,
                                            @Nullable ProcessingContext context) { /* ... */ }

    /* publish, firstToken, latestToken, tokenAt, subscribe, describeTo delegate
       unchanged to the inner EventStore -- they do not touch event payloads. */
}
```

**Behaviour**:
- `open(...)`: returns the inner stream piped through `chain.transform(stream)`. Tracking-processor reads. **Deviation from `InterceptingEventStore`**: that precedent delegates `open(...)` unwrapped; here we MUST wrap to honour FR-012. No per-call caching: `open(...)` accepts a `@Nullable ProcessingContext`, so a `Context.ResourceKey` is not always available; each call rewraps the stream lazily.
- `transaction(...)`: returns a `TransformingEventStoreTransaction` wrapping the delegate, cached per `ProcessingContext` via `Context.ResourceKey` (mirrors `InterceptingEventStore`). Entity loads + DCB reads.
- Everything else (`publish`, tokens, `subscribe`): delegated unchanged. Chain runs at READ only (FR-021).
- `SourcingCondition` / `StreamingCondition` filtering + `ConsistencyMarker` / `TerminalEventMessage` bookkeeping happen in the underlying engine BEFORE the chain receives the stream.
- **Tracking-token progress through drops** (FR-014): when a transformation drops an event, the underlying stream's tracking-token progress MUST still surface to the consumer (the chain only removes the element from the emitted output; it does NOT discard the position). A restarting tracking processor therefore resumes AFTER the dropped event and does not reprocess it.
- **Data-protection ordering** (FR-021): `DECORATION_ORDER` keeps `TransformingEventStore` outside any handler-side data-protection interceptor, so protection runs on the transformed shape the handler receives.
- **Order direction**: `ComponentRegistry` applies decorators in ascending order of the `order` argument (`DefaultComponentRegistry.java:299-303`); a higher order wraps a lower one. So `Integer.MIN_VALUE + 1000` runs outer (invoked first by the client, delegates inward to `InterceptingEventStore` at `+50`, then to the underlying `EventStore`).

**Cross-references**: FR-010, FR-011, FR-012, FR-014, FR-021, US1 scenario 4, US4 scenario 3.

---

## `TransformingEventStoreTransaction` (integration type, internal)

```java
package io.axoniq.framework.messaging.transformation.events;

import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.NullMarked;

/**
 * Wrapping {@link EventStoreTransaction} returned by
 * {@link TransformingEventStore#transaction(ProcessingContext)}. Applies the chain to
 * {@link #source(SourcingCondition)} only; append / position methods delegate unchanged
 * because the chain runs at read time.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@NullMarked
@Internal
final class TransformingEventStoreTransaction implements EventStoreTransaction {

    /**
     * @param delegate  the inner {@link EventStoreTransaction} to wrap
     * @param chain     the application's {@link EventTransformerChain} (passive registry)
     * @param converter the active {@link MessageConverter}; same instance the parent
     *                  {@link TransformingEventStore} holds, used to convert payloads to each
     *                  matched transformer's declared {@code inputType} before invoking the
     *                  mapper. Resolved at decorator-registration time, NOT at user-build time.
     */
    TransformingEventStoreTransaction(EventStoreTransaction delegate,
                                       EventTransformerChain chain,
                                       MessageConverter converter) { /* ... */ }

    /** Returns the delegate's stream piped through the chain. */
    @Override
    public MessageStream<? extends EventMessage> source(SourcingCondition condition) {
        /* applies chain to the delegate's stream */
    }

    /* appendEvent, onAppend, overrideAppendCondition, appendPosition all delegate
       to the inner transaction -- transformations run at READ only. */
}
```

`source(...)` applies the chain; the other four methods delegate unchanged.

**Cross-references**: FR-010, FR-021.

---

## `EventTransformationConfigurationEnhancer`

ServiceLoader-discovered enhancer that wires `TransformingEventStore` as a decorator on `EventStore` via `ComponentRegistry.registerDecorator(...)` (`ComponentRegistry.java:115-121`). At decorator-construction time the lambda receives a `Configuration` and resolves both the user-registered `EventTransformerChain` AND the active `MessageConverter` -- this is the established AF5 pattern (see `EventSourcingConfigurationDefaults.java:96-108` and `AnnotatedEventSourcedEntityModule.buildMetaModel(Configuration):113-131`).

```java
package io.axoniq.framework.messaging.transformation.events.configuration;

import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import io.axoniq.framework.messaging.transformation.events.TransformingEventStore;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.jspecify.annotations.NullMarked;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that installs the
 * {@link TransformingEventStore} decorator. Reads the user-supplied
 * {@link EventTransformerChain} and the active {@link MessageConverter} from the
 * {@code Configuration} at decorator-registration time; a no-op if no chain is registered.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@NullMarked
public final class EventTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerDecorator(EventStore.class, TransformingEventStore.DECORATION_ORDER,
            (config, name, delegate) -> {
                EventTransformerChain chain = config.getComponent(EventTransformerChain.class);
                if (chain == null) {
                    return delegate; // no chain registered -> no-op
                }
                MessageConverter converter = config.getComponent(MessageConverter.class);
                return new TransformingEventStore(delegate, chain, converter);
            });
    }
}
```

**Cross-references**: FR-004, FR-012.
