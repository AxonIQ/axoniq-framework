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
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.NullMarked;

/**
 * Event-specific specialization of {@link MessageTransformer}. Produced by the
 * {@code EventTransformation} factory; users almost never implement this interface directly.
 * The base SPI contract from {@link MessageTransformer} applies, plus the event-specific
 * envelope-preservation contract (FR-010) and snapshot pass-through behaviour described in
 * this contract document.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public interface EventTransformer extends MessageTransformer<EventMessage> {

    /**
     * {@inheritDoc}
     */
    @Override
    MessageStream<EventMessage> transform(MessageStream<EventMessage> stream);
}
```

**Event-specific contract** (in addition to base contract in [spi-base.md](spi-base.md)):
- **Envelope preservation** (FR-010): entity type, entity identifier, tracking token, and sequence number MUST be carried unchanged from input to output. Framework overrides any attempt to modify them. For 1:N splits, every replacement inherits the input's envelope (no renumbering -- all N share the input's tracking token + sequence number). Metadata MAY be modified via the message-level entry point.
- **Snapshot pass-through**: snapshots flow inline through the same `EventStore.transaction().source(...)` stream; FR-005 unknown-`MessageType` pass-through (base contract) covers them automatically. Snapshot transformation API is deferred to 5.3+ (Forward-compatibility invariant #6).

**Cross-references**: FR-001 to FR-003, FR-010, US1 to US5.

---

## `TransformedEvent`

Output type for 1:N split mappers. The framework wraps each into an `EventMessage`, preserving the input event's envelope per FR-010.

```java
package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.messaging.core.MessageType;
import org.jspecify.annotations.NullMarked;

/**
 * One output of a 1:N split mapper. The framework wraps each {@link TransformedEvent} into
 * an {@link org.axonframework.messaging.eventhandling.EventMessage}, preserving the input
 * event's envelope per FR-010 (entity type, entity identifier, tracking token, sequence
 * number all carried unchanged from input to output; all N outputs share the input's
 * envelope -- no renumbering).
 *
 * @param type    identity ({@link MessageType}) of the replacement event
 * @param payload new payload; runtime type drives downstream conversion via the registered
 *                {@code Converter} (FR-009)
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public record TransformedEvent(MessageType type, Object payload) {

    /**
     * Convenience factory equivalent to {@code new TransformedEvent(type, payload)}.
     *
     * @param type    identity ({@link MessageType}) of the replacement event
     * @param payload new payload
     * @return a {@link TransformedEvent} carrying the supplied type and payload
     */
    public static TransformedEvent of(MessageType type, Object payload) {
        return new TransformedEvent(type, payload);
    }
}
```

**Cross-references**: FR-003, FR-009, FR-010, US3.

---

## `TransformingEventStore` (integration type, internal)

Decorator on `EventStore` registered by `EventTransformationConfigurationEnhancer`. Follows the `InterceptingEventStore` blueprint in axon-framework: decorator on `EventStore` + wrapping `EventStoreTransaction` cached per `ProcessingContext`.

```java
package io.axoniq.framework.messaging.transformation.events;

import io.axoniq.framework.messaging.transformation.MessageTransformerChain;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Decorator on {@link EventStore} that applies a {@link MessageTransformerChain} to every
 * read path: entity loads, DCB reads, and tracking-processor reads (FR-012). Marked
 * {@link Internal} because the user never instantiates it directly; the
 * {@code EventTransformationConfigurationEnhancer} registers it as a decorator via
 * {@code ComponentRegistry.registerDecorator(...)} using {@link #DECORATION_ORDER}.
 * <p>
 * Follows the {@code InterceptingEventStore} blueprint in axon-framework: decorator on
 * {@link EventStore} + a wrapping {@link EventStoreTransaction} cached per
 * {@link ProcessingContext}. The decorator runs at READ only (FR-021); append / publish /
 * token paths delegate unchanged. Tracking-token progress through drops surfaces correctly
 * (FR-014).
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
@Internal
public final class TransformingEventStore implements EventStore {

    /**
     * Decorator ordering: outer (later) than {@code InterceptingEventStore} so the chain
     * sits closest to the consumer and any handler-side data-protection interceptor runs
     * downstream of the transformed shape (FR-021).
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 100;

    /**
     * Construct the decorator. Internal use only; produced by
     * {@code EventTransformationConfigurationEnhancer} via {@code ComponentRegistry}.
     *
     * @param delegate the inner {@link EventStore} to wrap
     * @param chain    the application's {@link MessageTransformerChain}
     */
    public TransformingEventStore(EventStore delegate, MessageTransformerChain chain) { /* ... */ }

    /**
     * {@inheritDoc}
     * <p>
     * Returns a {@link TransformingEventStoreTransaction} wrapping the delegate's transaction,
     * cached per {@link ProcessingContext} via {@code Context.ResourceKey}. The wrapping
     * transaction's {@code source(...)} pipes through {@code chain.transform(...)}.
     */
    @Override
    public EventStoreTransaction transaction(ProcessingContext processingContext) { /* ... */ }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the inner stream piped through {@code chain.transform(...)}. Used by
     * tracking-processor reads.
     */
    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition,
                                            @Nullable ProcessingContext context) { /* ... */ }

    /* publish, firstToken, latestToken, tokenAt, subscribe, describeTo delegate
       unchanged to the inner EventStore -- they do not touch event payloads. */
}
```

**Behaviour**:
- `open(...)`: returns the inner stream piped through `chain.transform(stream)`. Tracking-processor reads.
- `transaction(...)`: returns a `TransformingEventStoreTransaction` wrapping the delegate, cached per `ProcessingContext` via `Context.ResourceKey`. Entity loads + DCB reads.
- Everything else (`publish`, tokens, `subscribe`): delegated unchanged. Chain runs at READ only (FR-021).
- `SourcingCondition` / `StreamingCondition` filtering + `ConsistencyMarker` / `TerminalEventMessage` bookkeeping happen in the underlying engine BEFORE the chain receives the stream.
- **Tracking-token progress through drops** (FR-014): when a transformation drops an event, the underlying stream's tracking-token progress MUST still surface to the consumer (the chain only removes the element from the emitted output; it does NOT discard the position). A restarting tracking processor therefore resumes AFTER the dropped event and does not reprocess it.
- **Data-protection ordering** (FR-021): `DECORATION_ORDER` keeps `TransformingEventStore` outside any handler-side data-protection interceptor, so protection runs on the transformed shape the handler receives.

**Cross-references**: FR-010, FR-011, FR-012, FR-014, FR-021, US1 scenario 4, US4 scenario 3.

---

## `TransformingEventStoreTransaction` (integration type, internal)

```java
package io.axoniq.framework.messaging.transformation.events;

import io.axoniq.framework.messaging.transformation.MessageTransformerChain;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.NullMarked;

/**
 * Wrapping {@link EventStoreTransaction} returned by
 * {@link TransformingEventStore#transaction(ProcessingContext)}. Applies the chain to
 * {@link #source(SourcingCondition)} (entity loads + DCB reads); append / position methods
 * delegate unchanged because the chain runs at READ only (FR-021). Package-private:
 * instances are produced by {@link TransformingEventStore} and never exposed directly.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
@Internal
final class TransformingEventStoreTransaction implements EventStoreTransaction {

    /**
     * Construct the wrapping transaction.
     *
     * @param delegate the inner {@link EventStoreTransaction} to wrap
     * @param chain    the application's {@link MessageTransformerChain}
     */
    TransformingEventStoreTransaction(EventStoreTransaction delegate, MessageTransformerChain chain) { /* ... */ }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the delegate's stream piped through {@code chain.transform(...)}.
     */
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

ServiceLoader-discovered enhancer that wires `TransformingEventStore` as a decorator on `EventStore` via `ComponentRegistry.registerDecorator(...)` and reads the user-supplied `MessageTransformerChain` from the component registry.

```java
package io.axoniq.framework.messaging.transformation.events.configuration;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NullMarked;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that wires
 * {@code TransformingEventStore} as a decorator on the framework's {@code EventStore}
 * via {@link ComponentRegistry#registerDecorator}. Reads the user-supplied
 * {@code MessageTransformerChain} from the component registry; if no chain is registered,
 * the enhancer is a no-op so applications without transformations pay no overhead.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public final class EventTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * {@inheritDoc}
     */
    @Override
    public void enhance(ComponentRegistry registry) { /* ... */ }
}
```

**Cross-references**: FR-004, FR-012.
