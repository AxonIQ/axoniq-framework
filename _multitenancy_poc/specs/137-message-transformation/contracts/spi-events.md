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
 * Event-specific {@link MessageTransformer}. Use the {@code EventTransformation} factory
 * rather than implementing directly. The event envelope (entity type, entity identifier,
 * tracking token, sequence number) is preserved across transformation; snapshots flow
 * through unchanged.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public interface EventTransformer extends MessageTransformer<EventMessage> {

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
 * One output of a 1:N split mapper. The framework wraps each into an
 * {@link org.axonframework.messaging.eventhandling.EventMessage}; all N outputs inherit
 * the input event's envelope (tracking token, sequence number, entity identity).
 *
 * @param type    identity of the replacement event
 * @param payload new payload; its runtime type drives downstream conversion
 * @author AxonIQ
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
 * {@link EventStore} decorator that applies a {@link MessageTransformerChain} to every read
 * path (entity loads, DCB reads, tracking-processor reads). Installed automatically by
 * {@code EventTransformationConfigurationEnhancer}; not constructed by users.
 *
 * @author AxonIQ
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
     * @param delegate the inner {@link EventStore} to wrap
     * @param chain    the application's {@link MessageTransformerChain}
     */
    public TransformingEventStore(EventStore delegate, MessageTransformerChain chain) { /* ... */ }

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
 * {@link #source(SourcingCondition)} only; append / position methods delegate unchanged
 * because the chain runs at read time.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
@Internal
final class TransformingEventStoreTransaction implements EventStoreTransaction {

    /**
     * @param delegate the inner {@link EventStoreTransaction} to wrap
     * @param chain    the application's {@link MessageTransformerChain}
     */
    TransformingEventStoreTransaction(EventStoreTransaction delegate, MessageTransformerChain chain) { /* ... */ }

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

ServiceLoader-discovered enhancer that wires `TransformingEventStore` as a decorator on `EventStore` via `ComponentRegistry.registerDecorator(...)` and reads the user-supplied `MessageTransformerChain` from the component registry.

```java
package io.axoniq.framework.messaging.transformation.events.configuration;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NullMarked;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that installs the
 * {@link TransformingEventStore} decorator. Reads the user-supplied
 * {@link MessageTransformerChain} from the component registry; a no-op if none is registered.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public final class EventTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) { /* ... */ }
}
```

**Cross-references**: FR-004, FR-012.
