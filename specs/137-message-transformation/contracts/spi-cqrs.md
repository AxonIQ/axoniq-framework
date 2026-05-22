# SPI commands and queries contract

**Stability**: 5.3+ (deferred from 5.2.0 per scope decision). Documented now so the [shared SPI base](spi-base.md) commits to the right shape from day one.

**Module**: `axoniq-framework/messaging/axoniq-message-transformation/`
**Package**: `io.axoniq.framework.messaging.transformation.cqrs`

Same module + same shared chain as events (no module split). Lives in a separate sub-package so it can be added in 5.3+ without touching event code. Covers US8 (commands) and US9 (queries). User-facing factories and end-to-end usage are in [public-api.md](public-api.md); event SPI is in [spi-events.md](spi-events.md); chain-level concerns are in [spi-base.md](spi-base.md).

## `CommandTransformer`

`MessageTransformer<CommandMessage>` specialization. 1:1 only (FR-019).

```java
package io.axoniq.framework.messaging.transformation.cqrs;

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.MessageStream;
import org.jspecify.annotations.NullMarked;

/**
 * Command-specific {@link MessageTransformer}. 1:1 only -- split / drop do not apply to
 * commands. Use the {@code CommandTransformation} factory rather than implementing directly.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
public interface CommandTransformer extends MessageTransformer<CommandMessage> {

    /**
     * Single-entry convenience overload; commands arrive one at a time.
     *
     * @param stream a single-element command stream
     * @return the transformed single-element stream
     */
    MessageStream.Single<CommandMessage> transform(MessageStream.Single<CommandMessage> stream);

    /** Adapts the base SPI to the single-entry overload. */
    @Override
    default MessageStream<CommandMessage> transform(MessageStream<CommandMessage> stream) {
        return transform(stream.first());
    }
}
```

**Contract** (in addition to base contract in [spi-base.md](spi-base.md), which already covers FR-018 output identity check):
- **1:1 only** (FR-019): `CommandTransformation` does not expose `split(...)` or `drop(...)`. The chain Builder also rejects any multi-output / zero-output `MessageTransformer<CommandMessage>` at `.build()` lock time, in case one is constructed via the SPI directly.

**Cross-references**: FR-018, FR-019, US8.

---

## `QueryTransformer`

Same shape as `CommandTransformer`, for queries. Subscription-query update streams flowing back to subscribers are NOT transformed -- only the incoming query is.

```java
package io.axoniq.framework.messaging.transformation.cqrs;

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.NullMarked;

/**
 * Query-specific {@link MessageTransformer}. 1:1 only. Subscription-query update streams
 * flowing back to subscribers are NOT transformed -- only the incoming query is. Use the
 * {@code QueryTransformation} factory rather than implementing directly.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
public interface QueryTransformer extends MessageTransformer<QueryMessage> {

    /**
     * Single-entry convenience overload; queries arrive one at a time.
     *
     * @param stream a single-element query stream
     * @return the transformed single-element stream
     */
    MessageStream.Single<QueryMessage> transform(MessageStream.Single<QueryMessage> stream);

    /** Adapts the base SPI to the single-entry overload. */
    @Override
    default MessageStream<QueryMessage> transform(MessageStream<QueryMessage> stream) {
        return transform(stream.first());
    }
}
```

**Cross-references**: FR-018, FR-019, US9.

---

## `TransformingCommandBus` (integration type, 5.3+, internal)

Decorator on `CommandBus` that wraps every registered `CommandHandler` at subscription time. The unifying point is the registered handler, so the chain fires on every incoming command -- whether it arrived locally or via a `CommandBusConnector` from a remote node. Annotation-based subscriptions also go through `CommandBus.subscribe(...)` (via `AnnotatedCommandHandlingComponent.registerHandler(...)`), so no path is missed.

```java
package io.axoniq.framework.messaging.transformation.cqrs;

import io.axoniq.framework.messaging.transformation.MessageTransformerChain;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.NullMarked;

/**
 * {@link CommandBus} decorator that wraps each registered {@link CommandHandler} at
 * subscription time so the chain fires on every incoming command -- local or remote.
 * Installed automatically by {@code CqrsTransformationConfigurationEnhancer}; not
 * constructed by users. Outbound dispatch is not decorated.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
@Internal
public final class TransformingCommandBus implements CommandBus {

    /** Decoration order: outer (later) than {@code InterceptingCommandBus}. */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 110;

    /**
     * @param delegate the inner {@link CommandBus} to wrap
     * @param chain    the application's {@link MessageTransformerChain}
     */
    public TransformingCommandBus(CommandBus delegate, MessageTransformerChain chain) { /* ... */ }

    /** Wraps {@code handler} so the chain runs before the delegate handler. */
    @Override
    public TransformingCommandBus subscribe(QualifiedName name, CommandHandler handler) {
        // wraps handler so chain.transform fires before delegate.handle
    }

    /* dispatch and other methods delegate unchanged (outbound transformation out of scope). */
}
```

The outbound `CommandBus.dispatch(...)` path is NOT decorated (sender-side transformation is out of scope per Part C of spec).

**Cross-references**: FR-019, FR-021, US8.

---

## `TransformingQueryBus` (integration type, 5.3+, internal)

Mirror of `TransformingCommandBus` for queries.

```java
package io.axoniq.framework.messaging.transformation.cqrs;

import io.axoniq.framework.messaging.transformation.MessageTransformerChain;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.jspecify.annotations.NullMarked;

/**
 * Mirror of {@link TransformingCommandBus} for queries: {@link QueryBus} decorator wrapping
 * each registered {@link QueryHandler} at subscription time. Installed automatically by
 * {@code CqrsTransformationConfigurationEnhancer}; not constructed by users.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
@Internal
public final class TransformingQueryBus implements QueryBus {

    /** Decoration order aligned with {@link TransformingCommandBus#DECORATION_ORDER}. */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 110;

    /**
     * @param delegate the inner {@link QueryBus} to wrap
     * @param chain    the application's {@link MessageTransformerChain}
     */
    public TransformingQueryBus(QueryBus delegate, MessageTransformerChain chain) { /* ... */ }

    /** Wraps {@code handler} so the chain runs before the delegate handler. */
    @Override
    public TransformingQueryBus subscribe(QualifiedName name, QueryHandler handler) {
        // wraps handler so chain.transform fires before delegate.handle
    }

    /* other methods delegate unchanged. */
}
```

**Cross-references**: FR-019, US9.

---

## `CqrsTransformationConfigurationEnhancer` (5.3+)

Registers `TransformingCommandBus` and `TransformingQueryBus` as decorators, sharing the same `MessageTransformerChain` instance that `EventTransformationConfigurationEnhancer` already exposes.

```java
package io.axoniq.framework.messaging.transformation.cqrs.configuration;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NullMarked;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that installs
 * {@link TransformingCommandBus} and {@link TransformingQueryBus} decorators, sharing the
 * same chain instance as the event-side enhancer. A no-op if no chain is registered.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
public final class CqrsTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) { /* ... */ }
}
```

**Cross-references**: FR-019.
