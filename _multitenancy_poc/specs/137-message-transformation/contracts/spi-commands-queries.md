# SPI commands and queries contract

**Stability**: 5.3+ (deferred from 5.2.0 per scope decision). Documented now so the [shared SPI base](spi-base.md) commits to the right shape from day one.

**Module**: `axoniq-framework/messaging/axoniq-message-transformation/`
**Packages**: `io.axoniq.framework.messaging.transformation.commandhandling`, `io.axoniq.framework.messaging.transformation.queryhandling`

Two sub-packages mirroring the `axoniq-distributed-messaging` convention. Commands and queries share the same `MessageTransformerChain` instance as events (one chain per application) but live in their own sub-packages and are installed by their own enhancers, so neither layer pulls the other in. Covers US8 (commands) and US9 (queries). User-facing factories and end-to-end usage are in [public-api.md](public-api.md); event SPI is in [spi-events.md](spi-events.md); chain-level concerns are in [spi-base.md](spi-base.md).

## `CommandTransformer`

`MessageTransformer<CommandMessage>` specialization. 1:1 only (FR-019).

```java
package io.axoniq.framework.messaging.transformation.commandhandling;

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Command-specific {@link MessageTransformer}. 1:1 only -- split / drop do not apply to
 * commands. Output is always a {@link MessageStream.Single}. Use the
 * {@code CommandTransformation} factory rather than implementing directly.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
public interface CommandTransformer extends MessageTransformer<CommandMessage> {

    @Override
    MessageStream.Single<? extends CommandMessage> transform(CommandMessage message, @Nullable ProcessingContext context);
}
```

**Contract** (in addition to base contract in [spi-base.md](spi-base.md), which already covers FR-018 output identity check):
- **1:1 only** (FR-019): `CommandTransformation` does not expose `split(...)` or `drop(...)`. The chain Builder also rejects any multi-output / zero-output `MessageTransformer<CommandMessage>` at `.build()` lock time, in case one is constructed via the SPI directly.

**Cross-references**: FR-018, FR-019, US8.

---

## `QueryTransformer`

Same shape as `CommandTransformer`, for queries. Subscription-query update streams flowing back to subscribers are NOT transformed -- only the incoming query is.

```java
package io.axoniq.framework.messaging.transformation.queryhandling;

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Query-specific {@link MessageTransformer}. 1:1 only. Subscription-query update streams
 * flowing back to subscribers are NOT transformed -- only the incoming query is. Output is
 * always a {@link MessageStream.Single}. Use the {@code QueryTransformation} factory rather
 * than implementing directly.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
public interface QueryTransformer extends MessageTransformer<QueryMessage> {

    @Override
    MessageStream.Single<? extends QueryMessage> transform(QueryMessage message, @Nullable ProcessingContext context);
}
```

**Cross-references**: FR-018, FR-019, US9.

### Query response transformation (deferred)

Queries are bidirectional: a `QueryMessage` request flows to a handler, and a `MessageStream<QueryResponseMessage>` response flows back to the caller (verified at `QueryBus.java:69`). The current scope covers the **request side only** -- `QueryTransformer extends MessageTransformer<QueryMessage>` above. A `QueryResponseTransformer extends MessageTransformer<QueryResponseMessage>` is **architecturally compatible** with no SPI change: `QueryResponseMessage extends ResultMessage extends Message` (verified at `messaging/core/ResultMessage.java:31`, `messaging/queryhandling/QueryResponseMessage.java:32`) so it slots into the existing generic `MessageTransformer<M extends Message>` base.

**Why deferred**: receiver-side reading of an old-vs-new response IS read-time transformation (fits this work's read-only invariant, FR-021), distinct from sender-side new-to-old downcasting (out of scope per spec Part C). It is deferred from the first cut to keep the 5.3+ slice focused on request-side coverage; the decoration point would be `TransformingQueryBus.query(...)` piping the returned `MessageStream<QueryResponseMessage>` through a parallel chain lookup. No 5.2.0 wiring change is needed.

See also spec Part C "Query response transformation `[Deferred]`".

---

## `TransformingCommandBus` (integration type, 5.3+, internal)

Decorator on `CommandBus` (NOT on `CommandBusConnector`) that wraps every registered `CommandHandler` at subscription time. Decorating the bus rather than the connector matters: a user can drop in transformation without taking a dependency on a distributed-messaging module, and tests can exercise transformation without going over the wire. The chain fires on every incoming command -- whether local or remote, since both paths flow through `CommandBus.subscribe(...)` (annotation-based subscriptions go through it via `AnnotatedCommandHandlingComponent.registerHandler(...)`).

```java
package io.axoniq.framework.messaging.transformation.commandhandling;

import io.axoniq.framework.messaging.transformation.MessageTransformerChain;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.core.QualifiedName;
import org.jspecify.annotations.NullMarked;

/**
 * {@link CommandBus} decorator that wraps each registered {@link CommandHandler} at
 * subscription time so the chain fires on every incoming command -- local or remote.
 * Installed automatically by {@code CommandTransformationConfigurationEnhancer}; not
 * constructed by users. Outbound dispatch is not decorated.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
@Internal
public final class TransformingCommandBus implements CommandBus {

    /**
     * Decoration order: outer (later) than {@code InterceptingCommandBus} (which uses
     * {@code MIN_VALUE + 100}). The {@code +1000} offset leaves headroom for users or
     * framework to slot other decorators in between.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 1000;

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

Mirror of `TransformingCommandBus` for queries -- decorates `QueryBus`, not the connector.

```java
package io.axoniq.framework.messaging.transformation.queryhandling;

import io.axoniq.framework.messaging.transformation.MessageTransformerChain;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.jspecify.annotations.NullMarked;

/**
 * Mirror of {@link TransformingCommandBus} for queries: {@link QueryBus} decorator wrapping
 * each registered {@link QueryHandler} at subscription time. Installed automatically by
 * {@code QueryTransformationConfigurationEnhancer}; not constructed by users.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
@Internal
public final class TransformingQueryBus implements QueryBus {

    /** Decoration order aligned with {@link TransformingCommandBus#DECORATION_ORDER}. */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 1000;

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

## `CommandTransformationConfigurationEnhancer` + `QueryTransformationConfigurationEnhancer` (5.3+)

Two separate enhancers, one per sub-package, each installing its own bus decorator. They share the same `MessageTransformerChain` component that `EventTransformationConfigurationEnhancer` looks up.

```java
package io.axoniq.framework.messaging.transformation.commandhandling.configuration;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NullMarked;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that installs the
 * {@link io.axoniq.framework.messaging.transformation.commandhandling.TransformingCommandBus}
 * decorator. Reads the user-supplied {@code MessageTransformerChain} from the component
 * registry; a no-op if none is registered.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
public final class CommandTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) { /* ... */ }
}
```

```java
package io.axoniq.framework.messaging.transformation.queryhandling.configuration;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NullMarked;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that installs the
 * {@link io.axoniq.framework.messaging.transformation.queryhandling.TransformingQueryBus}
 * decorator. Reads the user-supplied {@code MessageTransformerChain} from the component
 * registry; a no-op if none is registered.
 *
 * @author AxonIQ
 * @since 5.3+
 */
@NullMarked
public final class QueryTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) { /* ... */ }
}
```

**Cross-references**: FR-019.
