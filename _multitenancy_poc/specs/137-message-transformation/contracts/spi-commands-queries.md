# SPI commands and queries contract

**Stability**: 5.3+ (deferred from 5.2.0 per scope decision). Documented now so the [shared SPI base](spi-base.md) commits to the right shape from day one.

**Module**: `axoniq-framework/messaging/axoniq-message-transformation/`
**Packages**: `io.axoniq.framework.messaging.transformation.commandhandling`, `io.axoniq.framework.messaging.transformation.queryhandling`

Two sub-packages mirroring the `axoniq-distributed-messaging` convention. Each message type has its own typed chain (`CommandTransformerChain`, `QueryTransformerChain`) and its own enhancer, so neither layer pulls the other in. Covers US8 (commands) and US9 (queries). User-facing factories and end-to-end usage are in [public-api.md](public-api.md); event SPI is in [spi-events.md](spi-events.md); shared SPI base + chain-separation rationale in [spi-base.md](spi-base.md).

## `CommandTransformer`

`MessageTransformer<CommandMessage>` specialization. 1:1 only (FR-019).

```java
package io.axoniq.framework.messaging.transformation.commandhandling;

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.MessageStream;
import org.jspecify.annotations.NullMarked;

/**
 * Command-specific {@link MessageTransformer}: a sealed handle, 1:1 only -- split / drop do
 * not apply to commands. Output is always a {@link MessageStream.Single}. Use the
 * {@code CommandTransformation} factory rather than implementing directly.
 *
 * @author Laura Devriendt
 * @since 5.3+
 */
@NullMarked
public interface CommandTransformer extends MessageTransformer<CommandMessage> {

    @Override
    @Internal
    MessageStream.Single<? extends CommandMessage> transform(CommandMessage message, TransformationContext context);
}
```

**Contract** (in addition to base contract in [spi-base.md](spi-base.md), which already covers FR-018 output identity check):
- **1:1 only** (FR-019): `CommandTransformation` does not expose `split(...)` or `drop(...)`. The chain Builder also rejects any multi-output / zero-output `MessageTransformer<CommandMessage>` at `.build()` lock time, in case one is constructed via the SPI directly.

**Cross-references**: FR-018, FR-019, US8.

### Command result transformation (deferred)

Parallel to the query response case below, for commands. `CommandResultMessage extends
ResultMessage extends Message` (verified at `messaging/commandhandling/CommandResultMessage.java`,
`messaging/core/ResultMessage.java`), so a future
`CommandResultTransformer extends MessageTransformer<CommandResultMessage>` slots into the
generic base with no SPI change. Decoration point: `TransformingCommandBus.dispatch(...)`
(verified at `messaging/commandhandling/CommandBus.java`) piping the returned
`CompletableFuture<CommandResultMessage>` through a parallel chain lookup.

See spec Part C "Command result transformation `[Deferred]`" for the deferral rationale.

---

## `CommandTransformerChain`

Public, immutable chain of `CommandTransformer` instances. Same shape as `EventTransformerChain` ([spi-events.md](spi-events.md)) typed to `CommandMessage`, plus the single-message overload required by bus wrapping (commands arrive one at a time at `CommandBus.subscribe(...)`-time wrappers, not as streams).

```java
package io.axoniq.framework.messaging.transformation.commandhandling;

import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Immutable chain of {@link CommandTransformer} instances. Built once at startup; register
 * with the Axon configuration as a {@code CommandTransformerChain.class}-typed component.
 *
 * @author Laura Devriendt
 * @since 5.3+
 */
@NullMarked
public final class CommandTransformerChain {

    /**
     * Stream-in / stream-out; used in tests or if a future caller exposes commands as a stream.
     * Mirrors the event chain's shape: the converter + resolver are supplied by
     * {@code TransformingCommandBus} (resolved by the enhancer), not by the user.
     */
    public MessageStream<? extends CommandMessage> transform(MessageStream<CommandMessage> stream,
                                                             @Nullable ProcessingContext context,
                                                             MessageConverter converter,
                                                             MessageTypeResolver messageTypeResolver) { /* ... */ }

    /**
     * Single-message entry point used by {@code TransformingCommandBus}'s subscribe-time
     * wrapping. Output is {@link MessageStream.Single} because commands are 1:1 only (FR-019).
     * The {@link TransformationContext} carries the active processing context plus the converter
     * and resolver.
     */
    public MessageStream.Single<? extends CommandMessage> transform(CommandMessage message,
                                                                     TransformationContext context) { /* ... */ }

    public static Builder builder() { /* ... */ }

    public static final class Builder {
        public Builder register(CommandTransformer transformer) { /* ... */ }
        public CommandTransformerChain build() { /* ... */ }
    }
}
```

Behaviour (FR-004 startup-only, FR-007 fixed-point iteration with last-match-wins, FR-008 conflicts, FR-011 hybrid lookup) is shared across the three typed chains; see [spi-base.md](spi-base.md).

---

## `QueryTransformer`

Same shape as `CommandTransformer`, for queries. Subscription-query update streams flowing back to subscribers are NOT transformed -- only the incoming query is.

```java
package io.axoniq.framework.messaging.transformation.queryhandling;

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.NullMarked;

/**
 * Query-specific {@link MessageTransformer}: a sealed handle, 1:1 only. Subscription-query
 * update streams flowing back to subscribers are NOT transformed -- only the incoming query is.
 * Output is always a {@link MessageStream.Single}. Use the {@code QueryTransformation} factory
 * rather than implementing directly.
 *
 * @author Laura Devriendt
 * @since 5.3+
 */
@NullMarked
public interface QueryTransformer extends MessageTransformer<QueryMessage> {

    @Override
    @Internal
    MessageStream.Single<? extends QueryMessage> transform(QueryMessage message, TransformationContext context);
}
```

**Cross-references**: FR-018, FR-019, US9.

### Query response transformation (deferred)

Queries are bidirectional: a `QueryMessage` request flows to a handler, and a `MessageStream<QueryResponseMessage>` response flows back to the caller (verified at `QueryBus.java:69`). The current scope covers the **request side only** -- `QueryTransformer extends MessageTransformer<QueryMessage>` above. A `QueryResponseTransformer extends MessageTransformer<QueryResponseMessage>` is **architecturally compatible** with no SPI change: `QueryResponseMessage extends ResultMessage extends Message` (verified at `messaging/core/ResultMessage.java:31`, `messaging/queryhandling/QueryResponseMessage.java:32`) so it slots into the existing generic `MessageTransformer<M extends Message>` base.

**Why deferred**: receiver-side reading of an old-vs-new response IS read-time transformation (fits this work's read-only invariant, FR-021), distinct from sender-side new-to-old downcasting (out of scope per spec Part C). It is deferred from the first cut to keep the 5.3+ slice focused on request-side coverage; the decoration point would be `TransformingQueryBus.query(...)` piping the returned `MessageStream<QueryResponseMessage>` through a parallel chain lookup. No 5.2.0 wiring change is needed.

See also spec Part C "Query response transformation `[Deferred]`" and the parallel
"Command result transformation (deferred)" under `CommandTransformer` above.

---

## `QueryTransformerChain`

Parallel to `CommandTransformerChain` above, typed to `QueryMessage`. Same shape -- stream entry point + the single-message overload required by `QueryBus.subscribe(...)`-time wrapping. Same shared behaviour clauses (FR-004 / FR-007 last-match / FR-008 / FR-011). Omitted here to avoid repetition; see [spi-base.md](spi-base.md) and the `CommandTransformerChain` block above.

---

## `TransformingCommandBus` (integration type, 5.3+, internal)

Decorator on `CommandBus` (NOT on `CommandBusConnector`) that wraps every registered `CommandHandler` at subscription time. Decorating the bus rather than the connector matters: a user can drop in transformation without taking a dependency on a distributed-messaging module, and tests can exercise transformation without going over the wire. The chain fires on every incoming command -- whether local or remote, since both paths flow through `CommandBus.subscribe(...)` (annotation-based subscriptions go through it via `AnnotatedCommandHandlingComponent.registerHandler(...)`).

```java
package io.axoniq.framework.messaging.transformation.commandhandling;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.jspecify.annotations.NullMarked;

/**
 * {@link CommandBus} decorator that wraps each registered {@link CommandHandler} at
 * subscription time so the chain fires on every incoming command -- local or remote.
 * Installed automatically by {@code CommandTransformationConfigurationEnhancer}; not
 * constructed by users. Outbound dispatch is not decorated.
 *
 * @author Laura Devriendt
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
     * @param delegate            the inner {@link CommandBus} to wrap
     * @param chain               the application's {@link CommandTransformerChain} (passive registry)
     * @param converter           the active {@link MessageConverter}
     * @param messageTypeResolver the active {@link MessageTypeResolver} for the FR-018 output-identity
     *                            check; both resolved from {@code Configuration} by the enhancer at
     *                            decorator-registration time
     */
    public TransformingCommandBus(CommandBus delegate,
                                   CommandTransformerChain chain,
                                   MessageConverter converter,
                                   MessageTypeResolver messageTypeResolver) { /* ... */ }

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

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.jspecify.annotations.NullMarked;

/**
 * Mirror of {@link TransformingCommandBus} for queries: {@link QueryBus} decorator wrapping
 * each registered {@link QueryHandler} at subscription time. Installed automatically by
 * {@code QueryTransformationConfigurationEnhancer}; not constructed by users.
 *
 * @author Laura Devriendt
 * @since 5.3+
 */
@NullMarked
@Internal
public final class TransformingQueryBus implements QueryBus {

    /** Decoration order aligned with {@link TransformingCommandBus#DECORATION_ORDER}. */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 1000;

    /**
     * @param delegate            the inner {@link QueryBus} to wrap
     * @param chain               the application's {@link QueryTransformerChain}
     * @param converter           the active {@link MessageConverter}
     * @param messageTypeResolver the active {@link MessageTypeResolver}; same resolution pattern as
     *                            {@link TransformingCommandBus}
     */
    public TransformingQueryBus(QueryBus delegate,
                                 QueryTransformerChain chain,
                                 MessageConverter converter,
                                 MessageTypeResolver messageTypeResolver) { /* ... */ }

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

Two separate enhancers, one per sub-package, each installing its own bus decorator and looking up its own typed chain component (`CommandTransformerChain.class` / `QueryTransformerChain.class`) -- independent of `EventTransformerChain`.

```java
package io.axoniq.framework.messaging.transformation.commandhandling.configuration;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NullMarked;

/**
 * ServiceLoader-discovered {@link ConfigurationEnhancer} that installs the
 * {@link io.axoniq.framework.messaging.transformation.commandhandling.TransformingCommandBus}
 * decorator. Reads the user-supplied {@code CommandTransformerChain}, the active
 * {@code MessageConverter}, and the active {@code MessageTypeResolver} from the component
 * registry at decorator-registration time (same pattern as
 * {@code EventTransformationConfigurationEnhancer}, see [spi-events.md](spi-events.md)); a
 * no-op if no chain is registered.
 *
 * @author Laura Devriendt
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
 * Mirror of {@code CommandTransformationConfigurationEnhancer} for queries: reads
 * {@code QueryTransformerChain} + {@code MessageConverter} + {@code MessageTypeResolver} and
 * installs the
 * {@link io.axoniq.framework.messaging.transformation.queryhandling.TransformingQueryBus}
 * decorator. No-op if no chain is registered.
 *
 * @author Laura Devriendt
 * @since 5.3+
 */
@NullMarked
public final class QueryTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) { /* ... */ }
}
```

**Cross-references**: FR-019.
