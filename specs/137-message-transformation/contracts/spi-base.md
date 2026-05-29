# SPI base contract

**Stability**: 5.2.0 GA. Generic over `Message` from day one so commands and queries (5.3+) join without an SPI break (Forward-compatibility invariant #1).

**Module**: `axoniq-framework/messaging/axoniq-message-transformation/`
**Package**: `io.axoniq.framework.messaging.transformation`

Shared SPI base for events (5.2.0) and -- when delivered -- commands and queries (5.3+). User-facing factory methods, builder usage, and end-to-end code samples are in [public-api.md](public-api.md); this file documents the SPI shape only.

## `MessageTransformer<M extends Message>`

Generic SPI base. Single-message input, stream output: each transformer emits zero elements (drop), one (1:1), or N (1:N split) for one matched input message. `M` is preserved across the call -- a transformer does not turn a command into an event. Each message type has its own typed chain that composes many transformers into a stream-in / stream-out pipeline: `EventTransformerChain` ([spi-events.md](spi-events.md), 5.2.0) and the deferred `CommandTransformerChain` / `QueryTransformerChain` ([spi-commands-queries.md](spi-commands-queries.md), 5.3+). This per-transformer SPI is intentionally the simpler shape so user code stays a plain `BiFunction`-equivalent returning a `MessageStream`.

```java
package io.axoniq.framework.messaging.transformation;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Base SPI for message transformations. The element type {@code M} is preserved -- a
 * transformer does not turn a command into an event. Per call it MAY change the
 * {@link org.axonframework.messaging.core.MessageType} identity, the payload's Java type
 * or structure, and the cardinality (1:N split / 1:0 drop for events; commands and
 * queries are 1:1 only).
 * <p>
 * Most users do not implement this directly; use the typed factory
 * {@code EventTransformation} (plus {@code CommandTransformation} / {@code QueryTransformation}
 * in 5.3+).
 *
 * @param <M> the {@link Message} subtype this transformer accepts and emits
 * @author Laura Devriendt
 * @since 5.2.0
 */
@FunctionalInterface
@NullMarked
public interface MessageTransformer<M extends Message> {

    /**
     * Transform a single matched message. Called by the chain only when {@code message}
     * matches this transformer's {@code from}. The output stream MAY contain zero (drop),
     * one (1:1), or more (1:N) elements.
     *
     * @param message the matched input message
     * @param context the active processing context. Non-null on the entity-load read path
     *                ({@code EventStore.transaction(ctx)}); MAY be {@code null} on the
     *                tracking-processor read path
     *                ({@code EventStore.open(StreamingCondition, @Nullable ProcessingContext)})
     *                when the caller passes {@code null}. Implementations MUST tolerate
     *                {@code null}.
     * @return the resulting output stream
     */
    MessageStream<? extends M> transform(M message, @Nullable ProcessingContext context);
}
```

**Contract**:
- **Deterministic + thread-safe** (FR-006): no external services, no time/randomness, no mutable shared state. Constant in-process data is fine. The framework MAY invoke `transform` concurrently. Not enforced at runtime.
- **Non-matching elements pass through unchanged** (FR-005) with no payload conversion (FR-011).
- **Output identity check** (FR-018): for any 1:1 transformer that supplies a payload mapper, the framework MUST verify after invocation that the output's resolved `MessageType` matches the declared `to`. A mismatch raises a clear error under FR-015 with full context (declared `to`, actual output identity, stream position). Pure renames (no payload mapper) satisfy this trivially because the framework sets the output identity itself. Does NOT apply to 1:N / 1:0 transformers (events only, FR-003): output identities there are mapper-determined by design.
- Users almost never implement this directly -- they use the typed factories declared in [public-api.md](public-api.md).

**Cross-references**: FR-001, FR-005, FR-006, FR-011, FR-018, FR-019, US1.

---

## Chains are per message type

Each message type has its own typed chain holding only its transformers. There is NO shared `MessageTransformerChain` -- separation is intentional (transformers do not overlap across message types: you never register the same instance against both an event and a command). The typed chains are defined in their respective files:

- `EventTransformerChain` -- [spi-events.md](spi-events.md), ships 5.2.0
- `CommandTransformerChain` -- [spi-commands-queries.md](spi-commands-queries.md), 5.3+
- `QueryTransformerChain` -- [spi-commands-queries.md](spi-commands-queries.md), 5.3+

All three share the same shape: a `builder()` that returns an immutable, locked chain on `build()`; one `register(...)` overload; one `transform(MessageStream<MessageSubtype>) -> MessageStream<? extends MessageSubtype>` method. They differ only in the bound `M`. Behaviour (FR-004 startup-only registration, FR-007 fixed-point iteration with last-match-wins, FR-008 conflict detection, FR-011 hybrid lookup) is identical across all three -- documented once per chain in its file, not duplicated here.

---

## `ChainConfigurationException`

Shared across all typed chains. Thrown by any chain's `Builder` when FR-008 conflicts are detected. Runtime exceptions from inside a transformer propagate to the caller with full context (FR-015) -- no silent skip.

```java
package io.axoniq.framework.messaging.transformation;

import org.jspecify.annotations.NullMarked;

/**
 * Thrown by {@code EventTransformerChain.Builder} (and the future
 * {@code CommandTransformerChain.Builder} / {@code QueryTransformerChain.Builder}) on chain
 * misconfiguration: duplicate {@code from}, self-loop, multi-step cycle, version-order
 * violation, or registration after the chain has been locked. Runtime exceptions from inside
 * a transformer's mapper propagate to the caller directly; this type is reserved for
 * configuration errors at registration or lock time.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@NullMarked
public final class ChainConfigurationException extends RuntimeException {

    /**
     * @param message human-readable description of the problem
     */
    public ChainConfigurationException(String message) { super(message); }

    /**
     * @param message human-readable description of the problem
     * @param cause   the underlying cause, may be {@code null}
     */
    public ChainConfigurationException(String message, Throwable cause) { super(message, cause); }
}
```

Detection points:

| Conflict (FR-008) | When detected |
|---|---|
| duplicate concrete `from` | `register(...)` |
| self-loop on concrete `from == to` | `register(...)` |
| multi-step cycle on concrete `from -> to` graph | `.build()` (lists full edge chain) |
| (defensive) runtime safety bound on chain iteration | runtime, only on pathological misconfiguration |

---

## Version-range matching

Version ranges (e.g. semver `1.x`, `>=1.0 <2.0`) are expressed by passing a `Predicate<MessageType>` to `from(...)` rather than a concrete `MessageType`. The framework ships no semver helper in 5.2.0; users compose their own predicates or pull in the `SemverPredicate` helper expected as follow-on work in `axon-common` (see [plan.md](plan.md) "Required axon-framework additions"). Registration order = apply order; no auto-detection of overlapping predicates.

**Cross-references**: FR-020.

---

## Per-transformer hooks (deferred)

`.when(Predicate<M>)` (skip if `false`) + `.onApplied(BiConsumer<M, MessageStream<? extends M>>)` (post-apply observer) on the base SPI -- matching AF4 `SingleEntryUpcaster.canUpcast` / `doUpcast` -- is deferred entirely. NOT in 5.2.0. The common "skip this transformer for this input" case is already covered by FR-005's predicate-based `from`. When the hook pair lands, it does so as a pure additive change: no chain-wide hooks on the Builder, no generic framework-owned name like `Observability`.

**Cross-references**: FR-013, US7.
