# SPI base contract

**Stability**: 5.2.0 GA. Generic over `Message` from day one so commands and queries (5.3+) join without an SPI break (Forward-compatibility invariant #1).

**Module**: `axoniq-framework/messaging/axoniq-message-transformation/`
**Package**: `io.axoniq.framework.messaging.transformation`

Shared SPI base for events (5.2.0) and -- when delivered -- commands and queries (5.3+). User-facing factory methods, builder usage, and end-to-end code samples are in [public-api.md](public-api.md); this file documents the SPI shape only.

## `MessageTransformer<M extends Message>`

Generic SPI base. Stream-in / stream-out: one transformer rewrites a `MessageStream<M>` into a new `MessageStream<M>` of the same `Message` subtype (`M` is preserved across the call).

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
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public interface MessageTransformer<M extends Message> {

    /**
     * Transform a single matched message. Called by the chain only when {@code message}
     * matches this transformer's {@code from}. The output stream MAY contain zero (drop),
     * one (1:1), or more (1:N) elements.
     *
     * @param message the matched input message
     * @param context the active processing context, or {@code null} if none
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

## `MessageTransformerChain`

One chain per application, holding all registered transformers across message types. Built once at startup; locked at `.build()`.

```java
package io.axoniq.framework.messaging.transformation;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.jspecify.annotations.NullMarked;

/**
 * Immutable chain of {@link MessageTransformer} instances applied at message read time.
 * Built once at startup via {@link Builder} and locked on {@link Builder#build()}.
 * Register the chain with the Axon configuration as a single component; the framework
 * installs the read-side decorators automatically.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public final class MessageTransformerChain {

    /**
     * Apply the chain to the given stream. Each input element is processed by fixed-point
     * iteration: walk the registrations in order, first match wins, restart on each match,
     * terminate when nothing matches. Non-matching elements pass through unchanged in
     * constant time (O(1) for concrete-{@code from}; O(P) when predicate-based {@code from}
     * transformations are registered).
     *
     * @param stream the input stream
     * @param <M>    the {@link Message} subtype of the stream
     * @return the transformed stream
     */
    public <M extends Message> MessageStream<? extends M> transform(MessageStream<M> stream) { /* ... */ }

    /**
     * Start building a new chain.
     *
     * @return a fresh {@link Builder}
     */
    public static Builder builder() { /* ... */ }

    /**
     * Fluent builder for {@link MessageTransformerChain}. Registration order = application
     * order. Calling {@link #build()} returns an immutable, locked chain.
     */
    public static final class Builder {

        /**
         * Register a transformer with the chain.
         *
         * @param transformer the transformer to add
         * @return this builder
         * @throws ChainConfigurationException if the chain is already locked, the transformer's
         *                                     {@code from} duplicates one already registered, or
         *                                     {@code from == to} (self-loop)
         */
        public Builder register(MessageTransformer<?> transformer) { /* ... */ }

        /**
         * Lock the chain and return an immutable instance. Runs multi-step cycle detection
         * on the graph of concrete-{@code MessageType} {@code from -> to} edges.
         *
         * @return the locked chain
         * @throws ChainConfigurationException on a multi-step cycle
         */
        public MessageTransformerChain build() { /* ... */ }
    }
}
```

**Contract**:
- **Startup-only registration** (FR-004). Late `register(...)` after `.build()` throws.
- **Fixed-point iteration** (FR-007): for each input message, walk the registrations in registration order; first match wins; on match, restart from the top with the output (1:1) or recurse per output (1:N); terminate when nothing matches (or on drop). Subsumes both same-name version chains and cross-name renames.
- **Hybrid lookup** (FR-011): transformations whose `from` is a concrete `MessageType` live in a `QualifiedName`-keyed map for O(1) non-matching lookup; transformations whose `from` is a `Predicate<MessageType>` live in a separate flat list, scanned linearly. Snapshots flow through unchanged (FR-005, no entries match).
- **Dispatch by message subtype `M`**: events flow only through `MessageTransformer<EventMessage>` entries, commands only through `MessageTransformer<CommandMessage>`, queries only through `MessageTransformer<QueryMessage>`.
- **Conflict detection** (FR-008): duplicate concrete `from` (registration time), self-loop (registration time), multi-step cycle on the concrete-`from -> to` graph (lock time). A defensive runtime safety bound guards against pathological infinite loops; under normal use it never fires.

**Cross-references**: FR-004, FR-005, FR-007, FR-008, FR-011, FR-013, FR-020, US1, US5, US6, US7.

---

## `ChainConfigurationException`

Thrown by `MessageTransformerChain.Builder` when FR-008 conflicts are detected. Runtime exceptions from inside a transformer propagate to the caller with full context (FR-015) -- no silent skip.

```java
package io.axoniq.framework.messaging.transformation;

import org.jspecify.annotations.NullMarked;

/**
 * Thrown by {@link MessageTransformerChain.Builder} on chain misconfiguration: duplicate
 * {@code from}, self-loop, multi-step cycle, version-order violation, or registration
 * after the chain has been locked. Runtime exceptions from inside a transformer's mapper
 * propagate to the caller directly; this type is reserved for configuration errors at
 * registration or lock time.
 *
 * @author AxonIQ
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
