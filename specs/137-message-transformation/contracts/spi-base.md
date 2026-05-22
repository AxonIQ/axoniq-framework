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
import org.jspecify.annotations.NullMarked;

/**
 * Base SPI for message transformations. The element type {@code M} is preserved -- a
 * transformer does not change a command into an event. Per element it MAY change the
 * {@link org.axonframework.messaging.core.MessageType} identity (rename / version bump),
 * the payload's Java type or structure, and the cardinality (1:N split / 1:0 drop;
 * commands and queries are 1:1 only, FR-019).
 * <p>
 * Users almost never implement this interface directly; the typed factories in
 * {@code io.axoniq.framework.messaging.transformation.events.EventTransformation} (and the
 * deferred {@code CommandTransformation} / {@code QueryTransformation} for 5.3+) produce
 * {@link MessageTransformer} instances for the common patterns. See
 * {@code contracts/public-api.md} for the user-facing usage.
 *
 * @param <M> the {@link Message} subtype this transformer accepts and emits
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public interface MessageTransformer<M extends Message> {

    /**
     * Apply this transformation to the given stream. The output stream MAY contain zero, one,
     * or more elements per input element (Forward-compatibility invariant #2). Non-matching
     * elements MUST pass through unchanged (FR-005) with no payload conversion (FR-011).
     *
     * @param stream the input stream of messages to transform
     * @return a stream of transformed messages of the same {@link Message} subtype
     */
    MessageStream<M> transform(MessageStream<M> stream);
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
 * Immutable chain of registered {@link MessageTransformer} instances grouped by source
 * {@link org.axonframework.messaging.core.QualifiedName} (FR-007). One chain per application,
 * built once at startup via {@link Builder} and locked on {@link Builder#build()} (FR-004).
 * <p>
 * The chain is registered with the Axon configuration as a single component; the
 * {@code EventTransformationConfigurationEnhancer} (5.2.0) and -- when delivered -- the
 * {@code CqrsTransformationConfigurationEnhancer} (5.3+) discover it and install the
 * decorators that invoke {@link #transform(MessageStream)} on every read / receive path.
 * See {@code contracts/public-api.md} for end-to-end usage.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public final class MessageTransformerChain {

    /**
     * Apply the relevant per-{@link org.axonframework.messaging.core.QualifiedName} sub-chain
     * to the given stream. Events whose {@link org.axonframework.messaging.core.QualifiedName}
     * matches no registered transformer skip the chain entirely in {@code O(1)} with no
     * per-event allocation (FR-005, FR-011). Output elements whose
     * {@link org.axonframework.messaging.core.QualifiedName} differs from the input's
     * re-enter routing at the output's sub-chain (FR-007).
     *
     * @param stream the input stream of messages to route through the chain
     * @param <M>    the {@link Message} subtype of the stream
     * @return a stream of transformed messages of the same subtype
     */
    public <M extends Message> MessageStream<M> transform(MessageStream<M> stream) { /* ... */ }

    /**
     * Start building a new chain.
     *
     * @return a fresh {@link Builder}
     */
    public static Builder builder() { /* ... */ }

    /**
     * Fluent builder for {@link MessageTransformerChain}. Collects transformers, enforces
     * registration order = application order (FR-004), and on {@link #build()} runs the
     * applicable conflict (FR-008) and version-order (FR-020) checks before returning
     * an immutable chain.
     */
    public static final class Builder {

        /**
         * Register a transformer with the chain. Registration order is preserved as
         * application order within each per-{@link org.axonframework.messaging.core.QualifiedName}
         * sub-chain (FR-004).
         *
         * @param transformer the {@link MessageTransformer} to add to the chain
         * @return this builder, for chaining
         * @throws ChainConfigurationException if the chain is already locked, if the
         *                                     transformer's {@code from} duplicates an already-registered
         *                                     {@code from}, or if {@code from == to} (self-loop) -- per FR-008
         */
        public Builder register(MessageTransformer<?> transformer) { /* ... */ }

        /**
         * Optionally enforce version ordering at lock time (FR-020). When set, the comparator
         * orders transformers within each sub-chain by {@code from.version()}; mismatches with
         * registration order surface at {@link #build()} as a {@link ChainConfigurationException}.
         * When omitted (the default), registration order alone determines apply order.
         *
         * @param ordering the {@link VersionComparator} to apply, or {@code null} to clear
         * @return this builder, for chaining
         */
        public Builder versionOrder(VersionComparator ordering) { /* ... */ }

        /**
         * Switch chain-level logging on or off (FR-013). Default is {@link Observability#enabled()};
         * use {@link Observability#disabled()} on performance-critical paths to suppress all
         * per-event allocation from the observability hook (Forward-compatibility invariant #9).
         *
         * @param observability the {@link Observability} mode to apply
         * @return this builder, for chaining
         */
        public Builder observability(Observability observability) { /* ... */ }

        /**
         * Lock the chain and return an immutable instance. Runs FR-008 multi-step cycle detection
         * and -- if a {@link VersionComparator} was registered via {@link #versionOrder(VersionComparator)}
         * -- the FR-020 ordering check.
         *
         * @return the locked, immutable {@link MessageTransformerChain}
         * @throws ChainConfigurationException if a multi-step cycle is detected (FR-008) or the
         *                                     registered transformers violate the version ordering (FR-020)
         */
        public MessageTransformerChain build() { /* ... */ }
    }
}
```

**Contract**:
- **Startup-only registration** (FR-004). Late `register(...)` after `.build()` throws.
- **Per-`QualifiedName` sub-chains** (FR-007). Registered transformers are grouped by `from.qualifiedName()`. Within a sub-chain, registration order = application order.
- **Dispatch by message subtype `M`**: events flow only through `MessageTransformer<EventMessage>` entries, commands only through `MessageTransformer<CommandMessage>`, queries only through `MessageTransformer<QueryMessage>`.
- **O(1) non-matching path** with no per-event allocation (FR-011, JMH-verified). Unknown `MessageType`s pass through (FR-005); this also makes snapshots automatically pass-through (Forward-compat invariant #6).
- **Re-entry on `QualifiedName` change** (FR-007): an output whose `QualifiedName` differs from its input's re-enters routing at the OUTPUT's sub-chain. Covers 1:N splits AND 1:1 cross-name renames / structural transforms, e.g. `CourseOpened@1.0.0 -> CourseCreated@1.0.0 -> CourseCreated@2.0.0 -> CourseCreated@3.0.0`. Same-name 1:1 hops (pure version bumps) continue in the current sub-chain.
- **Conflict detection (FR-008) and observability (FR-013)** call sites MUST exist from day one even when not implemented in 5.2.0 (Forward-compat invariants #8, #9).

**Cross-references**: FR-004, FR-005, FR-007, FR-008, FR-011, FR-013, FR-020, US1, US5, US6, US7.

---

## `ChainConfigurationException`

Thrown by `MessageTransformerChain.Builder` when FR-008 conflicts are detected. Runtime exceptions from inside a transformer propagate to the caller with full context (FR-015) -- no silent skip.

```java
package io.axoniq.framework.messaging.transformation;

import org.jspecify.annotations.NullMarked;

/**
 * Thrown by {@link MessageTransformerChain.Builder} when an FR-008 conflict is detected
 * (duplicate {@code from}, self-loop, multi-step cycle) or when the FR-020 version-order
 * check fails. Also thrown on registration after the chain has been locked (FR-004).
 * Runtime exceptions raised from inside a transformer's mapper propagate to the caller
 * with full context per FR-015; this exception type is reserved for chain-configuration
 * errors detected at registration or lock time.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public final class ChainConfigurationException extends RuntimeException {

    /**
     * @param message human-readable description of the chain configuration problem
     */
    public ChainConfigurationException(String message) { super(message); }

    /**
     * @param message human-readable description of the chain configuration problem
     * @param cause   the underlying cause, may be {@code null}
     */
    public ChainConfigurationException(String message, Throwable cause) { super(message, cause); }
}
```

Detection points:

| Conflict (FR-008) | When detected |
|---|---|
| duplicate `from` | `register(...)` |
| self-loop (`from == to`) | `register(...)` |
| multi-step cycle | `.build()` (lists full edge chain) |
| version-order violation | `.build()` (only when a `VersionComparator` is set) |

---

## `VersionComparator` + `SemverComparator`

Optional `Comparator<String>` that enforces version ordering at `.build()` lock time. Default: no comparator, registration order alone determines apply order (matches AF4 semantics).

```java
package io.axoniq.framework.messaging.transformation;

import java.util.Comparator;
import org.jspecify.annotations.NullMarked;

/**
 * Optional {@link Comparator} over version strings used by
 * {@link MessageTransformerChain.Builder#versionOrder(VersionComparator)} to enforce
 * version ordering at lock time (FR-020). Without one, registration order alone determines
 * apply order. AF4 compatibility allows arbitrary version strings, so the comparator is
 * pluggable; {@link SemverComparator} ships as a convenience for the common case.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public interface VersionComparator extends Comparator<String> {

    /**
     * Convenience factory returning the {@link SemverComparator} singleton.
     *
     * @return a {@link VersionComparator} that orders versions by MAJOR.MINOR.PATCH semver
     */
    static VersionComparator semver() { /* ... */ }
}
```

```java
package io.axoniq.framework.messaging.transformation;

import org.jspecify.annotations.NullMarked;

/**
 * Semver (MAJOR.MINOR.PATCH) implementation of {@link VersionComparator}. Non-parseable
 * versions raise a {@link ChainConfigurationException} at
 * {@link MessageTransformerChain.Builder#build()} -- no silent lexicographic fallback.
 * Remediation: fix the version to semver, or supply a custom {@link VersionComparator}
 * via {@link MessageTransformerChain.Builder#versionOrder(VersionComparator)}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public final class SemverComparator implements VersionComparator {

    /**
     * Return the shared singleton instance.
     *
     * @return the {@link SemverComparator} singleton
     */
    public static SemverComparator instance() { /* ... */ }

    /**
     * Compare two semver-formatted version strings by MAJOR, then MINOR, then PATCH.
     *
     * @param version1 the first version string (e.g. {@code "1.0.0"})
     * @param version2 the second version string (e.g. {@code "2.0.0"})
     * @return a negative integer, zero, or a positive integer as {@code version1} is less than,
     *         equal to, or greater than {@code version2}
     * @throws ChainConfigurationException if either argument is not a valid semver string
     */
    @Override
    public int compare(String version1, String version2) { /* ... */ }
}
```

**Cross-references**: FR-020, US5.

---

## `Observability`

Chain-level logging switch supplied to `Builder.observability(...)`. When disabled, the chain MUST NOT allocate per event (Forward-compatibility invariant #9). User-facing usage and sample DEBUG output are in [public-api.md](public-api.md).

```java
package io.axoniq.framework.messaging.transformation;

import org.jspecify.annotations.NullMarked;

/**
 * Chain-level logging switch supplied to
 * {@link MessageTransformerChain.Builder#observability(Observability)}. The chain emits a
 * single DEBUG entry at lock time listing all registered transformers, and one TRACE entry
 * per applied transformation (FR-013). When {@link #disabled()}, the chain MUST NOT allocate
 * per event from the observability hook (Forward-compatibility invariant #9).
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@NullMarked
public sealed interface Observability {

    /**
     * Default mode: DEBUG once at startup + TRACE per applied transformation.
     *
     * @return an {@link Enabled} instance
     */
    static Observability enabled() { /* ... */ }

    /**
     * Suppress all chain-level logging. No per-event allocation from the observability hook.
     *
     * @return a {@link Disabled} instance
     */
    static Observability disabled() { /* ... */ }

    /** Marker record indicating chain-level logging is enabled. */
    record Enabled() implements Observability {}

    /** Marker record indicating chain-level logging is suppressed. */
    record Disabled() implements Observability {}
}
```

**Cross-references**: FR-013, US7.
