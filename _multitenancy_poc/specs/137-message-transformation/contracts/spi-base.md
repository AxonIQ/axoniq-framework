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
 *
 * @param <M> the {@link Message} subtype this transformer accepts and emits
 * @since 5.2.0
 */
@NullMarked
public interface MessageTransformer<M extends Message> {

    /**
     * Apply this transformation. The output stream MAY contain zero, one, or more
     * elements per input element (Forward-compatibility invariant #2).
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

@NullMarked
public final class MessageTransformerChain {

    /** Apply the relevant sub-chain. Non-matching elements pass through in O(1). */
    public <M extends Message> MessageStream<M> transform(MessageStream<M> stream) { /* ... */ }

    public static Builder builder() { /* ... */ }

    public static final class Builder {

        /** Add a transformer. Throws if the chain is already locked (FR-004). */
        public Builder register(MessageTransformer<?> transformer) { /* ... */ }

        /** Optionally enforce version ordering at lock time (FR-020). */
        public Builder versionOrder(VersionComparator ordering) { /* ... */ }

        /** Switch chain-level logging on or off (FR-013). Default: enabled. */
        public Builder observability(Observability observability) { /* ... */ }

        /** Lock the chain. Runs FR-008 conflict detection and FR-020 ordering check. */
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

@NullMarked
public final class ChainConfigurationException extends RuntimeException {
    public ChainConfigurationException(String message) { super(message); }
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

@NullMarked
public interface VersionComparator extends Comparator<String> {

    /** Convenience factory returning a {@link SemverComparator}. */
    static VersionComparator semver() { /* ... */ }
}
```

```java
package io.axoniq.framework.messaging.transformation;

import org.jspecify.annotations.NullMarked;

/**
 * Semver (MAJOR.MINOR.PATCH) implementation. Non-parseable versions throw at `.build()`
 * lock -- no silent lexicographic fallback. Remediation: fix to semver, or supply a
 * custom {@link VersionComparator}.
 *
 * @since 5.2.0
 */
@NullMarked
public final class SemverComparator implements VersionComparator {

    /** Singleton instance. */
    public static SemverComparator instance() { /* ... */ }

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

@NullMarked
public sealed interface Observability {

    /** Default: DEBUG-at-startup + TRACE-per-applied. */
    static Observability enabled() { /* ... */ }

    /** No chain logging; no per-event allocation. */
    static Observability disabled() { /* ... */ }

    record Enabled() implements Observability {}
    record Disabled() implements Observability {}
}
```

**Cross-references**: FR-013, US7.
