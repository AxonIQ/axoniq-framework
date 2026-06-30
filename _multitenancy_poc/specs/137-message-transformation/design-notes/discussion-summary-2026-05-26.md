# Framework Design Support Group -- Meeting Summary

**Date**: 2026-05-26
**Attendees**: Allard Buijze, Jakob Hatzl, Laura Devriendt, Jan Galinski, Mateusz Nowak, Simon Zambrovski, Stefan Dragisic
**Apologies**: Sean (holiday), Steven (not present)
**Duration**: ~1h44m


## 3.  Message Transformation public API

Recap: chain decorates `EventStore` (5.2.0) and later `CommandBus` / `QueryBus` (5.3+). Receive-side only. Architecture allows future commands/queries without an SPI break.

### 3.1 Version matching on `from(...)`

Question: should `from(...)` always take a concrete `MessageType` (i.e. exact `qualifiedName + version` match), or should it support ranges?

**Decision**: **overload `from(...)`**.
- Simple form: `from(MessageType)` -- exact equals match (default behaviour).
- Predicate form: `from(Predicate<MessageType>)` -- equivalent to AF4's `canUpcast`. Users get full flexibility (calendar versioning, alphabetical, etc.).
- Built-in helpers planned: a `SemverComparator` so users can express `1.x`, `>=1.2.0 <2.0.0`, etc. Notation aligned with the SemVer spec (same syntax used by vulnerability databases / npm). Jakob to look at whether a Java SemVer library is suitable to depend on.

**Rationale**: hard-coding `from(MessageType)` would be *less* flexible than AF4 (which already had `canUpcast`). Real-world projects need ranges -- Jakob confirmed a previous project would have been blocked without it. Message versions often track Maven versions (patch releases that don't actually change the schema), so requiring a separate `register(...)` per micro-version is impractical.

### 3.2 Ordering & overlap

- **No auto-detection of overlap in v1.** Registration order = apply order. If multiple transformers match the same message, the first registered wins. Users responsible for ordering.
- Warnings on detectable overlap are nice-to-have, not required.
- **Chain re-evaluation strategy**: after a transformer fires, the new message is re-fed to the chain (not just to the next entry). This handles `v1 -> v2 -> v3` chains naturally and means ordering only matters when ranges genuinely overlap on the same input version. Cost is a few extra cheap `from` checks per event.

### 3.3 `when(...)` and `onApplied(...)` hooks

**Deferred** -- not in the first version. Users who need conditional skipping can do it inside the `transform` lambda and return the original payload. hooks can be a separate concern

### 3.4 Naming -- `MessageTransformer` vs. `Upcaster`

Keep **`MessageTransformer`**.
- Avoids future naming conflict if we add downcasting.
- Distances users from AF4 `Upcaster` mental model (the AF5 design is meaningfully different).
- Opens the door to reusing the same SPI for **Axon Server's event-store transformation** (one-to-one or one-to-zero rewrites of persisted events). If we can use the same `MessageTransformer` shape for that, big win. If not, the naming overlap becomes a problem -- but the architectural commitment is small enough to take the bet now.
- Also leaves room for snapshot transformation (Johns generic snapshot event message).

### 3.5 Serialization-format-aware transformations (Simon's old pain)

Out of scope for this issue. When binary format (JSON / XML / ...) needs to factor into transformer selection, that should be addressed as a **separate issue** (likely via `MessageType` carrying mime-type info, but separate work).

### 3.6 The big one -- SPI signature: `MessageStream` in vs. single message in

Original plan: `MessageStream<M> -> MessageStream<M>`. Justification was flexibility to support both 1:N and N:1.

**Decision**: change to **`(M message, ProcessingContext context) -> MessageStream<M>`**.

Reasoning:
- We already decided last week we will **never** support N:1 (stateful join across messages). That was a bad-idea-from-AF4 we don't want to reintroduce -- if users need it, they build a stateful event-handler component, not a transformer.
- 1:N is still supported because the return type is `MessageStream<M>`.
- Stream-in forces users to iterate / handle `hasNext` themselves, which contradicts the rest of AF5's handler API (handlers, interceptors all take a single message + context and return a stream).
- `ProcessingContext` lets transformers reach context-aware resources (tenant ID, cached defaults, etc.) -- per Jan, and consistent with Mitchell's "every new heavy method should take a context" guidance from this morning.
- For the server-side Axon Server event-store transformation use case, the framework can enforce "stream out must be empty or singleton" at the call site.

### 3.7 Transformer interface vs. inline lambda

The `transform(Class<T>, BiFunction)` step is the lambda form -- but there should also be a way to **implement the `MessageTransformer<M>` interface directly** (no registration DSL needed) so users can write their own class. That interface must take `(M, ProcessingContext)`.

### 3.8 What stays for 5.2.0 (re-confirmed)

- **MUST**: 1:1 transform (US1), rename (US2).
- **MAY**: split (US3), drop (US4), chaining (US5), conflict detection (US6).
- **DEFERRED**: `when`/`onApplied` (US7), commands (US8), queries (US9), snapshot transform.

---

## 4. Action items

| Owner | Action |
|---|---|
| Laura | Rework public-API contract: overload `from(...)` (concrete + predicate); change SPI to `(M, ProcessingContext) -> MessageStream<M>`; drop `when` / `onApplied` for v1; keep `MessageTransformer` naming. |
| Laura | Verify SemVer Java library candidates; decide whether to ship `SemverComparator` in 5.2.0. |
| Laura | When the rework is ready, post in the framework Slack channel to schedule a focused follow-up (don't need to wait for the next Thursday design session). |

---

## 5. References

- Pre-meeting cheat sheet: [discussion-26-05-26.md](general-design-discussion.md)
- Spec: [spec.md](../spec.md)
- Plan: [plan.md](../plan.md)
- Contracts: [public-api.md](../contracts/public-api.md), [spi-base.md](../contracts/spi-base.md), [spi-events.md](../contracts/spi-events.md), [spi-commands-queries.md](../contracts/spi-commands-queries.md)
