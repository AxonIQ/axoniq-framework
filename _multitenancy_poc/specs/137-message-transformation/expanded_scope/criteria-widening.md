# Read-Time Criteria Widening for Event Transformations

## Problem

A user that combines a transformation chain (Phase 3 1:1 transforms or Phase 4
renames) with a type-filtering `@EventCriteriaBuilder` silently gets incomplete
entities. Events stored under the transformer's `from` qualified name are
rejected by the storage layer's type filter before the chain can rename them.

This is **not rename-specific**. Any 1:1 transformer with a different `from`
and `to` has the same issue.

## Why it works at all

The type filter is applied against whatever criteria we hand the storage
engine. Widening the criteria at our wrapper boundary makes the filter
broader, regardless of where it physically runs (in-memory after fetch for
JPA and in-memory engines verified today, SQL pushdown or remote filtering
for AxonServer or future backends). The engine returns the additional events;
our chain transforms them.

### Where we sit in the call chain

```
1. Repository.load(courseId)
         |
         v
2. @EventCriteriaBuilder produces EventCriteria          (inert object)
         |
         v
3. SimpleEntityLifecycleHandler wraps it in SourcingCondition
         |
         v
4. eventStore.transaction(pc).source(condition)
         |   eventStore = TransformingEventStore (outermost decorator)
         v
5. TransformingEventStoreTransaction.source(condition)   *** INTERCEPT HERE ***
         |   widen condition.criteria()
         v
6. delegate.source(widenedCondition)
         |
         v
7. DefaultEventStoreTransaction.source(widenedCondition)
         |
         v
8. eventStorageEngine.source(widenedCondition)
         |   engine = JPA / in-memory / AxonServer-backed
         v
9. Engine applies the filter using widenedCondition.criteria()
         |   JPA: SQL fetch + in-memory match
         |   In-memory: scan with match
         |   AxonServer: gRPC request, server-side filter
         v
10. Filtered stream returns upward
         |
         v
11. chain.transform(stream)                              renames V1 -> V2
         |
         v
12. Repository receives transformed events
```

Interception at step 5 sits upstream of every backend's filter (step 9).
Widening runs once per `source(...)` or `open(...)` call; the stream that the
delegate returns keeps using the widened criteria for its full lifetime.

## Proposed solution

Intercept at our wrapper boundary in two methods:

- `TransformingEventStoreTransaction.source(SourcingCondition, ...)`
- `TransformingEventStore.open(StreamingCondition)`

Both rebuild the condition with a widened criteria before delegating. One
helper, two call sites.

### Hybrid widening per transformer

| Matcher | Contribution |
|---|---|
| `Concrete(from)` | add `from` to type filter |
| `PredicateBased(predicate, declaredFromTypes)` | add `declaredFromTypes` |
| `PredicateBased(predicate, emptySet)` | drop the type filter for that criterion |

Transitive renames (`V1 -> V2 -> V3`) are resolved to fixed point.

### API addition

```java
EventTransformation.from(predicate)
                   .declaringFromTypes(LegacyA, LegacyB)
                   .to(CourseCreatedV2)
                   .transform(...);
```

`from(predicate)` without `declaringFromTypes(...)` falls back to drop-type-filter.

Internally: extend `FromMatcher.PredicateBased` with a `Set<QualifiedName>` field.
No new permit, no second switch site.

### Predicate evaluation scope with declared `from` types

When a predicate-based transformer also declares `from` types via
`declaringFromTypes(...)`, those declared types act as a pre-filter: the
predicate is **only** evaluated against events whose type is one of the declared
`from` types. An event that is not one of the declared `from` types is never
passed to the predicate.

This behavior must be stated **explicitly** in the documentation. It is the
expected reading ("you declared it should be one of those `from` types, so the
predicate never runs against anything else"), but it is not self-evident from
the API and needs to be called out.

Without declared `from` types, the predicate falls back to drop-type-filter and
is evaluated against the widened stream as described above.

## Verified risks

| Risk | Status |
|---|---|
| `PooledStreamingEventProcessor` uses outermost decorator | cleared (`DecorationOrderTest`) |
| Snapshot path still passes through chain | cleared (chain wraps `delegate.source()` stream) |
| Spring Boot autoconfig installs the wrapper | cleared (ServiceLoader path) |
| `SourcingCondition.strategy()` accessible for rebuild | cleared (interface default) |

## Alternatives

| Option | Pros | Cons |
|---|---|---|
| Document only | zero work | silent footgun; scales badly |
| `@EvolvedFrom` annotation on events | declarative | two sources of truth; no fit for predicates |
| Decorate `CriteriaResolver` only | clean | misses streaming and programmatic paths |
| Drop type filter unconditionally | simplest | over-fetches even when not needed |
| Sample-based predicate learning | no user burden | fragile, non-deterministic |

## Proposal

1. **Builder order.** Place `declaringFromTypes(...)` between `from(predicate)`
   and `.to(...)`. Reads as "from predicate, declaring these source types,
   to V2". Groups source-side information together.
2. **No-chain behavior.** Keep current behavior: when no `EventTransformerChain`
   is registered, the wrapper does not install and widening does not run.
   Zero-cost for non-users.
3. **Diagnostics.** Extend `EventTransformerChain.describeTo(...)` with the
   widening graph: per declared `to`, list the union of `from` qualified
   names plus a flag indicating whether any predicate-based contribution
   drops the type filter. Surfaces in `AxonConfiguration.describe(...)` and
   Spring Boot Actuator endpoints for operator debugging.
4. **Documentation.** Both: Javadoc on the factory methods and a dedicated
   reference-guide page "How event transformations interact with
   `@EventCriteriaBuilder`". Javadoc covers the API; the guide covers the
   conceptual model. Both must state explicitly that declared `from` types
   pre-filter the predicate: the predicate is only evaluated against events
   whose type is one of the declared `from` types, never against anything else
   (see "Predicate evaluation scope with declared `from` types" above).
5. **Upstream the criteria-rewrite primitive (done).** Widening a criteria means
   rewriting each flattened `EventCriterion` (adding source types, or dropping
   the type filter) and recombining the results. Originally `widen()` did this by
   hand — flatten -> edit -> rebuild via `EventCriteria.either(...)` — *assuming*
   the round-trip preserved meaning, with no first-class API expressing that
   intent. Both tiers of upstream hardening are now implemented in scope:

   a. **Round-trip guarantee test (`EventCriteriaFlattenRoundTripTest`).** Pins
      that, for a criteria that has criteria, `flatten()` plus rebuild via the
      public factory methods matches exactly the same events. It also pins the
      one exception — a match-anything criteria flattens to nothing, which
      `either(...)` cannot reconstruct — which is precisely why the rewrite
      primitive short-circuits an empty flatten. This guards our module (and any
      future module doing the same trick) against a silent change to `flatten()`
      / `either()` semantics.

   b. **First-class `EventCriteria.mapCriteria(Function<EventCriterion,
      EventCriteria>)`.** A default method on `EventCriteria` that owns the
      flatten + `either(...)` round-trip internally, returning the *same instance*
      when it flattens to no criterion or the mapper changes nothing. `widen()`
      collapses to `criteria.mapCriteria(this::widenCriterion)`: the
      flatten/rebuild plumbing lives upstream once instead of being
      re-implemented per module, the round-trip is correct by construction
      (covered by `EventCriteriaMapCriteriaTest`), and the zero-cost
      short-circuit the read decorators rely on (`widen()` returning the same
      instance when nothing broadens) is preserved by `mapCriteria` returning
      `this` on a no-op.

   This is a public method on a *sealed* interface in the OSS framework — every
   `permits` implementation inherits the default and every consumer sees the new
   API — so it was a deliberate, framework-maintainer-facing decision rather than
   a silent addition; that is why it is recorded here and carried into `plan.md`
   and `spec.md` rather than landing unannounced.

   The widening *algorithm* (the backward-reachability graph over the
   transformer chain) stays in this module regardless; only the criteria-rewrite
   primitive moved upstream.

