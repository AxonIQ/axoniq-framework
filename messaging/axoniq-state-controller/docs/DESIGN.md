# State Controller — Design Rationale

*PoC design notes for `messaging/axoniq-state-controller`, branch `poc/state-controller`. Last updated
2026-07-04. Distills the branch review and the design discussion of 2026-06-18. This is not a decision record:
the module is a proof of concept, and everything here is input to that evaluation. When the module graduates,
the decisions that survived should be distilled into proper ADRs.*

## The two prototypes

Two designs for the State Controller's public surface were prototyped on separate branches, and this branch now
carries a synthesis of both. Understanding the synthesis requires understanding what each prototype got right.

**Lazy conditions** (`poc/state-controller`, original cut). Decisions declare `Condition`s over tag-scoped event
streams; nothing touches the store until a condition is forced, at which point every condition declared on the
scope is answered by **one** sourced read whose criteria narrow to exactly the tags and event types declared.
The criteria double as the decision's Dynamic Consistency Boundary (DCB), so the boundary is type-precise for
free. The cost was the surface: a yes/no question read as `.isA(X).not().isTrue()`, and a condition declared
after the first force failed at runtime with `LateConditionException` — an ordering ambush in an abstraction
meant to hide complexity.

**Eager `History`** (branch `poc/state-controller-bussnies-api`, described in its ADR-0001 *Business-first
decision API*). Decisions read plain values from an injected `History` — `has(...)` returns `boolean`,
`total(...)` returns `BigDecimal`, `latestOf(...)` returns the event for an `instanceof` pattern. The decision
body reads like the rulebook. The cost was hidden in the evaluation model: every value-returning read must query
*at that moment*, so the implementation either issues one blocking store round-trip per scope (what
`SourcedHistory` did — a transfer reads twice, sequentially) or loads the whole tag slice up front, discarding
the type-precision of the boundary. ADR-0001 itself lists the losses: no single-read batching across scopes, no
composition, tag-granular boundary by default. That branch also contributed a genuine safety improvement the
original cut lacked — a coverage guard closing the tagging-drift gap between read criteria and appended-event
tags — and its samples set the bar for what the decision code should read like.

## The axis the disagreement reduces to

**When is the read-set known?** The event types a decision touches are part of its DCB boundary, and each read
within one scope touches a different type-set. An API that hands back a plain value at the read site can only
know the types mentioned *so far* — the separation between *declaring* the questions and *consuming* the answers
is irreducible if the store is to be asked once, precisely. What is *not* irreducible is the ceremony: the
original surface's problem was that consumption happened through condition-typed evaluators (`.isTrue()`,
`.value()`, `OptionalCondition` unwrapping), not that conditions existed.

The sharpest concrete illustration came from the enrollment sample in the 2026-06-18 discussion — a single
*union* scope spanning two differently-tagged slices, with per-branch type restriction:

```java
History union = history.of("courseId", cmd.courseId())
                       .and(CourseCreated.class, CourseCapacityChanged.class,
                            StudentSubscribedToCourse.class, StudentUnsubscribedFromCourse.class)
                       .or("studentId", cmd.studentId())
                       .and(StudentEnrolledInFaculty.class);

var notEnrolled   = union.never(StudentEnrolledInFaculty.class);
var courseMissing = union.never(CourseCreated.class);
if (notEnrolled.resolve())   return reject("student not enrolled in faculty");
if (courseMissing.resolve()) return reject("course does not exist");
```

With eager, value-returning reads, each `never(...)` must materialize the union at the call site — two passes
over the combined stream. With declared conditions, both questions ride one pass. The per-branch type varargs
are also load-bearing here: when an event type could carry either tag, the declaration says which branch of the
union it belongs to — something a post-hoc `.filter()` cannot express against the store. The `and(types...)` /
`or(tagKey, value)` union builder is now on the unified `History`: each branch contributes one OR-term to the
sourced read's criteria (its tags × its declared types), a branch left unrestricted accumulates the types its
conditions read (the single-scope behavior), and a condition naming a type no branch declares fails fast with
`IllegalArgumentException` — the read could never contain it. The full enrollment decision runs end-to-end in
`sample/enrollment/CourseSubscriptions` against a real in-memory DCB store.

## The merged design

Keep the lazy engine and its guarantees; give it the business-first vocabulary and plain-value ergonomics. One
concept — `Condition` — spans both, with two terminals: `resolve()` for imperative bodies and `resolveAsync()`
for reactive ones.

### The programming model

A state-controlled handler is a **plain `@CommandHandler` method**. Its signature is the opt-in — a `History`
parameter and an `Outcome` (or `CompletableFuture<Outcome>`) return type. No dedicated annotation exists.

```java
@CommandHandler
public Outcome withdraw(Withdraw cmd, History history) {
    History account = history.of("account", cmd.accountId());

    var closed  = account.has(AccountClosed.class);                      // declares — no I/O
    var balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                         .minus(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));

    if (closed.resolve())                              return reject("account closed");   // ONE combined read
    if (balance.resolve().compareTo(cmd.amount()) < 0) return reject("insufficient funds"); // already resolved
    return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
}
```

The same declarations serve a fully non-blocking body — the reactive style is the same engine, not a parallel
API:

```java
@CommandHandler
public CompletableFuture<Outcome> register(RegisterAccount cmd, History history) {
    History account = history.of("account", cmd.accountId());
    var closed = account.has(AccountClosed.class);
    var exists = account.has(AccountOpened.class);

    return closed.combine(exists, (isClosed, doesExist) -> {
        if (isClosed)   return reject("account is closed");
        if (doesExist)  return reject("already exists");
        return accept(new AccountOpened(cmd.accountId())).returning("ACC-" + cmd.accountId());
    }).resolveAsync();
}
```

Cross-entity decisions narrow twice and stay one coordinated load:

```java
History from = history.of("account", cmd.fromAccountId());
History to   = history.of("account", cmd.toAccountId());
var fromClosed = from.has(AccountClosed.class);        // declared before the first resolve():
var toClosed   = to.has(AccountClosed.class);          // both scopes load together
```

### Core concepts

| Concept | Type | Role |
|---|---|---|
| The event history | `History` | Injected unbound; narrowed with `of(tagKey, tagValue)` — optionally restricted with `and(types...)` and extended into a union scope with `or(tagKey, tagValue)` — then read. Reads on the unbound root fail fast. |
| A declared question | `Condition<T>` (`BooleanCondition`, `NumericCondition`, `OptionalCondition`) | Returned by every `History` read. Lazy; composable via `map` / `combine` / `and` / `or` / `not` / `plus` / `minus`. |
| The terminals | `resolve()` / `resolveAsync()` | The only ways to obtain a value. `resolveAsync()` is the non-blocking primitive; `resolve()` joins it with a 30-second safety-net timeout (`FutureUtils.joinAndUnwrap`). |
| The result | `Outcome` | Sealed `Accept` / `Reject`; statically imported `accept(...)` / `reject(...)`, with `.returning(value)` and `.recording(events)`. |

The read vocabulary is ADR-0001's, unchanged: `has`, `never`, `count`, `total`, `totalLong`, `latest`,
`latestOf`, `first`.

### Evaluation model: declare, then resolve

1. Every `History` read (and every `EventStream` helper on the advanced tier) declares a lazy condition on the
   per-command loading session. Declaration performs no I/O.
2. The **first** `resolve()` / `resolveAsync()` anywhere in the decision seals **every scope declared so far**.
   Each scope issues one sourced read narrowed to its tags and the union of declared event types; the reads run
   concurrently on the shared `EventStoreTransaction`, which records a single `ConsistencyMarker` across them.
   Every declared condition then completes from those reads; subsequent resolutions are free.
3. On `accept(events)`, the framework appends conditionally on *(the union of read criteria, the recorded
   marker)*. Because the criteria are derived from the declarations, the boundary is **type-precise** without
   the developer listing types anywhere except the reads themselves.
4. Conditions declared **after** the first resolution do not fail. A scope lookup after its stream sealed mints
   a fresh stream; the late conditions are answered by a **supplementary read on the same transaction** —
   correct, one extra round-trip. This replaces `LateConditionException` as the user-facing policy (the
   exception survives only for resolving-then-reusing a *stale stream reference* on the advanced tier) and makes
   data-dependent reads possible, which neither prototype allowed. The hard seal's original purpose — preventing
   accidental re-streams when people make mistakes — is not dropped but relocated: the planned strict-mode
   diagnostic surfaces the mistake at development time instead of failing the command at runtime.

The idiom is therefore a convention, not a straitjacket: *declare your conditions first, then resolve*. Idiomatic
code gets exactly one round-trip per scope, concurrently; non-idiomatic code degrades gracefully and visibly
rather than failing or silently multiplying queries.

### DCB coverage guard

Adopted from the `bussnies-api` branch unchanged in substance: every sourced read records its folded
`EventCriteria` in `ReadBoundaries`, and on `Accept` the dispatch verifies each appended event — resolved through
the same `MessageTypeResolver` and `TagResolver` the append uses — falls inside at least one read boundary,
throwing `UncoveredEventException` (non-transient) otherwise. A decision that read nothing appends
unconditionally; rejection audit events are never guarded. This closes the drift gap where
`history.of("account", id)` reads one surface while `@EventTag` tags the appended event onto another, leaving
the optimistic lock guarding nothing.

### Naming

- **`Condition`** — the noun was rejected by ADR-0001 as part of the original surface's ceremony, but the
  ceremony (`.isTrue()` tax, evaluator types) was the problem, not the word. With plain-value terminals, "the
  decision resolves its conditions" is rulebook language. The known stretch — non-boolean reads like a balance —
  is accepted; no candidate covers both shapes better. **`Fact`** was tried and rejected: in event sourcing,
  facts are the *events*; naming the questions facts invites confusion with the very things they are asked
  about.
- **`Outcome`** over `Decision`, and no `@Decide`/`@StateController`/`@Action` — `Decision`/`Decide` evoke the
  decider pattern (inner-circle vocabulary both prototypes' goals reject), and the signature makes any
  annotation redundant. `@CommandHandler` is plain English, is honest about what the method is, and means
  everything users know about command handlers (discovery, routing, interceptors, testing) transfers verbatim.
  An independent strike against `@StateController` from the 2026-06-18 discussion: most Java developers'
  first association is Spring's `@Controller`, which may well be sitting a few lines above it in the same file.

### Two tiers, one engine

`History` is the documented surface; the expectation from the 2026-06-18 discussion is that ~99% of decisions
inject nothing else. The advanced tier — `DecisionContext` with `EventStream`'s `fold`, `latestMatch`, and
composite scopes — remains for cases the simple vocabulary omits, and is also where framework plumbing lives
when a decision genuinely needs it: access to the `ProcessingContext` and its resources, and the deterministic
`time()` clock. Both tiers share the same loading session (mixing them still batches into the same reads and
one marker) and return the same `Condition` types. The declarative `StateControllerComponent` registration path
routes through the same `Outcome` dispatch.

### Future direction: `History` beyond command handlers

`History` is deliberately shaped as a portable read abstraction, not a command-handling artifact — the
2026-06-18 discussion floated injecting it into other decision-making places, workflows in particular. The hard
requirement that surfaced there: **determinism under replay**. A workflow step that read a `History` must, on
re-execution, observe exactly the events it observed originally — not the slice as it exists now. Lazy declared
conditions fit that model naturally (a recorded read-set and position can be replayed; ad-hoc eager reads
cannot), which is an additional, forward-looking reason the lazy engine won. Nothing in the current module
implements this; it constrains the design rather than extending it.

## What this buys, and what it costs

Gains:

- Decisions read as plain Java over a named history — the `bussnies-api` branch's headline goal — while keeping
  the lazy model's guarantees: one type-precise coordinated load in idiomatic code, and a boundary derived from
  exactly what was read.
- One concept from the public reads to the engine. An interim two-layer design (a `Fact` wrapper over
  `Condition`) was built and then collapsed; `has(...)` returns the thing it always was.
- Reactive and imperative styles are the same declarations with different terminals; a handler switches by
  changing its return type.
- No new annotation to learn, document, or collide with.
- The ordering footgun is gone as a failure mode: late declarations cost a visible round-trip instead of a
  runtime exception.
- The coverage guard turns silent double-spend wiring errors into immediate, explained failures.
- Collapsing the surfaces surfaced and fixed a latent engine bug: four projection operators (`latest`/`first`
  payload extraction, `isA`/`isAnyOf`/`isNamed`, `as`, `MatchBuilder.orDefault`) eagerly forced the selection at
  declaration time, sealing scopes mid-declaration; all are now lazy `map` decorators.

Known costs and open edges:

- **Batching is idiomatic, not structural.** Nothing in the types forces declare-before-resolve; a body that
  interleaves reads and resolutions multiplies round-trips silently. Planned mitigation: a strict-mode
  diagnostic (test fixture fails / production metric warns when a decision needed more than one round-trip
  batch).
- **`resolve()` blocks.** One rendezvous per command, morally equivalent to an aggregate load, joined with a
  30-second safety-net timeout per the module's blocking rules — but still a blocked thread in the imperative
  style. The reactive style avoids it entirely.
- **Supplementary reads are transaction-consistent, not position-pinned.** They ride the shared
  `EventStoreTransaction` and its marker semantics; a strict "as of the first read's position P" pin (filtering
  late reads to entries ≤ P) is needed before the multi-read path can claim a single-snapshot guarantee under
  concurrent writers.
- **One `source(...)` per scope,** concurrent on one transaction, rather than a single OR-combined criteria
  statement per decision. Acceptable now (the reads overlap; the marker is shared); folding them into one store
  round-trip is a possible engine optimization that changes no API.
- Single-scope decisions still pay one `of(...)` line — accepted by ADR-0001 and unchanged here.

## Alternatives considered along the way

- **Eager `SourcedHistory` (the `bussnies-api` implementation).** Not carried forward: one blocking query per
  scope with no cross-scope batching, a tag-granular default boundary discarding type precision, and per-scope
  snapshots materialized at different moments while claiming reads "as of P". The *surface* survives; the eager
  engine does not.
- **`Fact<T>` as the public type over an internal `Condition`.** Built, then collapsed: "fact" collides with
  events-as-facts, and the two-layer design duplicated a concept the engine already had.
- **A `decide(conditions..., closure)` terminal** guaranteeing one read by shape. Deferred: expressible today as
  `combine(...).resolveAsync()`; the arity-overload surface can be added later if the closure form earns it.
- **`@Scope`-annotated `History` parameters** (framework prefetches declared scopes before the body runs).
  Deferred: it fixes the *tags* structurally but cannot know the *types*, which live in the body's reads — the
  axis this whole design turns on. May return later as a prefetch hint, not as the model.
- **Adaptive read-set prefetch** (remember each handler's historical (tag × type) read-set, speculatively source
  the union). Deferred: warm-up-dependent magic; only worth revisiting if the dynamic-read path proves common.
- **`resolveXxx(...)` shortcut methods on `History`** (e.g. `history.resolveLatestOf(...)` skipping the
  condition phase entirely — floated in the 2026-06-18 discussion alongside `.resolve()`). Dropped: the
  shortcuts double every read into an eager and a lazy variant, and each eager call is a hidden round-trip —
  exactly the cost model this design makes visible. One terminal, `latestOf(...).resolve()`, keeps the surface
  single and the batching idiom learnable.
- **A dedicated annotation** (`@StateController`, `@Decide`, `@Action`). Dropped: the `History` parameter plus
  `Outcome` return type carries strictly more information than any marker annotation, and reusing
  `@CommandHandler` keeps one mental model across the framework.

## Open questions for review

Points where pushback or a judgment call is specifically wanted before this hardens:

1. **Idiomatic vs. structural batching.** Is the convention (declare first, then resolve) plus a planned
   strict-mode diagnostic acceptable, or should the surface structurally guarantee one read (the `decide(...)`
   closure) at the cost of the free-form procedural style?
2. **`Outcome` vs. `Decision`.** With `@Decide` gone, does `Decision` still read as decider-speak, or is it the
   better noun after all?
3. **Ship the closure terminal now?** `combine(...).resolveAsync()` covers the declarative style, but a
   `decide(c1, c2, (v1, v2) -> ...)` family may be worth its arity overloads from day one.
4. **Position-pinning priority.** Is the supplementary-read consistency edge acceptable for the PoC evaluation,
   or does it need fixing before the design can be judged?

## Follow-ups

1. Strict-mode round-trip diagnostic (fixture assertion + production metric) for decisions exceeding one batch.
2. Position-pinning for supplementary reads (answer late conditions as of the first seal's position).
3. Port the remaining business samples from the `bussnies-api` branch (via-context variants, criteria and
   coverage suites) onto the unified surface. ~~Enrollment~~ — done: `sample/enrollment` carries the
   `CourseSubscriptions` decision plus an `AxonTestFixture` end-to-end suite. The union-scope builder
   (`and(types...)` / `or(tagKey, value)`, formerly follow-up 3) shipped with it.
4. Reference-guide documentation: the simple way (`History` + `resolve()`), then the advanced way
   (`DecisionContext`, folds, matchers, reactive composition).
