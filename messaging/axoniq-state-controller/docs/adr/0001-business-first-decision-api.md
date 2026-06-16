# ADR-0001: Business-first decision API for the State Controller

- **Status:** Proposed — 2026-06-16
- **Module:** `messaging/axoniq-state-controller`
- **Supersedes:** the `EventStream` / `Condition` surface introduced in `b3df8a82df`

## Context

The State Controller lets a developer make a write-side decision from a command and a slice
of event history, built on Dynamic Consistency Boundaries (DCB) — no rebuilt aggregate, no
`evolve`, no version locking. The consistency boundary is derived from the tags and event
types a decision actually reads.

The first cut exposed that machinery directly. A trivial yes/no required five plumbing
concepts:

```java
@StateController
public Decision rentBike(RentBike cmd, DecisionContext ctx) {
    EventStream bike = ctx.scope("bike", cmd.bikeId());
    var available = bike.latestOf(BikeRequested.class, RequestRejected.class, BikeReturned.class)
                        .isA(BikeRequested.class)
                        .not();
    if (available.isTrue()) {
        return Decision.emit(new BikeRequested(cmd.bikeId(), cmd.userId()));
    }
    return Decision.reject("bike is not available for rental");
}
```

`EventStream`, the lazy `Condition`, `.isA(...).not()`, `.isTrue()`, and `emit` are all
implementation vocabulary. They make event sourcing feel like a query engine instead of
business logic. The design goals are explicit:

- **Business-first.** A decision should read like the rulebook, in plain Java.
- **No scary terminology.** Avoid `Command Handler`, `Decider`, `(Left) Fold`, `Projection`,
  `Condition`. `History` and `Decision` are everyday words; decisions live in a plain `@Service`.
- **DCB-native.** Read the specific facts this decision needs — do not reintroduce a rebuilt
  `State` object (that is the decider pattern we are deliberately avoiding).
- **Threading-agnostic and testable.** No hidden ambient state; a decision is a function of
  its declared inputs.

## Decision

Adopt an eager, value-returning API built on one history type — `History` — plus the existing
`Decision` result.

### Core concepts

| Concept | Type | Role |
|---|---|---|
| The event history | `History` | Injected into a decision; narrowed to a scope with `of(tagKey, tagValue)`, then read. |
| The result of a decision | `Decision` | `accept(...)` / `reject(...)`; sealed `Accept` / `Reject`. |

`Condition`, `.isTrue()`, `fold`, and `EventStream` are removed from the public surface.
Every `History` read returns a plain value: `boolean`, `long`, `BigDecimal`, or the event.

### Obtaining a `History`

A decision receives the event history as a single injected `History` parameter and narrows it
to the scope(s) it cares about with `of(tagKey, tagValue)`. The same form serves one scope or
many — there is no second factory type and no implicit binding to reason about.

Decisions are `@Decide` methods on an ordinary application bean (`@Service`); there is no
dedicated controller annotation. The scope tag is always supplied explicitly to `of(...)`, so
nothing needs to be declared at class level — the framework discovers `@Decide` methods on
registered beans, the same way it discovers other message handlers.

Single scope:

```java
@Service
class Accounts {

    @Decide
    Decision withdraw(Withdraw cmd, History history) {
        History account = history.of("account", cmd.accountId());
        if (account.has(AccountClosed.class)) return reject("account closed");

        BigDecimal balance = account.total(MoneyDeposited.class, MoneyDeposited::amount)
                             .subtract(account.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
        if (balance.compareTo(cmd.amount()) < 0) return reject("insufficient funds");

        return accept(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
    }
}
```

Multiple scopes — identical shape, narrowed twice:

```java
@Decide
Decision transfer(TransferMoney cmd, History history) {
    History from = history.of("account", cmd.fromId());
    History to   = history.of("account", cmd.toId());

    if (from.has(AccountClosed.class))       return reject("source account closed");
    if (to.has(AccountClosed.class))         return reject("target account closed");

    BigDecimal balance = from.total(MoneyDeposited.class, MoneyDeposited::amount)
                         .subtract(from.total(MoneyWithdrawn.class, MoneyWithdrawn::amount));
    if (balance.compareTo(cmd.amount()) < 0) return reject("insufficient funds");

    return accept(new MoneyWithdrawn(cmd.fromId(), cmd.amount()),
                  new MoneyDeposited(cmd.toId(),   cmd.amount()));
}
```

The injected `History` is unbound until narrowed: the read vocabulary is only meaningful on the
result of `of(...)`, never on the root parameter itself.

### Reading a `History`

A small, business-readable vocabulary. Bare-payload accessors cover the common case; a
timestamped `Entry<E>` accessor exists for the rare time-based rule.

| Method | Returns | Question it answers |
|---|---|---|
| `has(Class<?>)` | `boolean` | did this ever happen? |
| `never(Class<?>)` | `boolean` | did this never happen? |
| `lastWas(Class<?>)` | `boolean` | is the single most recent event in the scope this type? (boundary = tag) |
| `latest(Class<E>)` | `Optional<E>` | the most recent event of this type |
| `latestOf(Class<?>...)` | `@Nullable Object` | the most recent event among these types — `instanceof` / `switch` it |
| `first(Class<E>)` | `Optional<E>` | the first event of this type |
| `count(Class<?>...)` | `long` | how many of these happened? |
| `total(Class<E>, Function<E,BigDecimal>)` | `BigDecimal` | sum of a field |
| `entry(Class<E>)` | `Optional<Entry<E>>` | latest event **with** its recorded timestamp (`occurredAt()` + `payload()`) |

### Producing a `Decision`

`accept(Object... events)` and `reject(String reason)` are statically imported. Chain
`accept(...).returning(value)` to hand a value (e.g. a generated id) back to the caller, and
`reject(...).recording(events)` to leave an audit trail on a denial. `accept()` with no events
is a valid idempotent success.

### Consistency boundary & determinism

- The DCB boundary is captured from the tags narrowed via `of(...)` and the event types touched
  by reads; see *Utilising Dynamic Consistency Boundaries* for how it is derived and the
  read-granularity trade-off.
- Time and generated identifiers are **inputs, not history**: inject a `Clock` / id source as
  a method parameter. A decision runs once per command (it is not replayed like a workflow),
  so this is a testability rule, not a correctness one.

## Worked example: bike rentals

The `rentBike` / `returnBike` decisions from the module's sample
(`src/test/java/io/axoniq/framework/statecontroller/sample/rental/BikeRentals.java`), rewritten in
the proposed API. `latestOf(...)` returns the most recent event among the named types (or
`null`), so each decision reads with a plain `instanceof` pattern. The deliberately narrow type
set in `rentBike` — which excludes `RequestApproved` — keeps the consistency boundary tight and is
expressed simply by naming only the three types that matter.

```java
@Service
public class BikeRentals {

    @Decide
    public Decision rentBike(RentBike cmd, History history) {
        History bike = history.of("bike", cmd.bikeId());

        // available unless the latest of these three is a pending/active request
        if (bike.latestOf(BikeRequested.class, RequestRejected.class, BikeReturned.class)
                instanceof BikeRequested) {
            return reject("bike is not available for rental");
        }
        return accept(new BikeRequested(cmd.bikeId(), cmd.userId()));
    }

    @Decide
    public Decision returnBike(ReturnBike cmd, History history) {
        History bike = history.of("bike", cmd.bikeId());

        // rented iff the latest of this pair is an approval; its userId is the current renter
        if (!(bike.latestOf(RequestApproved.class, BikeReturned.class) instanceof RequestApproved approved)) {
            return reject("bike is not currently rented");
        }
        if (!approved.userId().equals(cmd.userId())) {
            return reject("bike is rented by another user");
        }
        return accept(new BikeReturned(cmd.bikeId(), cmd.userId()));
    }
}
```

Against the original — `latestOf(...).isA(BikeRequested.class).not().isTrue()` and
`...as(RequestApproved.class).mapPresent(RequestApproved::userId).value()` over an
`OptionalCondition` — the lazy `Condition` chain and the `Optional` unwrapping each collapse into a
single `instanceof` pattern. Same behaviour, same tight boundary, no condition vocabulary.

## Utilising Dynamic Consistency Boundaries

This design is an ergonomic surface over AF5's DCB event store; the friendly API compiles down to
standard DCB constructs. Nothing here is new persistence machinery — only a translation.

**What DCB gives us.** A DCB event store answers a *query* (criteria: tags, optionally narrowed by
event type) rather than serving a single ordered stream, and lets an append be made *conditional*
on that query: "append these events only if no event matching this query has been written since
position `P`." The consistency boundary is therefore defined by the facts a decision actually read
— dynamically — instead of by a fixed aggregate identifier.

**How the API maps onto it.**

| API surface | DCB construct |
|---|---|
| `history.of("account", id)` | adds a tag `account=id` to the read criteria |
| `has` / `latest` / `latestOf` / `count` / `total` | register the event type(s) those reads touch, narrowing the criteria |
| first read on a `History` | executes the sourcing query and records the consistency marker (the store position at that moment) |
| `accept(events)` | appends the produced events under a condition of *(accumulated criteria, recorded marker)* |
| `reject(reason)` | no conditional append; `recording(...)` audit events go through the same event-store transaction unconditionally |

**Lifecycle of one decision.**

1. The decision narrows one or more scopes with `of(...)` and reads them. The first read fixes a
   snapshot position `P` and loads the matching events; later reads are answered as of `P`, so the
   decision observes one consistent view.
2. Each read contributes its *(tag × type)* criteria to a running sourcing query.
3. On `accept`, the framework appends the produced events conditioned on *(criteria, P)*. The store
   commits only if nothing matching `criteria` was written after `P`; otherwise the command fails
   with a concurrency conflict and may be retried. This is optimistic concurrency scoped to exactly
   the facts read — not a whole aggregate.
4. On `reject`, no condition is needed; any `recording(...)` audit events are appended directly.

**Why the design needs DCB, not aggregates.**

- *Minimal boundary.* Criteria are built from the tags and types a decision touches, so two
  decisions reading disjoint facts never conflict — even on the "same" entity.
- *Cross-entity in one shot.* `transfer` narrows two `account` scopes; both tags enter the one
  sourcing query, so the append is conditional across both accounts atomically. A single-aggregate
  model cannot express this without a saga.
- *No rebuilt state.* The store query replaces aggregate event replay; `History`'s readers fold in
  memory only what was loaded. There is no long-lived state object to evolve.

**Key implementation decision — read granularity.** Eager reads cannot know up front every event
type a decision will touch, so the framework picks one of:

- **Per-tag load (simpler):** on first touch of a scope, load that tag's full slice; the boundary
  is tag-granular. Fewer round-trips; a wider boundary that may over-conflict.
- **Incremental, type-precise (matches the prior lazy design):** each read extends the criteria and
  loads *(tag × new types)* as of `P`; the boundary stays type-precise at the cost of extra
  round-trips.

Recommendation: start with per-tag load (one read per `of(...)`, snapshot at `P`) for simplicity,
and revisit type-precise loading only if boundary width measurably hurts contention. Either way the
recorded marker `P` and the append condition keep the same shape — only the criteria's precision
differs.

## Today's equivalent (current AF5 DCB)

For comparison, the same `transfer` written **without** this module, directly against the AF5 DCB
event store today. Signatures below are verified against the published event-store source; an
equivalent live consumer already exists at
`messaging/axoniq-state-controller/src/main/java/io/axoniq/framework/statecontroller/eventstream/SourcedEventStream.java:310`
(single combined tag-set — the transfer case only adds a second account via `or()`).

```java
// A programmatic CommandHandler.handle(CommandMessage, ProcessingContext)
public MessageStream.Single<CommandResultMessage> handle(CommandMessage command, ProcessingContext context) {
    TransferMoney cmd = (TransferMoney) command.payload();

    // 1) ONE criteria spanning BOTH account tag-sets, OR'd together (+ narrow types to keep the boundary tight)
    EventCriteria criteria =
        EventCriteria.havingTags(Tag.of("account", cmd.fromId()))
                     .andBeingOneOfTypes(new QualifiedName("MoneyDeposited"),
                                         new QualifiedName("MoneyWithdrawn"),
                                         new QualifiedName("AccountClosed"))
                     .or()
                     .havingTags(Tag.of("account", cmd.toId()))
                     .andBeingOneOfTypes(new QualifiedName("MoneyDeposited"),
                                         new QualifiedName("MoneyWithdrawn"),
                                         new QualifiedName("AccountClosed"));

    // 2) Source on the active transaction — it auto-captures the ConsistencyMarker and threads it into the append
    EventStoreTransaction tx = eventStore.transaction(context);
    MessageStream<? extends EventMessage> stream = tx.source(SourcingCondition.conditionFor(criteria));

    // 3) Fold balance + closed-flag in memory. The OR'd stream mixes BOTH accounts' events, so you must
    //    filter by accountId yourself — history.of(...) is what scopes that per-entity in the proposed API.
    record Acc(BigDecimal fromBal, boolean toClosed) {}
    CompletableFuture<Acc> folded = stream.reduce(new Acc(BigDecimal.ZERO, false), (acc, entry) -> {
        Object p = entry.message().payload();
        BigDecimal bal = acc.fromBal();
        boolean closed = acc.toClosed();
        if (p instanceof MoneyDeposited d && d.accountId().equals(cmd.fromId())) bal = bal.add(d.amount());
        if (p instanceof MoneyWithdrawn w && w.accountId().equals(cmd.fromId())) bal = bal.subtract(w.amount());
        if (p instanceof AccountClosed  c && c.accountId().equals(cmd.toId()))   closed = true;
        return new Acc(bal, closed);
    });

    // 4) Decide + conditionally append on the same tx (guarded by the marker captured in step 2)
    return MessageStream.fromFuture(folded.thenApply(acc -> {
        if (acc.toClosed())                            throw new IllegalStateException("target account closed");
        if (acc.fromBal().compareTo(cmd.amount()) < 0) throw new IllegalStateException("insufficient funds");
        tx.appendEvent(new GenericEventMessage(new MessageType("MoneyWithdrawn"), new MoneyWithdrawn(cmd.fromId(), cmd.amount())));
        tx.appendEvent(new GenericEventMessage(new MessageType("MoneyDeposited"), new MoneyDeposited(cmd.toId(),   cmd.amount())));
        return null; // CommandResultMessage
    }));
}
```

Verified API surface (`file:line`):

- `EventStore.transaction(ProcessingContext)` → `EventStoreTransaction` — `eventsourcing/eventstore/EventStore.java:68`
- `EventStoreTransaction.source(SourcingCondition)` / `.appendEvent(EventMessage)` — `EventStoreTransaction.java:59,106`
- `EventCriteria.havingTags(Tag...).andBeingOneOfTypes(QualifiedName...)` + `.or()` — `eventstreaming/EventCriteria.java:221,256`, `EventTypeRestrictableEventCriteria.java:57`
- `SourcingCondition.conditionFor(EventCriteria)` — `eventstore/SourcingCondition.java:47`
- `MessageStream.reduce(identity, BiFunction)` → `CompletableFuture` — `messaging/core/MessageStream.java:432`
- Conflict surfaces as `AppendEventsTransactionRejectedException` — `eventstore/AppendEventsTransactionRejectedException.java:51`

What this comparison establishes:

- **The proposed API adds no consistency safety — only ergonomics.** The consistency marker is
  *already* captured and threaded automatically by `DefaultEventStoreTransaction` on the same
  transaction; a developer never touches it. The append is guarded either way.
- **It does hide four real burdens:** hand-built `EventCriteria` per tag-set OR'd together;
  `source` + `MessageStream.reduce` + `CompletableFuture` composition; manual per-account filtering
  (the OR'd stream returns both accounts' events interleaved); and `MessageType`/`GenericEventMessage`
  wrapping on append.
- **One burden stays out of band in both worlds:** a `TagResolver`/`eventTagger` must map appended
  payloads to `Tag("account", id)`, or the DCB guard silently covers the wrong boundary. The
  proposed API should fold this into the same declaration so the read tags and the write tags
  cannot drift — a point for the implementation phase.

Net delta: a ~6-line declarative decision versus ~35 lines of criteria-building, stream folding, and
future composition, plus the same out-of-band tag configuration.

## Consequences

### Positive

- A decision reads as plain Java over a named history — no query-engine vocabulary, no
  `.isTrue()` tax. Lowest possible learning curve for the framework's flagship simplification.
- `History` and `Decision` are domain words; nothing leaks DCB or event-sourcing jargon.
- One type, one verb (`of`), one shape for one scope or many — nothing to disambiguate.
- Cross-entity decisions (transfer) are first-class via multiple `of(...)` calls — the DCB
  payoff, expressed without ceremony.
- No rebuilt `State`, no `evolve` — stays on the DCB side of the line, away from the decider
  pattern.
- No ambient state; decisions are pure functions of their parameters and trivially unit-tested.

### Negative / trade-offs

- **The injected `History` is unbound until `of(...)`.** The root parameter carries the read
  vocabulary, but those methods are only meaningful after narrowing — calling them on the root
  must be rejected as a programming error (never a silent global query).
- **Single scope still costs one `of(...)` line.** There is no zero-ceremony auto-bound
  injection; every decision narrows explicitly. Accepted in exchange for one type and one
  uniform shape across single- and multi-scope decisions.
- **Lost optimizations from dropping `Condition`:** no lazy single-read batching across
  multiple scopes (each `of(...)` is its own read), and no `map`/`zip`/`fold` composition.
  Eager reads also default to a tag-granular consistency boundary rather than the type-precise
  one the lazy design could derive (see *Utilising Dynamic Consistency Boundaries*). Accepted:
  those were the technical surface we set out to remove, and single-scope (the 95% case) is
  still one read.
- **Custom accumulation** beyond `count` / `total` needs a deliberately-named escape hatch
  (e.g. `summarize(...)`); deferred to a follow-up ADR rather than re-exposing `fold`.
- **Time reads** require the `entry(...)` accessor plus an injected `Clock`; bare-payload
  accessors are insufficient for time-based rules.

## Alternatives considered

- **Bound injection + `Events` factory (two types):** inject an already-bound `History` for
  single scope and a separate `Events` factory for multi-scope. Gives zero-ceremony single-
  scope decisions, but needs a second type, implicit id-resolution from the command, and the
  two `History` meanings (bound slice vs root) are ambiguous when either could be injected.
  Rejected in favour of one uniform type. Auto-bound injection can be reintroduced later behind
  a parameter annotation (e.g. `@Scope History account`) without a second type, should the
  explicit `of(...)` line prove tiresome.
- **Static `History.of(tag, id)` (Option C):** no parameter at all. Requires ambient
  per-decision binding via `ThreadLocal` / `ScopedValue`. Rejected: violates the framework's
  no-internal-`ThreadLocal` and threading-agnostic principles and breaks isolated testability.
- **Keep `EventStream` + lazy `Condition`:** retains batching and composition but is the exact
  surface the goals reject. Rejected.
- **Decider pattern (`decide(cmd, state)` + `evolve`):** well-researched (Chassaing, Emmett)
  and testable, but reintroduces a rebuilt `State` and the `evolve`/fold step DCB exists to
  eliminate. Rejected as off-goal.
