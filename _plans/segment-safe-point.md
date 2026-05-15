# Plan: Segment Safe-Point for Asynchronous Event Handlers

## Problem

Event handlers that are inherently asynchronous (e.g., those with their own persistent state not tied to the event stream position) may need to declare a **safe point**: the earliest processing position from which they need events replayed to guarantee internal consistency. Without this, the processor blindly resumes from wherever the `TokenStore` left off, which may be ahead of what an async handler has actually durably committed.

This is not a hypothetical concern. Two concrete scenarios that hit it today:

- **Workflows.** Workflow execution is asynchronous, yet driven by events. The tracking token of the event processor delivering events to the Workflow Engine advances to the latest event handed over to the engine, but that event may not have been entirely processed by the workflow yet. When the processor restarts, the stored token reflects "delivered to the engine," not "fully processed by the workflow." To recover workflow state correctly, the Workflow Engine must be able to replay a selective section of events back to the last point each workflow durably committed its state.
- **In-memory buffered projections.** A high-throughput projection technique is to keep the read model in memory and flush state to the backing store asynchronously in batches. This dramatically reduces write amplification during replays and steady-state ingestion. The cost is that the processor's tracking token can be ahead of what has been durably persisted at any given moment. On restart, the projection needs to be replayed from the last persisted position (its safe point) rather than from the stored token, otherwise the in-memory state that was never flushed is silently lost.

## Summary

This plan introduces a **safe point** mechanism: when a segment is claimed, each handler can declare the earliest position it needs replayed, and the processor rewinds to the lowest reported safe point before processing begins.

The change spans two repositories:

- **AF5 (Axon Framework)** gets the plumbing: `SegmentChangeListener.onSegmentClaimed()` return type changes to `CompletableFuture<Optional<TrackingToken>>`, and the `Coordinator` applies the returned safe point by wrapping the stored token in a `ReplayToken` before creating the `WorkPackage`. The break is judged acceptable because the listener was introduced in 5.1.0 and is not yet widely adopted; factory signatures are preserved so most call sites are unaffected. The break ships in AF5 5.2.0.
- **AxonIQ Framework** (this repo) gets the user-facing pieces: a new `SegmentSafePointProvider` interface and a `SafePointConfigurationEnhancer` that auto-discovers providers through arbitrary decorator chains (via a new `CapturingComponentDescriptor` that walks the existing `describeTo` graph). The feature is gated on a valid AxonIQ license through an `EntitlementObserver`; without one, the listener returns empty and the processor behaves like stock AF5. No new Maven module or licensable addon is introduced; everything lives in the existing `messaging/axoniq-event-streaming/`.

For an AF5-only user, the mechanism is still usable by manually registering a listener via the new `SegmentChangeListener.onClaimWithSafePoint()` factory. The AxonIQ Framework enhancer removes that wiring entirely.

The plan also documents an additive (non-breaking) alternative (Option B) at the bottom, kept as a fallback if external `SegmentChangeListener` implementers surface that we can't reach.

## Goal

When a segment is claimed by a `PooledStreamingEventProcessor` at startup, each registered event handler should be given the opportunity to return an `Optional<TrackingToken>` representing its safe point. The processor collects all safe points, takes the lower bound, and, if any handler's safe point is behind the stored token, resets the segment's token to that position before processing begins.

## Boundary

- **AF5** (the Axon Framework repository, checked out alongside this repo in a sibling project location) receives a focused, deliberately breaking change to `SegmentChangeListener.onSegmentClaimed()` (return type becomes `CompletableFuture<Optional<TrackingToken>>`), with corresponding updates to `SimpleSegmentChangeListener`, the factory methods, `andThen()`, and `Coordinator`. The listener was introduced in 5.1.0 and is not yet widely adopted; the break is judged acceptable to keep the API surface minimal. An additive non-breaking variant is documented at the bottom of the plan as **Alternative: additive approach (Option B)** for reference. No application-level concepts (safe point, event handler registry) leak into AF5.
- **AxonIQ Framework** (this repository) owns the `SegmentSafePointProvider` interface and the `ConfigurationEnhancer` that bridges registered event handlers into the `SegmentChangeListener` mechanism. All new types live in the existing `messaging/axoniq-event-streaming/` module under a new sub-package `io.axoniq.framework.messaging.eventstreaming.safepoint`, alongside the existing `MultiSourceTrackingToken` / `MultiStreamableEventSource`.

The `_archive/` folder in this repository is read-only reference material and must not be modified. All AF5 changes go to the separate Axon Framework repository checkout.

The proprietary interface (`SegmentSafePointProvider`) never appears in AF5. AF5 only sees the generic `SegmentChangeListener` contract with its updated return type.

**AF5-only usability:** A plain AF5 user _can_ use the safe-point mechanism by manually registering a `SegmentChangeListener` via `PooledStreamingEventProcessorConfiguration.addSegmentChangeListener()` using the new `onClaimWithSafePoint()` factory:

```java
processorConfig.addSegmentChangeListener(
    SegmentChangeListener.onClaimWithSafePoint(segment ->
        myHandler.safePointFor(segment.getSegmentId())
    )
);
```

This requires explicit, per-processor wiring. The core mechanics (token reset, stream restart from safe point) work without AxonIQ Framework.

**AxonIQ Framework advantage:** AxonIQ Framework eliminates the manual wiring entirely. The `SafePointConfigurationEnhancer` (shipped in `axoniq-event-streaming`) automatically discovers all `EventHandlingComponent`s that implement `SegmentSafePointProvider` (looking through decorator chains via `CapturingComponentDescriptor`) and registers the listener for every processor, with no per-processor configuration required. The `SegmentSafePointProvider` interface also gives the concept a first-class name rather than a raw `Function`. This is the intended division: AF5 provides the plumbing; AxonIQ Framework provides the "it just works" experience.

---

## Changes in AF5 (paths below are relative to the Axon Framework repository root)

### 0. Decorator-transparent capability discovery via `CapturingComponentDescriptor`

**Problem:** `ConfigurationEnhancer`s such as `DeadLetterQueueConfigurationEnhancer` wrap registered `EventHandlingComponent`s in decorator classes (e.g., `DeadLetteringEventHandlingComponent`). The outer wrapper implements `EventHandlingComponent` but not `SegmentSafePointProvider`, hiding the fact that the inner component does. A plain `instanceof` scan in `SafePointConfigurationEnhancer` would silently miss these providers.

**Solution:** Reuse the existing `describeTo` / `ComponentDescriptor` infrastructure. `DelegatingEventHandlingComponent.describeTo()` already calls `descriptor.describeWrapperOf(delegate)`, which the `ComponentDescriptor` contract requires to recursively call `delegate.describeTo(descriptor)` if the delegate is itself a `DescribableComponent`. A `ComponentDescriptor` implementation that captures every object passed to it therefore walks the full decorator chain automatically, with no new AF5 methods needed.

**New class: `CapturingComponentDescriptor`** (in AxonIQ Framework, in `messaging/axoniq-event-streaming/` under `io.axoniq.framework.messaging.eventstreaming.safepoint`)

A `ComponentDescriptor` that collects `EventHandlingComponent` instances it receives and recurses only into those, not into unrelated described properties such as repositories, factories, or stores. Since decorator chains for event handling are composed exclusively of `EventHandlingComponent` instances all the way down, there is no path from a non-`EventHandlingComponent` property back to one; limiting recursion to `EventHandlingComponent` keeps the traversal tight and avoids accidentally walking unrelated object graphs. Visited objects are tracked by identity to prevent cycles.

```java
class CapturingComponentDescriptor implements ComponentDescriptor {

    private final Set<EventHandlingComponent> captured =
            Collections.newSetFromMap(new IdentityHashMap<>());

    @Override
    public void describeProperty(String name, @Nullable Object object) {
        if (object instanceof EventHandlingComponent ehc && captured.add(ehc)) {
            ehc.describeTo(this);
        }
    }

    // Collection and Map overloads iterate and delegate to describeProperty(String, Object)
    // String / Long / Boolean overloads are no-ops

    @Override
    public String describe() { return ""; }

    public Set<EventHandlingComponent> captured() { return Collections.unmodifiableSet(captured); }
}
```

`SafePointConfigurationEnhancer` then discovers providers through any decorator depth:

```java
config.getComponents(EventHandlingComponent.class).values().stream()
      .flatMap(component -> {
          var capturing = new CapturingComponentDescriptor();
          component.describeTo(capturing);
          return capturing.captured().stream();
      })
      .filter(SegmentSafePointProvider.class::isInstance)
      .map(SegmentSafePointProvider.class::cast)
```

This requires **no changes to any AF5 class**. Any decorator built on `DelegatingEventHandlingComponent` (in AxonIQ Framework or from a third party) is automatically transparent, because `DelegatingEventHandlingComponent.describeTo()` already participates in the traversal by contract.

### 1. `SegmentChangeListener`: change `onSegmentClaimed()` return type

**File:** `messaging/src/main/java/org/axonframework/messaging/eventhandling/processing/streaming/segmenting/SegmentChangeListener.java`

```java
// Before
CompletableFuture<Void> onSegmentClaimed(Segment segment);

// After
CompletableFuture<Optional<TrackingToken>> onSegmentClaimed(Segment segment);
```

`onSegmentReleased(Segment): CompletableFuture<Void>` is **unchanged**; the release path has no safe-point concept.

**Javadoc on `onSegmentClaimed`** should explicitly state: the returned safe point is ignored on the first-ever claim of a segment (i.e. when the stored token is `null`), because there is no prior position to roll back from. The listener is still invoked in that case for its observational role.

**Factory methods:**

- `onClaim(Function<Segment, CompletableFuture<Void>>)`: public signature preserved (still takes a `CompletableFuture<Void>` function for source compatibility with observational callers). The factory body is updated: it now passes `segment -> onClaim.apply(segment).thenApply(unused -> Optional.<TrackingToken>empty())` to `SimpleSegmentChangeListener` so the produced listener satisfies the new return type.
- `runOnClaim(Consumer<Segment>)`: public signature preserved. Factory body updated: the lambda passed to `SimpleSegmentChangeListener` now returns `CompletableFuture.completedFuture(Optional.<TrackingToken>empty())` (instead of `completedFuture(null)`) so it satisfies the new `Function<Segment, CompletableFuture<Optional<TrackingToken>>>` constructor parameter type.
- `onClaimWithSafePoint(Function<Segment, CompletableFuture<Optional<TrackingToken>>>)`: **new** factory for listeners that report a safe point.
- `onRelease(...)`, `runOnRelease(...)`, `noOp()`: unchanged (release-side path is untouched).

**`andThen()` merges safe points** from both listeners, preserving the **sequential** invocation order of the current implementation (`thenCompose`, not `thenCombine`) so that listeners with side effects continue to run in the order they were composed:

```java
default SegmentChangeListener andThen(SegmentChangeListener next) {
    Objects.requireNonNull(next, "Next listener may not be null");
    return new SimpleSegmentChangeListener(
        segment -> onSegmentClaimed(segment).thenCompose(
            t1 -> next.onSegmentClaimed(segment).thenApply(t2 -> {
                if (t1.isEmpty()) return t2;
                if (t2.isEmpty()) return t1;
                return Optional.of(t1.get().lowerBound(t2.get()));
            })
        ),
        segment -> onSegmentReleased(segment)
                       .thenCompose(unused -> next.onSegmentReleased(segment))
    );
}
```

**Breaking-change footprint.**
- Method descriptor of `onSegmentClaimed` changes; source and binary incompatible.
- Within both repositories, internal call sites that need adapting are limited to AF5 itself; see §1 of the *Scope of breakage* table in [Alternative: additive approach (Option B)](#alternative-additive-non-breaking-approach-option-b) at the bottom of the plan, which itemizes them.
- The AxonIQ Framework's `DeadLetterQueueConfigurationEnhancer` only uses `SegmentChangeListener.onRelease(...)`; it is **unaffected**.
- External implementers (if any) migrate mechanically by appending `.thenApply(unused -> Optional.<TrackingToken>empty())` to their `onSegmentClaimed` return statement.

### 2. `SimpleSegmentChangeListener`: update `onClaim` field type

**File:** `messaging/src/main/java/org/axonframework/messaging/eventhandling/processing/streaming/segmenting/SimpleSegmentChangeListener.java`

The `onClaim` field changes type to match the new return type of `onSegmentClaimed`. Constructor parameter type changes accordingly. Class shape (two fields, one constructor) is unchanged.

```java
public class SimpleSegmentChangeListener implements SegmentChangeListener {

    private final Function<Segment, CompletableFuture<Optional<TrackingToken>>> onClaim;
    private final Function<Segment, CompletableFuture<Void>> onRelease;

    public SimpleSegmentChangeListener(
            Function<Segment, CompletableFuture<Optional<TrackingToken>>> onClaim,
            Function<Segment, CompletableFuture<Void>> onRelease) {
        this.onClaim   = Objects.requireNonNull(onClaim, "Claim listener may not be null");
        this.onRelease = Objects.requireNonNull(onRelease, "Release listener may not be null");
    }

    @Override
    public CompletableFuture<Optional<TrackingToken>> onSegmentClaimed(Segment segment) {
        return onClaim.apply(segment);
    }

    @Override
    public CompletableFuture<Void> onSegmentReleased(Segment segment) {
        return onRelease.apply(segment);
    }
}
```

The constructor signature change is the binary-incompatible part of this file. Internal AF5 callers (factory methods on `SegmentChangeListener`, `andThen`) are updated to pass functions of the new type; the `onClaim`/`runOnClaim` factories internally wrap a `CompletableFuture<Void>` function into the new shape so their external signatures stay backward compatible.

### 3. `Coordinator`: apply safe point before creating the `WorkPackage`

**File:** `messaging/src/main/java/org/axonframework/messaging/eventhandling/processing/streaming/pooled/Coordinator.java`

Currently `createWorkPackage()` calls `onSegmentClaimed()` **after** `workPackageFactory.apply(segment, token)`, too late to affect the starting token or stream position.

**Restructure the claim loop** (currently the segment loop at lines ~843–854, followed by the log block at ~856–862) so that listener notification and safe-point retrieval happen before the `WorkPackage` is created:

```java
Map<Segment, TrackingToken> newSegments = claimNewSegments();
for (Map.Entry<Segment, TrackingToken> entry : newSegments.entrySet()) {
    Segment segment = entry.getKey();
    TrackingToken token = entry.getValue();

    // 1. Notify listeners (existing behavior) and collect optional safe point (new)
    TrackingToken effectiveToken = applySafePoint(segment, token);

    // 2. Compute stream start position from effective token
    TrackingToken unwrapped = WrappedToken.unwrapLowerBound(effectiveToken);
    streamStartPosition = streamStartPosition == null || unwrapped == null
            ? null : streamStartPosition.lowerBound(unwrapped);

    // 3. Create WorkPackage with effective token
    workPackages.computeIfAbsent(segment.getSegmentId(),
                                 wp -> createWorkPackage(segment, effectiveToken));
}
```

New private helper `applySafePoint(Segment, TrackingToken)`:
```java
private TrackingToken applySafePoint(Segment segment, TrackingToken token) {
    // Always notify the listener; the segment is being claimed, that is a fact.
    Optional<TrackingToken> safePoint =
            joinAndUnwrap(segmentChangeListener.onSegmentClaimed(segment));

    // A safe point is only meaningful relative to a position we have already moved past.
    // For a first-ever claim (token == null), there is nothing in front to roll back from,
    // so any reported safe point is ignored.
    if (token == null) {
        return null;
    }

    return safePoint
            // Wrap only when safe point is STRICTLY BEHIND token.
            // token.covers(sp) == true  ⇔ sp is at-or-behind token.
            // sp.covers(token) == false ⇔ sp is not at-or-ahead of token, i.e. strictly behind.
            .filter(sp -> token.covers(sp) && !sp.covers(token))
            .map(sp -> {
                logger.info("Processor [{}] (Coordination Task [{}]). Wrapping segment [{}] token with safe point [{}].",
                            name, generation, segment, sp);
                return ReplayToken.createReplayToken(token, sp);
            })
            .orElse(token);
}
```

**Failure semantics.** Exceptions from `onSegmentClaimed()` propagate. The surrounding claim loop already has a `try/catch (Exception e)` that calls `abortAndScheduleRetry(e)`; a failing safe-point lookup will land there and cause a retry rather than allowing the segment to start from a potentially too-far-ahead stored token (which is precisely the bug this feature exists to prevent).

This is a deliberate departure from the pre-existing "swallow and continue" behavior in `createWorkPackage()`. Previously, `onSegmentClaimed` exceptions were swallowed because they were purely observational. With the unified safe-point return shape, the call is now safety-critical. Existing `onSegmentClaimed` implementations that previously had their exceptions silently logged will now abort the claim and retry. Worth a 5.1.x → next-version changelog entry.

**Why `covers()` and not `samePositionAs()` for the filter.** `samePositionAs` is bidirectional coverage; using `!samePositionAs` would also wrap when safe point is *ahead* of the token, which would then be silently truncated by `ReplayToken.createReplayToken` (it returns `startPosition` when `isStrictlyAfter(startPosition, tokenAtReset)`), moving the segment forward. The two-clause filter (`token.covers(sp) && !sp.covers(token)`) explicitly restricts wrapping to the "strictly behind" case.

The returned `ReplayToken` wraps the original `token` (current stored position) and `safePoint` (where to start streaming from). `WrappedToken.unwrapLowerBound()`, already used in the `streamStartPosition` calculation, returns `safePoint` from a `ReplayToken`, so the event stream opens from the right position without any further changes.

No `storeToken()` call is needed. The token store is not written to during this step. When the `WorkPackage` processes its first event, it will call `storeToken()` with the new position, at which point the `ReplayToken` is naturally replaced by a plain token. If the work package never starts (e.g., due to a crash), the next claim cycle will query safe points again and reconstruct the `ReplayToken`. The reset is idempotent at no extra cost.

**This idempotency requires** that `SegmentSafePointProvider` implementations derive their safe point from **externally committed state** (e.g., the position last acknowledged by the external system), not from the token store. Deriving the safe point from the stored token would produce a chicken-and-egg loop: each claim would reset to a position the handler hasn't actually committed yet. This contract is documented on `SegmentSafePointProvider`.

**Edge case, first-ever claim (`null` token).** `claimNewSegments()` returns the result of `tokenStore.fetchToken(...)` directly, and per `TokenStore.fetchToken`'s contract that value is `null` for a never-before-stored segment. (`NoToken.INSTANCE` is private to the `Coordinator` and only used as `lastScheduledToken`; it never appears in the `newSegments` map.) `applySafePoint` short-circuits this case: the listener is still invoked (the segment is being claimed, a fact worth notifying), but the returned safe point is ignored and `null` is returned to the claim loop unchanged. A safe point is conceptually a "roll back from here" instruction, and on a first-ever claim there is nothing in front to roll back from. Any safe point would either be at-or-behind the natural start (no-op) or in front of it (which is exactly the silent-forward-jump case the filter exists to suppress).

**Edge case, blocking on coordinator thread.** `joinAndUnwrap` blocks the coordination thread with a 30s default timeout. Safe-point providers should perform fast, in-memory or local lookups; long external calls will stall claim cycles for all segments. This is documented on `SegmentSafePointProvider`.

`createWorkPackage()` is simplified: it no longer calls `onSegmentClaimed()` itself (the claim loop now invokes it via `applySafePoint`). The method just constructs the `WorkPackage` and registers the batch callback.

---

## Changes in AxonIQ Framework

### 4. `SegmentSafePointProvider` (new interface)

A new interface in AxonIQ Framework, placed in the existing `messaging/axoniq-event-streaming/` module (alongside `MultiSourceTrackingToken` and `MultiStreamableEventSource`). Package `io.axoniq.framework.messaging.eventstreaming.safepoint`, a new sub-package to keep the new classes grouped, rather than flattening five new types next to the existing two.

```java
/**
 * Implemented by components that need to declare a safe point: the earliest tracking token from which
 * events must be replayed for the component to be in a consistent state with the external resources it manages.
 * <p>
 * <strong>Contract:</strong> The returned safe point MUST be derived from externally committed state
 * (e.g., the last position acknowledged by the external system this handler writes to). It MUST NOT be
 * derived from the processor's stored tracking token; doing so creates a chicken-and-egg loop where the
 * processor never advances past the safe point.
 * <p>
 * <strong>Performance:</strong> {@code onSegmentClaimed} is invoked on the processor's coordination thread
 * and blocks segment claiming. Implementations should perform fast, in-memory or local lookups only.
 */
public interface SegmentSafePointProvider {
    CompletableFuture<Optional<TrackingToken>> onSegmentClaimed(Segment segment);
}
```

Event handlers that need to declare a safe point implement this interface alongside `EventHandlingComponent`. A combined convenience interface may be provided:

```java
public interface SafePointAwareEventHandlingComponent
        extends EventHandlingComponent, SegmentSafePointProvider {}
```

**Decorator-transparency contract for third parties.** `SafePointConfigurationEnhancer` discovers providers by walking the decorator chain via `describeTo` (see §0). Custom `EventHandlingComponent` decorators not built on `DelegatingEventHandlingComponent` must call `descriptor.describeWrapperOf(delegate)` (or equivalently expose the delegate via `describeProperty`) for safe-point discovery to find an inner provider. This expectation is documented on `SegmentSafePointProvider`.

### 5. `SafePointConfigurationEnhancer` (new `ConfigurationEnhancer`)

Registers a `DecoratorDefinition.forType(PooledStreamingEventProcessorConfiguration.class)` (same pattern as `DeadLetterQueueConfigurationEnhancer`) that adds a `SegmentChangeListener` via the new `onClaimWithSafePoint()` factory. The listener uses `CapturingComponentDescriptor` (§0) to walk through any decorator chains and discover all `SegmentSafePointProvider` instances in the module-scoped configuration, then merges their safe points via `lowerBound`:

```java
delegate.addSegmentChangeListener(
    SegmentChangeListener.onClaimWithSafePoint(segment ->
        config.getComponents(EventHandlingComponent.class).values().stream()
              .flatMap(component -> {
                  var capturing = new CapturingComponentDescriptor();
                  component.describeTo(capturing);
                  return capturing.captured().stream();
              })
              .filter(SegmentSafePointProvider.class::isInstance)
              .map(SegmentSafePointProvider.class::cast)
              .map(p -> p.onSegmentClaimed(segment))
              .reduce(
                  CompletableFuture.completedFuture(Optional.empty()),
                  (acc, f) -> acc.thenCombine(f, SafePointConfigurationEnhancer::lowerBound)
              )
              .orElse(CompletableFuture.completedFuture(Optional.empty()))
    )
);
```

The `lowerBound` helper: if both present → `t1.lowerBound(t2)` wrapped in `Optional`; if one empty → the other; if both empty → `Optional.empty()`.

The `flatMap`-through-`CapturingComponentDescriptor` is what allows providers wrapped by `DeadLetteringEventHandlingComponent` or any other `DelegatingEventHandlingComponent`-based decorator to still be discovered. A plain `instanceof` check on the top-level component would miss them.

This works because the `config` in the decorator lambda is module-scoped: it contains exactly the `EventHandlingComponent`s belonging to this processor's module. The lookup is lazy (evaluated at claim time, not at enhancer time), matching the DLQ pattern.

**Order.** This enhancer should run before `DeadLetterQueueConfigurationEnhancer` (which is `Integer.MAX_VALUE - 100`) is irrelevant for correctness because the lookup is lazy, but for predictability set `ENHANCER_ORDER = Integer.MAX_VALUE - 200` so the safe-point listener is added first and `andThen`-composes earlier in the chain.

### 6. License check via `EntitlementObserver` (no new addon)

The safe-point feature is commercial and must require a valid AxonIQ license to operate, but it deliberately does **not** introduce its own `AxoniqAddon`. There should be no separately-licensable "segment safe point" line item on the customer's license; the feature is part of the broader commercial framework offering.

Instead, `SafePointConfigurationEnhancer` registers an `EntitlementObserver` with `EntitlementManager.INSTANCE`. The observer is, per `EntitlementManager`'s javadoc, "immediately notified with the current entitlement and throughput control state" upon registration, so we get the startup state synchronously and any later license arrival/expiry asynchronously.

Design sketch:

```java
public class SafePointConfigurationEnhancer implements ConfigurationEnhancer {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AtomicBoolean licensed = new AtomicBoolean(false);
    private final Clock clock;

    public SafePointConfigurationEnhancer() {
        this(Clock.systemDefaultZone());
    }

    SafePointConfigurationEnhancer(Clock clock) {
        this.clock = clock;
        EntitlementManager.INSTANCE.registerObserver((entitlement, throughputControl) -> {
            boolean valid = entitlement != null
                    && !entitlement.validity().isGracePeriodExpired(clock);
            boolean wasLicensed = licensed.getAndSet(valid);
            if (!valid && wasLicensed) {
                logger.warn("AxonIQ license no longer valid; segment safe-point feature is inactive.");
            } else if (valid && !wasLicensed) {
                logger.info("AxonIQ license detected; segment safe-point feature is active.");
            } else if (!valid) {
                logger.warn("No valid AxonIQ license detected; segment safe-point feature is inactive. "
                        + "Implement a SegmentSafePointProvider only with a valid commercial license.");
            }
        });
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        // Decorator registration as per §5. The listener consults `licensed.get()`
        // at claim time and returns Optional.empty() when no valid license is present,
        // so unlicensed users see no behavioral effect even if they implement the interface.
    }
}
```

The §5 listener body is amended to short-circuit when `licensed.get()` is false:

```java
SegmentChangeListener.onClaimWithSafePoint(segment -> {
    if (!licensed.get()) {
        return CompletableFuture.completedFuture(Optional.empty());
    }
    return config.getComponents(EventHandlingComponent.class).values().stream()
                 .flatMap(component -> { ... })
                 ...
})
```

**Behavior in the unlicensed case:** the enhancer is still registered and the `SegmentChangeListener` is still added to every `PooledStreamingEventProcessorConfiguration`, but the listener returns `Optional.empty()` for every claim, so no safe point is applied and the processor behaves exactly as a stock AF5 processor. A warning is logged once at startup (and on subsequent license transitions). No exception is thrown, no per-message enforcement happens, and `EntitlementManager.claimMessage` is never invoked.

**Why not gate the enhancer registration itself?** The license state can change at runtime (license arrives later, license expires) and the observer is notified asynchronously. Gating at enhancer time would freeze the decision at startup and miss later transitions. Gating at listener-invocation time tracks the live state.

**Test handle.** Tests construct the enhancer via the package-private `SafePointConfigurationEnhancer(Clock)` constructor and inject license state via a test `EntitlementObserver` registration (or by directly flipping the `licensed` field through reflection / package-private accessor; to be decided in test design).

### 7. Module wiring: ServiceLoader registration and POM edit

No new Maven module is needed: `axoniq-event-streaming` already exists, is already listed in the root `pom.xml` (line ~89) and in `axoniq-framework-bom/pom.xml` (line ~74). The wiring reduces to:

**New file:** `messaging/axoniq-event-streaming/src/main/resources/META-INF/services/org.axonframework.common.configuration.ConfigurationEnhancer`

Contents: a single line with the FQN `io.axoniq.framework.messaging.eventstreaming.safepoint.SafePointConfigurationEnhancer`. The `META-INF/services/` directory does not yet exist in this module; it must be created. Without this file, `ServiceLoader` will not pick up the enhancer and the "no wiring required" promise of §5 fails silently: the module would compile, ship, and do nothing.

**Edit:** `messaging/axoniq-event-streaming/pom.xml`

Add a `<dependency>` on `io.axoniq.license:entitlement-manager` (matches the `axoniq-dead-letter` pom). Required for the `EntitlementManager` / `EntitlementObserver` types used in §6. This is the only new runtime dependency the module gains from this work.

No `META-INF/services/io.axoniq.license.entitlement.AxoniqAddon` file is created; the feature does not register an addon.

---

## Alternative: additive non-breaking approach (Option B)

The plan above (Option A) makes a focused breaking change to `SegmentChangeListener.onSegmentClaimed()`. The non-breaking alternative leaves `onSegmentClaimed`'s signature alone and adds a sibling default method `safePointFor()`. This is the safer choice if external implementers of `SegmentChangeListener` exist that we don't have visibility into; it preserves source and binary compatibility for all of them.

### Changes under Option B

**`SegmentChangeListener` keeps `onSegmentClaimed(Segment): CompletableFuture<Void>` unchanged** and adds a new default method:

```java
public interface SegmentChangeListener {

    /** Unchanged. */
    CompletableFuture<Void> onSegmentClaimed(Segment segment);

    /**
     * Returns the safe point this listener wants the segment reset to, or {@code Optional.empty()}.
     * Default: empty. Invoked by the {@code Coordinator} after {@link #onSegmentClaimed(Segment)} returns.
     */
    default CompletableFuture<Optional<TrackingToken>> safePointFor(Segment segment) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    /** Unchanged. */
    CompletableFuture<Void> onSegmentReleased(Segment segment);
}
```

**Factory methods.** All existing factories (`onClaim`, `onRelease`, `runOnClaim`, `runOnRelease`, `noOp`) keep their signatures and behavior; their listeners' `safePointFor` falls through to the default. Add one new factory:

```java
static SegmentChangeListener onClaimWithSafePoint(
        Function<Segment, CompletableFuture<Optional<TrackingToken>>> safePoint) {
    return new SimpleSegmentChangeListener(
        segment -> CompletableFuture.completedFuture(null),
        safePoint,
        segment -> CompletableFuture.completedFuture(null)
    );
}
```

**`andThen()` composes three independent chains** (anonymous class instead of `SimpleSegmentChangeListener`):

```java
default SegmentChangeListener andThen(SegmentChangeListener next) {
    SegmentChangeListener self = this;
    return new SegmentChangeListener() {
        @Override public CompletableFuture<Void> onSegmentClaimed(Segment s) {
            return self.onSegmentClaimed(s).thenCompose(u -> next.onSegmentClaimed(s));
        }
        @Override public CompletableFuture<Optional<TrackingToken>> safePointFor(Segment s) {
            return self.safePointFor(s).thenCompose(
                t1 -> next.safePointFor(s).thenApply(t2 -> {
                    if (t1.isEmpty()) return t2;
                    if (t2.isEmpty()) return t1;
                    return Optional.of(t1.get().lowerBound(t2.get()));
                })
            );
        }
        @Override public CompletableFuture<Void> onSegmentReleased(Segment s) {
            return self.onSegmentReleased(s).thenCompose(u -> next.onSegmentReleased(s));
        }
    };
}
```

**`SimpleSegmentChangeListener` keeps its existing two-arg constructor** and gains a third field plus a new three-arg constructor:

```java
private final Function<Segment, CompletableFuture<Void>> onClaim;
private final Function<Segment, CompletableFuture<Optional<TrackingToken>>> safePoint;
private final Function<Segment, CompletableFuture<Void>> onRelease;

public SimpleSegmentChangeListener(
        Function<Segment, CompletableFuture<Void>> onClaim,
        Function<Segment, CompletableFuture<Void>> onRelease) {
    this(onClaim, s -> CompletableFuture.completedFuture(Optional.empty()), onRelease);
}

public SimpleSegmentChangeListener(
        Function<Segment, CompletableFuture<Void>> onClaim,
        Function<Segment, CompletableFuture<Optional<TrackingToken>>> safePoint,
        Function<Segment, CompletableFuture<Void>> onRelease) { ... }
```

**`Coordinator.applySafePoint()` makes two `joinAndUnwrap` calls per claim:**
```java
private TrackingToken applySafePoint(Segment segment, TrackingToken token) {
    joinAndUnwrap(segmentChangeListener.onSegmentClaimed(segment));            // existing
    Optional<TrackingToken> safePoint =
            joinAndUnwrap(segmentChangeListener.safePointFor(segment));        // new
    return safePoint
            .filter(sp -> token.covers(sp) && !sp.covers(token))
            .map(sp -> ReplayToken.createReplayToken(token, sp))
            .orElse(token);
}
```

For listeners that don't override `safePointFor`, the second call resolves to an already-completed `Optional.empty()` future, effectively free.

### Option A vs. Option B: trade-off

| Dimension | Option A (primary, breaking) | Option B (alternative, additive) |
|---|---|---|
| API surface | one method (`onSegmentClaimed`) | two methods (`onSegmentClaimed`, `safePointFor`) |
| Conceptual model | "what happens at claim, including where to start from", unified | "notify of claim" and "tell me where to restart", separated |
| `joinAndUnwrap` per claim per listener | 1 | 2 (second is free for default-impl listeners) |
| `SimpleSegmentChangeListener` shape | 2 fields, 1 constructor | 3 fields, 2 constructors |
| `andThen` body | one merged chain + release chain | three independent chains |
| Source compatibility for AF5 5.1.x users | **breaks** existing `SegmentChangeListener` impls and direct `SimpleSegmentChangeListener` constructor calls | preserved |
| Internal Axon migration | none beyond §1–§3 (the DLQ enhancer is unaffected; the one test uses `runOnClaim` which keeps its signature) | none |
| Externally-visible behavior change for legacy listeners | merged into the API-shape change | none for listeners that don't override `safePointFor` |

### Scope of breakage if we go with Option A (the chosen route)

| File | Repo | Breaks? | Migration |
|---|---|---|---|
| `SegmentChangeListener.java`, `SimpleSegmentChangeListener.java`, `Coordinator.java` | AF5 main | yes (intentional) | covered by §1–§3 above |
| `PooledStreamingEventProcessorTest.java` (uses `runOnClaim`) | AF5 test | no | factory signature preserved |
| `PooledStreamingEventProcessorConfigurationTest.java` | AF5 test | needs review (only references the type) | likely no-op |
| `DeadLetterQueueConfigurationEnhancer.java` | AxonIQ Framework | **no** | uses `SegmentChangeListener.onRelease(...)` only |

Third-party code that has implemented `SegmentChangeListener` since 5.1.0 would need a mechanical fix:
- For listeners that don't care about safe points: append `.thenApply(unused -> Optional.<TrackingToken>empty())` to the `onSegmentClaimed` return statement.
- For listeners constructed via `SimpleSegmentChangeListener`'s constructor with a `Function<Segment, CompletableFuture<Void>>`: rewrap the function similarly.

### When to switch to Option B

- Evidence emerges of external `SegmentChangeListener` implementers we can't trivially reach (downstream products, third-party extensions).
- The team decides the next AF5 release line should be strictly source-compatible with 5.1.x.

If neither holds, Option A is the chosen route per the boundary at the top of this plan.

---

## Why `EventHandlingComponent` does NOT get `onSegmentClaimed()` in AF5

The `TokenStore` interception approach (previous plan revision) was rejected because `TokenStore` is a persistence abstraction that has no access to (and no business knowing about) `EventHandlingComponent`s. The information available in the `TokenStore` API (`processorName`, `segmentId`, `ProcessingContext`) is insufficient to discover which handlers are registered for that processor.

`SegmentChangeListener`, by contrast, is called from within the `Coordinator` which already holds references to the processor name and its configuration. The AxonIQ Framework `ConfigurationEnhancer` can reach into the module `Configuration` to discover handlers at the point the listener is registered, naturally and without coupling persistence to application logic.

Adding `onSegmentClaimed()` to `EventHandlingComponent` in AF5 would expose the concept to all AF5 users for free. Keeping it in `SegmentSafePointProvider` (AxonIQ Framework only) ensures the feature requires an AxonIQ Framework dependency.

---

## Summary of Files

### AF5 (Axon Framework repository; paths relative to its repository root)

| File | Change |
|---|---|
| `messaging/src/main/java/org/axonframework/messaging/eventhandling/processing/streaming/segmenting/SegmentChangeListener.java` | Change `onSegmentClaimed` return type to `CompletableFuture<Optional<TrackingToken>>`; add `onClaimWithSafePoint` factory; update `andThen()` to merge safe points. Factory signatures for `onClaim`/`runOnClaim`/`noOp`/`onRelease`/`runOnRelease` preserved (bodies of `onClaim`/`runOnClaim` adjusted to satisfy the new constructor parameter type). Javadoc on `onSegmentClaimed` documents that the safe point is ignored on first-ever claim. |
| `messaging/src/main/java/org/axonframework/messaging/eventhandling/processing/streaming/segmenting/SimpleSegmentChangeListener.java` | Update `onClaim` field type and constructor parameter type to match the new `onSegmentClaimed` return type. Two fields, one constructor; shape unchanged. |
| `messaging/src/main/java/org/axonframework/messaging/eventhandling/processing/streaming/pooled/Coordinator.java` | Restructure claim loop; add `applySafePoint()` that always invokes `onSegmentClaimed` and applies the returned safe point only when the stored token is non-null; remove the swallowed `onSegmentClaimed` call from `createWorkPackage`. |
| `docs/reference-guide/modules/release-notes/pages/minor-releases.adoc` | Add a changelog entry describing the breaking signature change to `SegmentChangeListener.onSegmentClaimed` and the migration recipe (append `.thenApply(unused -> Optional.<TrackingToken>empty())` to existing implementations). |
| `docs/reference-guide/modules/events/pages/event-processors/index.adoc` | Audit any snippet that references `SegmentChangeListener` and update for the new return type. (Confirm during implementation; content there today may only reference the type by name.) |

### AxonIQ Framework (this repo, existing module `messaging/axoniq-event-streaming/`)

All files below sit under `messaging/axoniq-event-streaming/`. No new Maven module is created; the safe-point types are added to the existing module under a new sub-package `io.axoniq.framework.messaging.eventstreaming.safepoint`.

| File | Change |
|---|---|
| `src/main/java/io/axoniq/framework/messaging/eventstreaming/safepoint/SegmentSafePointProvider.java` | New interface (§4). |
| `src/main/java/io/axoniq/framework/messaging/eventstreaming/safepoint/SafePointAwareEventHandlingComponent.java` | New convenience interface combining `EventHandlingComponent` + `SegmentSafePointProvider` (§4, optional). |
| `src/main/java/io/axoniq/framework/messaging/eventstreaming/safepoint/CapturingComponentDescriptor.java` | New `ComponentDescriptor` for decorator-chain traversal (§0). |
| `src/main/java/io/axoniq/framework/messaging/eventstreaming/safepoint/SafePointConfigurationEnhancer.java` | New `ConfigurationEnhancer` (§5) that also registers an `EntitlementObserver` to gate activation on a valid AxonIQ license (§6). |
| `src/main/java/io/axoniq/framework/messaging/eventstreaming/safepoint/package-info.java` | `@NullMarked` package marker, matching the existing `eventstreaming/package-info.java`. |
| `src/main/resources/META-INF/services/org.axonframework.common.configuration.ConfigurationEnhancer` | New ServiceLoader registration for `SafePointConfigurationEnhancer`. The `META-INF/services/` directory does not yet exist in this module; it must be created (§7). |
| `pom.xml` | Add `<dependency>` on `io.axoniq.license:entitlement-manager` (§7). Required for the `EntitlementManager` / `EntitlementObserver` types referenced in §6. |

### AxonIQ Framework: repo-level edits

No edits are needed to the root `pom.xml` or `axoniq-framework-bom/pom.xml`; `axoniq-event-streaming` is already registered in both.

### Docs (subject to a separate team decision; see Open Question 1)

Whether to publicly document this feature is **out of scope of this implementation plan**. If the team later decides to publish, the candidate edits are:

| File | Change |
|---|---|
| `docs/reference-guide/modules/advanced-event-streaming/pages/segment-safe-points.adoc` (or similar) | New page using the draft below. Lives under an "advanced event streaming" docs module mirroring the existing `advanced-migration` / `advanced-release-notes` / `advanced-testing` pattern. Exact module name to be agreed when/if the team decides to publish. |
| `docs/reference-guide/modules/ROOT/partials/modules-advanced-framework.adoc` | Optional: add a row for the `axoniq-event-streaming` module (currently absent from the commercial-modules table) if/when the team wants the module surfaced there. Safe-points are not surfaced as a separate row. |

`CapturingComponentDescriptor` lives in this module for now; if a second feature needs the same decorator-chain-walking utility, it can be promoted to a shared AxonIQ Framework location.

---

## Tests

### AF5 (`messaging/` in the Axon Framework repository)

- **`CoordinatorTest` / pooled processor integration test**: newly claimed segment with a listener that returns a safe point strictly behind the stored token produces a `ReplayToken`-wrapped effective token; `WorkPackage` is constructed with the wrapped token; `streamStartPosition` reflects the safe point.
- **First-ever claim (`null` token)**: segment claimed for the first time (stored token is `null`) with a non-empty safe point: the listener IS invoked (verify via a recording listener) but the safe point is ignored and the effective token remains `null`. No NPE, no `ReplayToken` constructed.
- **Safe point at-or-ahead of token**: listener returns a token equal to or ahead of the stored token: no wrapping, original token is used unchanged.
- **Listener exception**: `onSegmentClaimed()` failure surfaces through `applySafePoint`, lands in the surrounding `try/catch`, and triggers `abortAndScheduleRetry` (no silent fallback to the stored token).
- **`andThen` ordering**: composed listeners are invoked sequentially; both safe points are merged via `lowerBound`; empty + non-empty returns non-empty.
- **Factory methods**: `onClaim`/`runOnClaim`/`noOp` produce listeners whose `onSegmentClaimed` returns `Optional.empty()`; `onClaimWithSafePoint` plumbs the supplied function's `Optional<TrackingToken>` through; release-side factories are unaffected.
- **Internal migration of `runOnClaim` test usage**: the existing `PooledStreamingEventProcessorTest.runOnClaim(...)` call site compiles unchanged; assert this in CI by leaving the test as-is and verifying the build succeeds.

### AxonIQ Framework (`messaging/axoniq-event-streaming/`)

- **Enhancer discovery, direct provider**: `EventHandlingComponent` implementing `SegmentSafePointProvider` is discovered and its safe point reaches the processor.
- **Enhancer discovery, through DLQ decorator**: provider wrapped by `DeadLetteringEventHandlingComponent` is still discovered (validates `CapturingComponentDescriptor` against a real decorator from `axoniq-dead-letter`).
- **Enhancer discovery, through multiple decorators**: `InterceptingEventHandlingComponent` + `SequenceCachingEventHandlingComponent` + DLQ wrapping a provider all transparent.
- **No providers registered**: listener composes to a no-op result (`Optional.empty()`) and does not affect token.
- **Multiple providers**: safe points are combined via `lowerBound`.
- **`CapturingComponentDescriptor`**: unit tests for cycle handling (identity-based dedup), `Collection`/`Map` overload behavior, and non-`EventHandlingComponent` property filtering.
- **License gating via `EntitlementObserver`**: with the observer reporting no entitlement, the listener returns `Optional.empty()` for every claim and no safe point is applied; with a valid entitlement, providers' safe points reach the processor as normal. Use a test `EntitlementObserver` / configured `EntitlementManager` state to drive both branches. Also verify the transition from unlicensed to licensed and licensed to expired flips behavior accordingly.

---

## Reference guide section (draft, for use IF the team decides to document this feature)

Whether to publicly document this feature is a separate team decision (see Open Question 1). If published, the draft below is the starting point. Location would be under an "advanced event streaming" docs section, not under `safe-point/`.

---

= Segment safe points for asynchronous event handlers

Some event handlers maintain their own persistent state that is not strictly coupled to the event processor's tracking token.
A typical example is a handler that writes events to an external system and tracks its own committed position separately.
When the processor restarts and claims a segment, its stored token may be ahead of the position the handler has actually durably committed, which would cause the handler to miss events.

To address this, an event handler can implement `SafePointAwareEventHandlingComponent` and declare its own *safe point*: the earliest position from which it needs events replayed to be in a consistent state.
When the processor claims a segment, it queries each handler for its safe point and, if any handler is behind the stored token, resets the segment to the lowest reported safe point before processing begins.

==== Implementing a safe-point-aware event handler

Implement `SafePointAwareEventHandlingComponent` instead of `EventHandlingComponent` and override `onSegmentClaimed()`:

[tabs]
====
Spring Boot::
+
--
[source,java]
----
@Component
public class ExternalSystemProjection implements SafePointAwareEventHandlingComponent {

    private final ExternalSystemOffsetRepository offsetRepository;

    @Override
    public CompletableFuture<Optional<TrackingToken>> onSegmentClaimed(Segment segment) {
        // Return the last position that has been durably committed to the external system.
        // The processor will reset to this position if it is behind the stored tracking token.
        return offsetRepository.lastCommittedToken(segment.getSegmentId())
                               .thenApply(Optional::ofNullable);
    }

    @Override
    public Set<QualifiedName> supportedEvents() { ... }

    @Override
    public MessageStream<Message> handle(EventMessage event, ProcessingContext context) { ... }

    // ... other EventHandlingComponent methods
}
----
--

Configuration API::
+
--
[source,java]
----
ExternalSystemProjection projection = new ExternalSystemProjection(offsetRepository);

MessagingConfigurer.create()
    .eventProcessing(processing ->
        processing.registerEventHandlingComponent("my-processor", "projection", projection)
    )
    .buildConfiguration();
----
--
====

No additional wiring is required.
`SafePointConfigurationEnhancer` is automatically applied and discovers all `EventHandlingComponent` beans or registered components that implement `SegmentSafePointProvider`.

==== What happens at startup

When the processor claims a segment, the following sequence occurs:

1. The stored tracking token for the segment is retrieved and the claim is taken.
2. Each registered event handler that implements `SegmentSafePointProvider` is asked for its safe point for that segment.
3. The lowest safe point across all handlers is determined.
4. If that safe point is behind the stored token, the stored token is reset to the safe point.
5. Processing begins from the effective token. The handler will receive all events it has not yet durably committed.

If no handler reports a safe point (the default), behaviour is unchanged and the processor resumes from the stored token as normal.

==== Returning `Optional.empty()`

Return `Optional.empty()` when the handler has no safe-point requirement for the given segment, for example because it has no state for that segment yet or it is fully caught up.
Handlers that do not implement `SegmentSafePointProvider` at all contribute nothing to the safe-point calculation and incur no overhead.

==== Caveats and contract

* The safe point you return MUST be derived from externally committed state (e.g., the last position acknowledged by the external system this handler writes to). It MUST NOT be derived from the processor's stored tracking token; doing so creates a chicken-and-egg loop where the processor never advances past the safe point.
* `onSegmentClaimed` runs on the processor's coordination thread and blocks segment claiming. Implementations should perform fast, in-memory or local lookups only.
* If `onSegmentClaimed` throws, the segment claim is aborted and retried on the next coordination cycle. The segment is not started from a potentially-too-far-ahead stored token.
* Custom `EventHandlingComponent` decorators must propagate `describeTo` calls to their delegate (calling `descriptor.describeWrapperOf(delegate)` is sufficient) for safe-point discovery to traverse through them. Decorators built on `DelegatingEventHandlingComponent` do this automatically.

---

## Resolved Decisions

1. **`covers()` semantics for the safe-point wrap filter.** Wrap only when the safe point is strictly behind the stored token: `token.covers(sp) && !sp.covers(token)`. The earlier `!token.covers(sp)` formulation was the inverse of the intended behavior; it would have wrapped only when the safe point was ahead of or divergent from the token, which is the case we explicitly want to skip.

2. **`applySafePoint()` exception handling.** Failures propagate. The surrounding claim loop catches and routes them to `abortAndScheduleRetry`. A safe-point lookup failure must not allow the segment to start from a potentially-stale stored token; that would silently reintroduce the missed-events bug the feature exists to prevent.

3. **Module placement.** New types live in the **existing** `messaging/axoniq-event-streaming/` module under a new sub-package `io.axoniq.framework.messaging.eventstreaming.safepoint`. No new Maven module is introduced; the BOM and root pom already register `axoniq-event-streaming`.

4. **First-ever-claim semantics.** When the stored token is `null`, the listener is still invoked (the segment is being claimed, a fact worth notifying) but the returned safe point is ignored. A safe point only makes sense relative to a position the processor has moved past; on first claim there is no such position.

5. **License gating without an addon.** No `SegmentSafePointAxoniqAddon` is introduced. The safe-point feature is part of the broader commercial framework; there should be no separately-licensable "segment safe point" line item. Instead, `SafePointConfigurationEnhancer` registers an `EntitlementObserver`; the listener returns `Optional.empty()` for every claim while no valid license is present (with a single startup warning logged), and switches on once a valid license is observed. No per-message `claimMessage` enforcement, no addon registration, just a startup/runtime gate driven by license state.

6. **`CapturingComponentDescriptor` location.** Lives in the `axoniq-event-streaming` module under the new `safepoint` sub-package alongside the rest of this feature's classes. Move to a more general AxonIQ Framework location only if a second feature later needs the same decorator-chain-walking utility; duplication is acceptable now to avoid premature promotion.

7. **`onSegmentClaimed` exception handling: propagate.** All exceptions from `onSegmentClaimed` (whether from the safe-point lookup or from third-party observational listeners composed via `andThen`) propagate to the claim loop's `try/catch`, which routes them to `abortAndScheduleRetry`. The listener chain is treated as safety-critical end-to-end: any failure aborts the claim rather than letting the segment start from a potentially-stale stored token. This is a behavior change from 5.1.x; previously, `createWorkPackage` logged-and-swallowed observational listener exceptions. Existing 5.1.x observational listeners that throw will now cause claim retries; this needs an AF5 changelog entry. We accept the change rather than splitting per-listener error policy because the AF5 API surface has no way to mark a listener as safety-critical vs observational, and the safety-critical role now applies to the chain as a whole.

8. **`SegmentChangeListener` API shape: Option A (breaking), shipping in AF5 5.2.0.** Change the return type of `onSegmentClaimed` to `CompletableFuture<Optional<TrackingToken>>` rather than add a sibling `safePointFor` default method. AF5 5.1.0 was released on 2026-04-28 and `main` is now `5.2.0-SNAPSHOT`; while `SegmentChangeListener` is technically a released 5.1.0 API, it was in the wild only ~2 weeks before this design and is not yet widely adopted. Within both repositories the only `SegmentChangeListener` user beyond AF5 itself is `DeadLetterQueueConfigurationEnhancer`, which touches only the release side and is unaffected. Factory signatures (`onClaim`, `runOnClaim`, `noOp`, the release-side methods) are preserved so internal call sites and existing user code that goes through the factories compile unchanged. External implementers of the interface migrate mechanically (append `.thenApply(unused -> Optional.<TrackingToken>empty())`). The 5.2.0 release notes (`docs/reference-guide/modules/release-notes/pages/minor-releases.adoc` in the Axon Framework repository) document the breaking change and the recipe. The additive alternative (Option B) is documented near the bottom of the plan for reference only.

## Remaining Open Questions

1. **Whether to publicly document this feature is a team decision and explicitly out of scope of this plan.** The implementation does not depend on the docs decision. The draft reference-guide section preserved below is provided as a starting point if the team decides to publish; it is not a deliverable of this work. If published, it belongs under an "advanced event streaming" docs section (e.g. a new `docs/reference-guide/modules/advanced-event-streaming/`, mirroring the existing `advanced-migration` / `advanced-release-notes` / `advanced-testing` pattern), not under a feature-specific module like `safe-point/`. The Summary-of-Files table reflects "may add" rather than "must add" for any docs files.
