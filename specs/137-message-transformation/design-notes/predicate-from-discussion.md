## What this is about

We're building a DSL for registering upcasters, roughly `from(type).to(type).transform(...)`. The open question is how much freedom `from` should have. Should it only match an exact version, or should it also accept a predicate or a range? Right now we have the predicate implemented, because that's what we landed on in the design meeting on 26 May.

## What we decided on 26 May, and why

We agreed that alongside an exact match (equals), `from` would also get a predicate variant, meaning a predicate over the qualified name plus version. `to` would stay a single concrete version. We deliberately left out overlap detection, so registration order decides what happens, the same way `@Order` works elsewhere.

Three arguments carried the decision at the time.

The first was **parity with AF4**. AF4's `canUpcast` already supported ranges, so a fixed `from` to `to` would actually be a step back in flexibility. The predicate is basically that same `canUpcast` function.

The second was that **ranges are a genuine problem people hit**. A lot of teams tie their message version to the Maven or app version, so one structure ends up spanning several versions, for example across patch releases. Without ranges you'd be stuck mapping every earlier version by hand.

The third was that it kept **the cheap and expensive checks cleanly separated**. Because `from` only ever sees the type, there's no way to smuggle an expensive payload inspection into it. That turned out to be a nice design constraint to enforce.

For the record, I argued against it. More flexibility means more complexity, and the overlap problem only surfaces late, at runtime. That got acknowledged but didn't win out against the parity argument at the time.

## After review: John pushed back, and I agree

John raised the same concerns again, and having sat with it, I agree with him. Our position at this point is that we shouldn't expose this variant at all.

The only real reason it exists is to support version ranges. The trouble is that a generic predicate carries no semantic information, so there's nothing for us to optimize against later. You always have to actually call the predicate to find out whether it matches. That means a single one of these anywhere in a chain makes the whole chain unoptimizable, because you can't rule the predicate out without checking it, especially when it's registered early. So our suggestion is to leave it out.

The other thing John questioned, and I think he's right, is whether registration order is really the best way to resolve conflicts between matchers. If we don't allow arbitrary predicates, we can use rules closer to Java method resolution, where the most specific match wins. An exact match would beat a range. So if you have an exact match on 1.0 and a range covering 0.9 to 1.1, the exact match wins whenever the version is 1.0, no matter what order they were registered in. That also makes an annotation-based or bean-based setup much simpler, since there's no need for a priority annotation.

It would also let us reject odd configurations up front. Two exact matches on 1.0 wouldn't be allowed, since they're duplicates. A range of 1.0 to 1.5 together with a range of 1.4 to 1.9 wouldn't be allowed either, since they overlap. But a range of 1.0 to 1.5 alongside an exact match on 1.4 would be fine, because the exact match takes priority and there's no genuine conflict.

The nice thing is that this maps cleanly onto how method resolution already works in Java. `method(String a)` and `method(Object a)` can coexist, and the `String` version wins when the argument is a `String`. `method(String a)` and `method(String b)` is a compile error because it's a duplicate. And `method(String a, String b)` alongside `method(String... multiple)` is fine, because the two-argument version wins when exactly two strings are passed. The conflict rules we'd want follow the same logic.

## What we want to put to Steven

We would suggest we drop the predicate variant for now. It buys us time to actually understand the consequences, and it makes the whole ordering question moot.

What we'd keep is the exact match, including the ability to pass several versions at once via a List, for example `IntStream.range(0, 100).toList()`. That's fast, it's optimizable, and it covers the case of a customer with a pile of odd versions, which is what the predicate was supposed to solve.

## Planning

I'm on leave from the end of next week for two weeks. My PRs haven't been fully reviewed yet, and the main thing I need from Steven is a read on how he sees this going. The goal is to get everything through before I'm out, but that hinges on how fast the reviews move and on where we land on the predicate question.

One thing worth flagging is that the longer we sit on this decision, the harder the remaining PRs get to review, because they're built on top of this work. Settling the API shape unblocks the rest of the review flow, so the predicate decision is effectively a blocker. Ideally we'd have it resolved as soon as possible, so the base pr can be merged in into the feature branch and I do not need to rework everything that is built on top of it

## Conclusion (decided 2026-06-15, implemented)

We landed somewhere different from "drop the predicate entirely." The problem was never that predicates exist; it was that registration order let a predicate fall _between_ exact matches, which is what breaks an annotation-based setup. So we keep predicates, but give them a fixed, lowest priority instead of removing them.

**Resolution model.** Matching is now two tiers, resolved like Java overload resolution rather than by registration order:

1. **Exact identity matches** (a concrete `from`, including a multi-version `from(List)`) are resolved by identity, independent of registration order. The most specific match wins, and ambiguity is a build-time error.
2. **Predicates are a fallback**, consulted only when no exact match claims the event. Among predicates the first registered match wins. A predicate never sits between exact matches, and an exact match always beats a predicate.

There are no "pre" predicates; only this single fallback tier.

**What ships now:**

- `from` also accepts a `List<MessageType>`: a multi-version exact match with one mapper, covering the "customer with a pile of odd versions" case (`IntStream.range(...).mapToObj(...).toList()`) that the predicate was meant to solve, while staying fast and order-independent.
- Conflict rejection at build time: two transformations claiming the same exact identity (including a list overlapping another list or a single, or a drop overlapping a transform) are rejected with a `ChainConfigurationException`.
- Predicates stay, demoted to the fallback tier and documented as the last resort: never preferred over an exact match, and always evaluated on a no-exact-match read, so they must be cheap. The docs steer people to `from(List)` instead of `type -> versions.contains(type.version())`.

**Why predicates stay rather than being dropped.** Dropping them would have been simpler, but the real fix for the concerns John and I raised is the _ordering_, not the predicate's existence. A fixed-priority fallback removes the ordering problem (and the need for an `@Order`-style priority on annotations) without losing the open-ended matching predicates provide.

**Deliberately deferred, and provably additive:**

- **Version ranges** as a first-class semantic matcher (a structured low/high bound, not an opaque predicate), slotting into the same exact-first resolution.
- **Annotation-based / bean-based registration**, which can only ever produce semantic (exact/range) matchers and so needs no priority annotation.

Both are new surface added beside what ships now and require no change to existing user code, _because_ the order-independent resolution lands now. That order-independence is the one change that could not have been deferred without a breaking change later.

**Status.** Implemented as a new PR layered behind the drop PR: axoniq-framework branch `feature/137/impl-phase-6-semantic-matching` (off `feature/137/impl-phase-5-drop`), AxonFramework docs/demo on `feature/axoniq/137/phase-6-semantic-matching` (off `feature/axoniq/137/phase-5-drop`). The feature is implemented in the drop branch's existing naming (`EventTransformation` units, `EventTransformerChain`); the `EventTransformation`->`EventTransformer` rename remains its own separate PR. The `message-transformation` reference guide and the `university-message-transformation` demo lead with `from(List)` and frame predicates as the fallback.