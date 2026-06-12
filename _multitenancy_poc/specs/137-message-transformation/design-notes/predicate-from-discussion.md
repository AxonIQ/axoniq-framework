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