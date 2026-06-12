# `EventTransformer`: functional interface or internal handle?

## The problem

`EventTransformer` is the type the event-transformation chain works with. The open
question is **what shape it should have**:

- a **functional interface** users can implement (lambda or class), or
- an **internal handle** that only the `EventTransformation` factory can produce?

And if functional, **at which layer**: the message level (`EventMessage -> MessageStream`)
or the payload level (`Course -> CourseV2`)? Those are different abstractions, and the
choice ripples into routing, forward-compat, and naming.

**The catch with a user lambda.** The spec requires the chain to know each
transformer's `from`/`to` `QualifiedName`, which drives routing/matching
(FR-011), overlap/conflict and cycle detection at `build()` (FR-008), and the
output-identity check (FR-018). A freely-implementable lambda
(`(msg, ctx) -> stream`) carries **none** of that, so the chain can't index it or
validate it. That's exactly why the current code rejected raw lambdas at runtime, and
why sealing it (step 3 below) made the rejection a compile-time guarantee. So "let
users pass a lambda" only works if `from`/`to` come from somewhere else (registration
or the factory). A bare lambda on its own goes against the chain's detection contract.

## Context

The shape has evolved through review discussion, and each step pins down a constraint
worth keeping in mind, so it's useful to walk through how we got here.

### 1. At PR open: functional, message-level

```java
@FunctionalInterface
public interface MessageTransformer<M extends Message> {
    MessageStream<M> transform(M message, @Nullable ProcessingContext context);
}

@FunctionalInterface
public interface EventTransformer extends MessageTransformer<EventMessage> { /* same, M = EventMessage */ }
```

`DefaultEventTransformer` implemented `EventTransformer`, **but** the chain invoked its
package-private `applyTo(...)`, never `transform(...)`.

Why this shape at all? The interface was deliberately **message-level and
stream-returning** to leave room for the transformations planned beyond 1:1 renames:
**split** (one event → many), **drop** (one → none), and **multi-output**, none of
which a payload `BiFunction<T, U>` can express. So `transform` was forward-looking
scaffolding for those shapes. In the meantime, `DefaultEventTransformer` *had* to
implement `transform(...)` (it implemented the interface), but that signature was too
thin to do today's work: extracting the typed payload and running the output-identity
check needs the `MessageConverter` + `MessageTypeResolver`, which only
`applyTo(message, runtime)` carries. So the chain called `applyTo`, and `transform` was
left as dead inherited weight.
### 2. Review point: the interface method is never used (#9/#10)

> "This interface specifies the `transform` method which is never called or
> implemented. The API it defines isn't reflected in the implementation. Everything
> goes through the `EventTransformation` factory and the chain requires
> `DefaultEventTransformer`."

A fair catch: the single abstract method was dead, and `register()` would reject any hand-written lambda
anyway.

### 3. What I changed in response: sealed empty markers

```java
public interface MessageTransformer<M extends Message> {}                 // empty

public sealed interface EventTransformer
        extends MessageTransformer<EventMessage>
        permits DefaultEventTransformer {}                                  // empty, sealed
```

Removed the unused single abstract method and sealed the interface, so only the factory's
`DefaultEventTransformer` can exist, turning "rejected at runtime" into "impossible to
write". This is the **current state**.

**What happens when split / drop / multi-output arrive later?** Nothing *blocks* them:
they'd come as new factory methods (a 1:1 `BiFunction` can't return zero or many, so e.g.
`EventTransformation.split(...)` with a mapper yielding a `MessageStream`), plus a
generalized or additional internal impl. Sealing isn't an obstacle, since users never
author transformers anyway.

**But the empty marker scales badly here, so was maybe not that good of an idea:** with no method on
the interface, the chain can't dispatch polymorphically. it casts to
`DefaultEventTransformer` and calls `applyTo`, so a second shape (`SplitEventTransformer`)
turns that cast into a `switch` over every permitted type, in `register(...)` and in the
apply loop.

Giving the interface a **real message-level method** (`transform(...) ->
MessageStream`) would absorb all of it through one call: the stream return already
expresses zero / one / many, so 1:1, drop, and split share a code path. So the planned
features argue for putting a real method back on the interface (the *message-level
functional* shape).

### 4. Review follow-up: make it functional at the mapper (#13)

A follow-up comment points at the **payload mapper** in the factory:

```java
// EventTransformation.transform(...)
public static <T, U> EventTransformer transform(
        Class<T> inputType,
        BiFunction<T, @Nullable ProcessingContext, U> payloadMapper) { ... }
//      ^^^^^^^^ "this BiFunction is where EventTransformer should have been used"
```

> "Restore `EventTransformer` (and `MessageTransformer`) as a functional interface and
> use it here, so we can pass a lambda *or* a class in `EventTransformation#transform`.
> `DefaultEventTransformer` doesn't need to implement it; it's just a facility to put the
> transformer to work in the chain."

The problem however with this is the following:

1. *Forward-compat.* The base is generic over the **message** subtype
   (`<M extends Message>`) so 5.3 adds `CommandTransformer extends MessageTransformer<CommandMessage>`
   with no SPI break. A payload interface would make the base `<T, U>` and lose that.
2. *The chain can't invoke it.* We have **one chain per message type**, but a single
   event chain still holds mixed payloads (`Course -> CourseV2`, `Student -> StudentV2`).
   Only a message-level shape loops uniformly:

   ```java
   for (EventTransformer t : chain)                 // <M>: every entry is EventMessage -> EventMessage ✅
       msg = t.transform(msg, ctx).first();

   for (EventTransformer<?,?> t : chain)            // <T,U>: different T per entry, needs an unchecked cast ✗
       ((EventTransformer<Object,Object>) t).transform(msg.payload(), ctx);
   ```

   So even payload-level you'd still need a message-level `DefaultEventTransformer`, leaving
   the `<T,U>` interface a redundant alias for `BiFunction`.

## My proposal

**Go with the message-level functional interface, kept sealed for 5.2.0: restore the
message-level `transform` single abstract method and make the chain actually call it.**

```java
public interface MessageTransformer<M extends Message> {
    MessageStream<? extends M> transform(M message, @Nullable ProcessingContext context);
}
public sealed interface EventTransformer
        extends MessageTransformer<EventMessage> permits DefaultEventTransformer {}
```

- The chain calls `transform(...)` instead of `applyTo(...)` → addresses the #9/#10
  point ("never used") and gives the interface real meaning (fixes the status quo's only con).
- `from`/`to` stay registration/factory metadata, not on the single abstract method → routing keeps working,
  no raw-lambda hole.
- Keep it **sealed** for now: 5.2 only does 1:1 renames, so there's no user-implementation
  case yet. Revisit **unsealing** in 5.3 if/when we expose split/drop/multi-output or
  annotation-based registration, and define how `from`/`to` is supplied then.


This honors the intent behind both review threads (#9/#10 and #13): a meaningful,
genuinely-used interface. It keeps the chain and the forward-compat base intact, and
defers the one genuinely open call (user-implementability) until we have a concrete need.
The "keep `DefaultEventTransformer` as a chain facility" idea from #13 also still fits here as
a variant; see naming below.

**Knock-on for #16 (naming):** under this proposal `DefaultEventTransformer` still implements
`EventTransformer`, so `DefaultEventTransformer` (matching `DefaultSourcingCondition` etc.)
is the right rename => as Matheusz suggested on the pr. 

Cleanest C = the **thin** variant: the chain keeps the conversion + identity check (it already holds the converter and resolver and already reads transformer metadata for routing), and the transformer's `transform(message, context)` does only map + rewrap. That gives a genuinely real public method with the exact declared signature, no internal types leaked, and a clean split — chain owns framework plumbing, transformer owns the user's mapping intent.

## Options at a glance

| Dimension | Step 1 (before A) | **A — empty marker (now)** | B — payload `<T,U>` (#13) | **C — message-level, sealed (chosen)** |
|---|---|---|---|---|
| Interface method | `transform(M,ctx)` declared | none | `apply(T,ctx)` | `transform(M,ctx) → MessageStream` |
| Is the method real & called? | no — impl throws, chain ignores | n/a | yes | **yes — chain calls it directly** |
| Generic base | `<M extends Message>` | `<M>` | `<T,U>` | `<M extends Message>` |
| User lambda? | compiles, rejected at runtime | impossible (sealed) | yes, but **no from/to** | impossible (sealed; revisit 5.3) |
| Chain dispatch | cast → `applyTo` | cast → `applyTo` | unchecked cast → `apply` | polymorphic `transform` |
| Who converts payload + checks identity | transformer (`applyTo` + runtime) | transformer (`applyTo` + runtime) | transformer | **chain** (holds converter/resolver) |
| What `transform` body does | nothing (throws) | n/a | map only | **map + rewrap** |
| from/to source | factory metadata | factory metadata | must be registration | factory metadata |
| Forward-compat `<M>` (cmd/query) | kept | kept | **lost** | kept |
| drop/split later | cast → `switch` | cast → `switch` | impossible (payload can't 0/many) | same code path (stream return) |
| Dangling `#transform` doc link | — | **present** | n/a | fixed |
| Verdict | dead façade | works, but cast + stale link | reopens routing, drops `<M>` | **real method, clean signature, `<M>` kept** |

## Conclusion

Discussed with Allard we went for option C