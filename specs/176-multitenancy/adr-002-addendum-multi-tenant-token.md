# ADR 002, addendum: one shared processor for all tenants needs its own token

Date: 2026-07-24
Status: accepted
Extends: [ADR 002, multi-tenant pooled streaming](adr-002-pooled-streaming.md)
Related: issue [#210](https://github.com/AxonIQ/axoniq-framework/issues/210) (event processing)

This addendum records why the shared-processor read side positions its stream with a module-owned token rather than reusing or relaxing the shared `MultiSourceTrackingToken`, including the option we implemented and then reverted.

## Context

One pooled streaming processor reads every tenant's events, so all tenants' positions live inside one combined token. The tenant set is not fixed: the `TenantProvider` starts after the processors and discovers tenants asynchronously, so the very first token is always written with zero tenants, and tenants are then added and removed at runtime. The processor's coordinator reads and compares the persisted tokens directly, outside the engine, so whatever token type positions the stream has to survive being compared against a token written for a different set of tenants.

`MultiSourceTrackingToken`, the framework's combined token, refuses on purpose to be compared with a token that tracks a different set of sources. It throws. That strict behaviour is a deliberate guardrail: it has been there since Axon Framework 4.2, and it makes a renamed source fail loudly rather than silently replay from the beginning. So it cannot position this stream unchanged.

We tried three fixes.

First we reconciled the stored token inside the engine when the stream opens. That does not work: the coordinator compares the persisted tokens directly, outside our reach, so the clash still happens.

Then we relaxed `MultiSourceTrackingToken` itself, so its comparisons tolerate a changing set of sources. That works and is the smallest change, but it turns a loud configuration mistake into a silent full replay for every existing user of the class, to serve a need only multi-tenancy has. We reverted it.

Finally we gave multi-tenancy its own token, and that is what we kept.

## Decision

The read side positions its stream with `MultiTenantTrackingToken`, a module-owned internal token. It reuses `MultiSourceTrackingToken` for the per-tenant positions, their aggregation, and their serialization, and adds only what that token deliberately refuses: tolerance for a changing set of tenants. Where `MultiSourceTrackingToken` throws when the sets differ, this token compares over the union of both sets and treats a tenant that only one side knows as being at its beginning. A tenant added since the token was written streams from the start of its store. A removed tenant is not read.

Reuse keeps the token to its one reason to exist. Only the union-tolerant comparison is new; storage, position aggregation, `advancedTo`, and Jackson serialization are the shared token's. Because [ADR 002](adr-002-pooled-streaming.md) merges the per-tenant streams natively in the engine rather than through `MultiStreamableEventSource`, the token advances positions directly and no longer needs the unwrap-and-rewrap dance the earlier `DynamicSourcesTrackingToken` performed around the merge.

`MultiSourceTrackingToken` is untouched. Its strict guard and its tests are exactly as they were, and nothing changes for anyone outside multi-tenancy.

### The invariant the tolerance depends on

Tolerating a changing set of tenants is not enough on its own. A token must also never name a tenant at a position the processor has not actually consumed. Absent means consumed nothing, and only genuinely consumed positions are named.

The reason is the processor's own duplicate check. A pooled streaming processor re-opens its stream at the earliest position any of its segments holds, so after a restart it deliberately re-reads events it has already handled, and relies on comparing each segment's stored token against the token on the streamed event to drop them again. That comparison is a single answer for the whole token. A token that names a tenant the stored token predates makes the comparison fail on that name alone, so the processor stops recognizing its own already-handled events and hands them to the handlers a second time. A projection then counts every one of them twice.

Two things follow. The beginning of every tenant's store is the token that holds no position at all, rather than one naming each tenant at position zero, so it cannot disagree with a token written before a tenant existed. And the per-tenant beginnings filled in to open a newly added tenant's stream stay out of the tokens carried on the merged entries, since the processor never reached those positions.

That removes the case where a tenant is dragged back to the start of its store. It does not make delivery exactly once, and the next section says why.

### What a tenant change still costs: duplicate delivery

A tenant change can still hand a tenant's event to the handlers more than once. Adding a tenant is a routine operation rather than a failure, so this is worth stating plainly.

The processor recognizes an event it has already handled by asking whether the token it last delivered covers the token on the streamed event. That is a single answer for the whole token, while the question that matters here is per tenant. A stored token written before a tenant existed holds no position for it, so once that tenant's position enters the emitted tokens, the comparison fails on that name alone, and events of a completely different tenant that the stored token does cover are handed over again.

A worked example. Tenant A holds two events and tenant B one, timestamped `A1 < B1 < A2`, and B becomes a tenant only after A's two events have been handled. The re-opened stream walks them in timestamp order. `A1` is recognized and dropped. `B1` is new and handled. `A2` arrives carrying B's position as well, which the stored token cannot cover, so `A2` is handled a second time.

Making the comparison per tenant is not a small change. `covers` is not only the duplicate check: `ReplayToken` uses it to decide when a replay has caught up, and `samePositionAs` is defined in terms of it. Redefining it for one source would break those. So it needs either a new way for the processor to ask about one source's position, or per-tenant processors, and that is a decision of its own rather than a detail of this one.

Until then the guarantee is the one the framework gives anyway, at least once, and event handlers on a multi-tenant stream have to be idempotent. The difference from a single-tenant processor is not the guarantee but how often it bites: a duplicate follows an ordinary tenant change, not only a crash.

## Consequences

- Existing users keep their guardrail. No shared class changed.
- The awkward first startup, a processor up before any tenant is discovered, works, and tenants can be added and removed at runtime.
- The decision is cheap to reverse. If the tolerant comparison is ever judged the correct general behaviour, the shared class can adopt it and this token can be deleted.
- The token's class name lands in customer token stores. Even though the class is internal, it cannot be renamed or moved later without a legacy type mapping.
- Keys of removed tenants stay in stored tokens forever. Harmless, but they accumulate.
- A tenant change can deliver another tenant's already-handled events again, so handlers on a multi-tenant stream have to be idempotent. At least once is the framework's guarantee anyway, but here a routine tenant change triggers it rather than a failure. Closing this needs a per-source duplicate check or per-tenant processors.
- The silent-replay risk we refused to give everyone still exists for multi-tenant applications: a renamed tenant context replays from the start without an error. For added tenants this is the intended behaviour. For renames it is a caveat the documentation states.

## Alternatives considered

- Reconcile the token when the stream opens. Rejected: the coordinator compares persisted tokens outside our reach.
- Relax `MultiSourceTrackingToken` for everyone. Implemented, then reverted: it turns a loud configuration mistake into a silent replay for existing users, and forces a multi-tenancy requirement onto a general-purpose class.
- Copy `MultiSourceTrackingToken` into a standalone token, as an earlier draft did. Rejected: it duplicated the shared token's storage, aggregation, and serialization for no gain. Reusing it and overriding only the comparison keeps the token to its one reason to exist.
- Per-tenant processors as the default, each with its own token store. Deferred as an opt-in rather than chosen for the must-have. Almost all of the complexity above is the price of not choosing it. If the default ever flips, this token and the restart mechanism can both be deleted.
