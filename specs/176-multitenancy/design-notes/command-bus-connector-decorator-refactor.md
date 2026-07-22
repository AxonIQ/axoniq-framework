# Command Bus Connector: Fold Abstract Base + Decorator Refactor

**Status**: Implemented, 2026-07-16
**Context**: `MultiTenantAxonServerCommandBusConnector#registerTenant`/`#registerAndStartTenant` review surfaced a broader question about the class hierarchy under `axon-server-connector`'s command package. This note captures the reasoning for the refactor that followed.

## Starting point

The Multitenancy PoC refactored Axon Server command-channel plumbing (subscribe/unsubscribe/dispatch/disconnect mechanics, shutdown-latch handling) to reuse in the following subclasses:

- `AxonServerCommandBusConnector` -- one connection.
- `MultiTenantAxonServerCommandBusConnector` -- one connection per tenant, via a private `TenantState` inner class that duplicated subscription/in-flight-command bookkeeping and called back into the abstract class's `protected final` helpers.

## Step 1 -- the decoration idea

The two implementors' `subscribe`/`unsubscribe`/`dispatch` control flow genuinely diverges (single call vs. dedup-then-fan-out-then-replay), so forcing a template method onto the abstract class was the wrong direction -- the shared outer shape assumption a template method needs didn't hold.

But `TenantState` wasn't doing anything `AxonServerCommandBusConnector` didn't already do correctly -- it re-implemented a single-tenant connector, just less completely (no load-factor validation, no subscribe/unsubscribe/disconnect logging). So instead of hand-rolling per-tenant state, `MultiTenantAxonServerCommandBusConnector` composes N real `AxonServerCommandBusConnector` instances, one per tenant, and delegates to their already-correct public methods.

By doing that, `MultiTenantAxonServerCommandBusConnector` now directly implements `CommandBusConnector` and we could fold `AbstractAxonServerCommandBusConnector` back into `AxonServerCommandBusConnector` as only subclass, simplifying the class hierarchy.

## Net effects

- Two-class structure instead of three; each class's public methods are readable end-to-end in one file instead of requiring a jump into a shared base.
- Composition fixed latent bugs for free: the multi-tenant path had been missing the `loadFactor >= 0` assertion and subscribe/unsubscribe/disconnect logging that `AxonServerCommandBusConnector` already had correctly -- once the multi-tenant class calls the real connector instead of duplicating its mechanics, it inherits those fixes automatically.
- During the fold, an existing asymmetry was caught and fixed: `AxonServerCommandBusConnector#disconnect()` never actually called `connection.disconnect()` (only `prepareDisconnect()` + drained in-flight commands), while the multi-tenant side's `TenantState#disconnect()` did. Both now call it consistently.
- Behavior-preserving where it mattered: all 7 pre-existing `MultiTenantAxonServerCommandBusConnectorTest` cases passed unchanged, since they exercise the class purely through its public `CommandBusConnector`/`MultiTenantAwareComponent` API.
- One genuinely new piece of behavior was required, not "free" anymore: `onIncomingCommand` propagation to tenants registered after the handler was set, since each child connector now owns its own handler field instead of sharing one via the abstract base. Covered by a new test.
