# ADR 008: Tenant-scoped sequencing policy (issue [#254](https://github.com/AxonIQ/axoniq-framework/issues/254))

Date: 2026-08-19
Status: accepted

## Context

Sequencing policies are defined for generic messages, rather than separately for command dispatching and event
processing. The framework already offers a fully sequential policy, which deliberately gives every message the same
sequence identifier, and policies that derive an identifier from message data.

Multi-tenancy resolves the tenant of a message through `TenantRouter`, which combines the tenant carried by the
`ProcessingContext`, the configured `TenantResolver`, and the known tenants supplied by `TenantDescriptors`. Today, an
application that needs sequential handling within each tenant has to implement its own policy against those same
contracts. Issue [#254](https://github.com/AxonIQ/axoniq-framework/issues/254) asks the multi-tenancy module to provide
that common policy.

The issue also asks whether sequential processing must be isolated per tenant, or whether applications should be able
to choose one sequence shared by all tenants.

## Decision: provide an explicit policy factory and leave configuration to the application

The multi-tenancy module provides a `TenantSequencingPolicy` factory that sequences messages by their resolved tenant.
Messages for one tenant use the same sequence identifier; messages for different tenants use different identifiers and
may therefore progress independently.

Applications that require one sequence across every tenant continue to configure the framework's existing
`SequentialPolicy`. The tenant policy does not gain a switch for global sequencing.

Applications must explicitly choose where to apply the policy. Command sequencing and event sequencing use different
framework configuration mechanisms, so the multi-tenancy defaults enhancer does not register a policy component and does
not change either default.

This keeps the meaning of each policy precise:

- `TenantSequencingPolicy` preserves tenant isolation and avoids one tenant's ordering requirement delaying another
  tenant.
- `SequentialPolicy` remains the explicit choice for the exceptional case where an application intentionally needs
  global ordering.

Providing both through separate policies is preferable to a configuration flag on the new policy. A flag would make a
tenant-named policy capable of cross-tenant behaviour, obscure the application's ordering boundary, and duplicate the
existing global policy.

## Implementation

`TenantSequencingPolicy` lives in the multi-tenancy API package as a functional interface extending the generic
`SequencingPolicy<Message>` contract shared by command and event processing. Its factory returns a lambda backed by a
`TenantRouter`, so applications can use the same tenant-routing component as the rest of the multi-tenancy module when
they configure command or event sequencing.

The processing context is considered first, and only an absent context tenant falls back to message-based resolution.
Only known tenants are used as sequence identifiers. When no known tenant can be resolved, the policy returns no
sequence identifier.

The policy is opt-in. No default command or event-processing sequencing configuration changes.

## Consequences

- Tenant-aware applications gain a supported, reusable ordering policy without re-implementing tenant resolution.
- Sequential work for one tenant does not impose ordering on another tenant when `TenantSequencingPolicy` is selected.
- No compatibility or default-behaviour change is introduced for applications that do not opt in.
- Users choose the command sequencing policy, event sequencing policy, or both, depending on which processing path needs
  tenant-local ordering.
- Cross-tenant ordering remains available, but is a deliberate application-level choice through `SequentialPolicy`.
