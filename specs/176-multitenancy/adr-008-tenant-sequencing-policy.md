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

## Decision: support both behaviours through separate policies

The multi-tenancy module registers a new opt-in `TenantSequencingPolicy` component. It sequences messages by their
resolved tenant. Messages for one tenant use the same sequence identifier; messages for different tenants use different
identifiers and may therefore progress independently.

Applications that require one sequence across every tenant continue to configure the framework's existing
`SequentialPolicy`. The tenant policy does not gain a switch for global sequencing.

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
`SequencingPolicy<Message>` contract shared by command and event processing. Its default implementation is a lambda
backed by the registered `TenantRouter`, so it uses the same tenant-routing component as the rest of the
multi-tenancy module.

The processing context is considered first, and only an absent context tenant falls back to message-based resolution.
Only known tenants are used as sequence identifiers. When no known tenant can be resolved, the policy returns no
sequence identifier.

The policy is opt-in. No default command or event-processing sequencing configuration changes.
The multi-tenancy configuration registers the policy as a component so applications can refer to it from their own
command or event-processing configuration.

## Consequences

- Tenant-aware applications gain a supported, reusable ordering policy without re-implementing tenant resolution.
- Sequential work for one tenant does not impose ordering on another tenant when `TenantSequencingPolicy` is selected.
- No compatibility or default-behaviour change is introduced for applications that do not opt in.
- Cross-tenant ordering remains available, but is a deliberate application-level choice through `SequentialPolicy`.
