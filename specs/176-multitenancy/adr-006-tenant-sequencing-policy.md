# ADR 006: Tenant-scoped sequencing policy (issue [#254](https://github.com/AxonIQ/axoniq-framework/issues/254))

Date: 2026-08-19
Status: work in progress

> This ADR records the intended direction before implementation. It must be updated as implementation and validation
> progress, especially if the existing command or event-processing configuration seams require a different approach.

## Context

Sequencing policies are defined for generic messages, rather than separately for command dispatching and event
processing. The framework already offers a fully sequential policy, which deliberately gives every message the same
sequence identifier, and policies that derive an identifier from message data.

Multi-tenancy attaches the tenant identifier to message metadata under `tenantId`
(`TenantDescriptor.TENANT_ID_KEY`). Today, an application that needs sequential handling within each tenant has to
implement its own policy based on that metadata. Issue [#254](https://github.com/AxonIQ/axoniq-framework/issues/254)
asks the multi-tenancy module to provide that common policy.

The issue also asks whether sequential processing must be isolated per tenant, or whether applications should be able
to choose one sequence shared by all tenants.

## Decision: support both behaviours through separate policies

The new opt-in `TenantSequencingPolicy` will sequence messages by their tenant identifier. Messages for one tenant use
the same sequence identifier; messages for different tenants use different identifiers and may therefore progress
independently.

Applications that require one sequence across every tenant will continue to configure the framework's existing
`SequentialPolicy`. The tenant policy will not gain a switch for global sequencing.

This keeps the meaning of each policy precise:

- `TenantSequencingPolicy` preserves tenant isolation and avoids one tenant's ordering requirement delaying another
  tenant.
- `SequentialPolicy` remains the explicit choice for the exceptional case where an application intentionally needs
  global ordering.

Providing both through separate policies is preferable to a configuration flag on the new policy. A flag would make a
tenant-named policy capable of cross-tenant behaviour, obscure the application's ordering boundary, and duplicate the
existing global policy.

## Implementation plan

1. Add one reusable `TenantSequencingPolicy` to the multi-tenancy module, implementing the generic
   `SequencingPolicy<Message>` contract shared by command and event processing.
2. Derive its sequence identifier from the existing `tenantId` metadata. Confirm and document the behaviour for a
   message without a tenant identifier; the current preferred direction is to return no identifier, like the generic
   metadata policy.
3. Keep the policy opt-in. Do not change any default command or event-processing sequencing configuration.
4. Add focused policy tests for equal tenant identifiers, distinct tenant identifiers, and absent tenant metadata.
   Add configuration-level coverage at the existing command and event seams when those seams are identified during
   implementation.
5. Document application opt-in usage and the distinction between tenant-scoped and global sequencing.

## Consequences

- Tenant-aware applications gain a supported, reusable ordering policy without re-implementing metadata lookup.
- Sequential work for one tenant does not impose ordering on another tenant when `TenantSequencingPolicy` is selected.
- No compatibility or default-behaviour change is introduced for applications that do not opt in.
- Cross-tenant ordering remains available, but is a deliberate application-level choice through `SequentialPolicy`.

## Open implementation checks

- Verify the exact configuration entry points used by command dispatching and event processing in the supported
  framework version.
- Verify whether the absence of tenant metadata should remain concurrent or require a documented fallback before the
  policy API is finalized.
- Update this ADR with the final class location, public API details, test coverage, and documentation links once the
  implementation lands.
