# ADR 005: Tenant-specific converters for data protection (issue [#176](https://github.com/AxonIQ/axoniq-framework/issues/176))

Date: 2026-08-07
Status: accepted
Extends: [ADR 001, per-tenant event storage routing](adr-001-event-storage-tenant-routing.md)

## Context

Data protection encrypts fields while a message is converted to or from bytes. A converter deliberately receives only
the value to convert; it does not receive the processing context that carries the tenant. Deriving a tenant from event
metadata would persist routing information that is not part of the event contract, and ambient state such as a
`ThreadLocal` is unsafe across asynchronous work and retries.

The data-protection extension supplies one `FieldEncryptingConverter` in its single-tenant example. Multi-tenancy
needs one of those converters, and therefore one crypto-engine choice, for each tenant. Requiring applications to
configure the same field-encrypting converter separately for event storage, snapshots, commands, queries, and
persistent streams would be both error-prone and unlike the single-tenant configuration model.

## Decision

The multi-tenancy module resolves a `Converter` once, while it constructs infrastructure already bound to a known
`TenantDescriptor`. `TenantComponentProvider<Converter>` is optional:

- When it is present, each tenant receives its provider-produced converter. Event storage and persistent streams wrap
  it in a `DelegatingEventConverter`; snapshot stores use it directly; command and query connectors wrap it in a
  `DelegatingMessageConverter`.
- When it is absent, every path retains the ordinary application `EventConverter`, `GeneralConverter`, or
  `MessageConverter` instance. Multi-tenancy without data protection therefore takes the same converter path it did
  before this feature.

`TenantComponentLookup` is the small common abstraction used by the command and query connectors. Its default lookup
returns the configured singleton, preserving the existing constructors and their eager null validation. The provider
is found from the configuration root, so nested module configurations see the application registration without
requiring the data-protection extension as a module dependency.

This keeps tenant selection at an infrastructure construction boundary, where it is explicit and stable, rather than
trying to make a converter tenant-aware while it has no reliable tenant input.

## Consequences

- An application configures one tenant-scoped `Converter` provider. A data-protection setup can create a
  `FieldEncryptingConverter` with each tenant's crypto engine in that one factory.
- Event payloads, snapshots, command/query payloads, and persistent-stream events use the converter associated with
  the tenant connection that processes them.
- Tenant identity is not read from, or added to, persisted event metadata, and no ambient tenant state is introduced.
- The data-protection extension remains optional. The multi-tenancy module knows only Axon's converter interfaces.
- Although field-encrypting converters are the primary reason for this capability, tenant-specific conversion is not
  exclusive to data protection. Every multi-tenant conversion path can now use the provider, including deliberately
  choosing, for example, JSON for one tenant and XML for another. This is a useful general configuration option, but
  applications should be aware that it makes serialization format potentially a tenant-level compatibility decision.
- Conversion is intentionally the scope of this decision. It does not define tenant-scoped crypto-key storage,
  annotation configuration, or Spring conveniences; those belong to the extension and its integration layer.

## Tests

Unit tests assert that event storage and snapshots retain their configured default converters without a provider and
that snapshots select the provider's converter when one is registered. Two two-tenant Axon Server integration tests
cover the separate paths: one stores encrypted events and reads them through tenant-specific persistent streams; the
other stores and reloads encrypted snapshots through each tenant's store. The tests use distinct
`InMemoryCryptoEngine` instances and record their key creation; the extension's in-memory engine shares its backing
map, so this proves converter routing rather than a production key-store isolation guarantee.
