# ADR 012: Stored deadline type names resolve through the `XStreamConverter`'s mapper

Date: 2026-10-12
Status: accepted
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#595](https://github.com/AxonIQ/axoniq-framework/issues/595) (XStream for deadlines and sagas), [ADR 005](adr-005-deadline-stored-job-format.md) (stored job format), [ADR 008](adr-008-deadlines-and-sagas-on-the-xstream-converter.md) (XStream converter)

## Context

Axon Framework 4's `XStreamSerializer` stored a deadline's payload and scope descriptor under the type name XStream's
mapper gave the class, and resolved it through the same mapper when reading. Where a class has an XStream alias, the
stored name is that alias: a built-in one such as `uuid`, `int` or `long`, or one the application registered.

ADR 008 decided not to look up such names: `StoredDeadlineConverter` loads every stored type name as a class name, apart
from a fixed map for `string` and `byte-array`. An aliased payload fires as an `UnknownDeadlinePayload` and reaches no
handler; an aliased scope descriptor fails every read, which db-scheduler retries indefinitely. XStream-stored deadlines 
are common: Axon Framework 4's deadline manager builders defaulted to XStream.

## Options considered

- **A: extend the fixed alias map** with XStream's built-in aliases. It misses the application's own aliases and
  duplicates XStream's alias list, which changes with XStream versions.
- **B: resolve through the `XStreamConverter`'s mapper.** The application already configures its `XStream` instance with
  the aliases its Axon Framework 4 nodes used (ADR 008); reading the stored XML needs them anyway.

## Decision

Option B. With an `XStreamConverter`, `StoredDeadlineConverter` resolves a stored payload or scope type name through the
mapper of its `XStream` instance (`realClass`, as Axon Framework 4 did), and otherwise loads it as a class name. The
`XStreamConverter` is also found inside Axon Framework's delegating converters (`DelegatingEventConverter`,
`DelegatingMessageConverter`, `DelegatingGeneralConverter`, at any depth), as Axon Framework wraps the general converter
in them by default. The fixed `string`/`byte-array` map goes, as the mapper resolves those aliases too. Other converters
behave as before.

This replaces ADR 008's "no lookup of stored type names" for deadlines. ADR 008 otherwise remains accepted, including
the saga type column: an aliased saga still needs that column rewritten.

## Consequences

- Deadlines Axon Framework 4 stored under an alias fire with their real payload and scope, provided the `XStream`
  instance has the same aliases. Built-in aliases need no configuration.
- A converter writing XStream XML that is not an `XStreamConverter`, or is wrapped in another kind of converter, gets no
  alias resolution.
- Deadlines Axon Framework 5 stores keep class names as type names, which Axon Framework 4 also resolves, so both
  directions of a rolling upgrade keep working.
