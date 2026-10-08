# ADR 008: Deadline managers and saga stores read XStream data through the `XStreamConverter` (issue [#595](https://github.com/AxonIQ/axoniq-framework/issues/595))

Date: 2026-10-07
Status: accepted
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005) (deadline backends), [#592](https://github.com/AxonIQ/axoniq-framework/issues/592) (XStream converter), [#3728](https://github.com/AxonIQ/AxonFramework/issues/3728) (legacy saga stores), [#5048](https://github.com/AxonIQ/AxonFramework/issues/5048) (migration tooling), [ADR 005](adr-005-deadline-stored-job-format.md) (stored job format), [ADR 006](adr-006-deadline-manager-builders-on-af5.md) (builders)

## Context

[#592](https://github.com/AxonIQ/axoniq-framework/issues/592) adds the `XStreamConverter` to `axoniq-legacy`
(`org.axonframework.conversion.xstream`). It is a `Converter` around an `XStream` instance that the application
configures. It aliases Axon Framework 5's `Metadata` to Axon Framework 4's `<meta-data>` element, and `UUID` to
`uuid`. It ships deprecated for removal, is never registered by default, and is meant only for draining data that Axon
Framework 4's `XStreamSerializer` stored.

That serializer was Axon Framework 4's default in several places:
- the Quartz deadline manager, when built without a serializer;
- the JobRunr and db-scheduler deadline managers of Spring Boot applications that did not set `axon.serializer.*`;
- the JPA, JDBC and Mongo saga stores when built without a serializer, and the JPA and JDBC saga stores of Spring Boot
  applications without `axon.serializer.general`.

For these applications, every stored deadline and every stored saga is XStream XML.

ADR 005 keeps Axon Framework 4's stored job layout and verifies it for Jackson. It left four questions about an
XStream-backed converter to #595:
- how metadata is written and read;
- whether the Axon Framework 3.3 Quartz layout becomes readable;
- whether stored type names that are XStream aliases are looked up;
- whether Spring Boot wires the converter.

ADR 006 records as a consequence that an application relying on Quartz's `XStreamSerializer` default cannot read the
jobs that default wrote.

Running the deadline backends against Axon Framework 4.13.2's own `XStreamSerializer` showed one more gap, which
affects Jackson as well. Axon Framework 4's serializers always serialized the payload:
- `JacksonSerializer` wrote the `String` `hello` as the JSON string `"hello"`, and a `byte[]` as a Base64 JSON string;
- `XStreamSerializer` wrote `<string>hello</string>` and `<byte-array>…</byte-array>`, and stored the type names
  `string` and `byte-array`, which are XStream's built-in aliases for these types.

An Axon Framework 5 `Converter` treats a `String` or `byte[]` as content that is already in its stored form, and only
changes its content type. So a deadline with such a payload reached the handler as the raw stored content without any
error, and one written by Axon Framework 5 failed on an Axon Framework 4 node. `Converter#convert(input, targetType)`
has no direction: it cannot tell "store this `String` payload" from "this `String` is the stored form". Neither
`JacksonConverter` nor `XStreamConverter` can fix this on their own.

## Options considered

For `String` and `byte[]` payloads:

**Option A: reject them.** `StoredDeadlineConverter` throws a `DeadlineException` when such a deadline is scheduled or
read. This is safe and small, but it refuses payloads that Axon Framework 4 accepted, and deadlines that Axon Framework
4 nodes stored with them never fire.

**Option B: store them as Axon Framework 4 did, per format (chosen).** `StoredDeadlineConverter` knows the direction
and the payload type, so it writes these payloads itself, in the form Axon Framework 4 used for the converter's format.
It fails loudly only for a format it does not know.

**Option C: fix it in the converters.** Ruled out: without a direction, a converter cannot distinguish the two cases.

For telling which format the configured converter writes:

**Option D: check the converter's class**, unwrapping `DelegatingEventConverter` and `DelegatingMessageConverter`.
This ties the deadline managers to the class structure of Axon Framework 5's converters. It also fails for an
application's own wrapper.

**Option E: probe the converter once (chosen).** `StoredDeadlineConverter` converts `Boolean.TRUE` to a `String` when it
is built:
- `true` means JSON;
- `<boolean>true</boolean>` means XStream XML;
- anything else, or a failure, means an unknown format.

The result follows from what the converter actually writes, through any wrapper. A `JacksonConverter` with default
typing still writes `true`, since a `Boolean` carries no type information.

## Decision

- **No production change in the deadline managers or saga stores for XStream.** An application passes an
  `XStreamConverter` to `converter(...)` on the builder of a deadline manager or saga store. Its `XStream` instance has
  to allow the application's payload, saga and metadata types.
- **Metadata: no deadline manager change.**
  - Writing: the managers already pass Axon Framework 5's `Metadata` to the converter, which writes it as Axon
    Framework 4's `<meta-data>`. Axon Framework 4's JobRunr and db-scheduler details assign the metadata they read to a
    `MetaData` field, so any other root element would fail on an Axon Framework 4 node.
  - Reading: the `XStreamConverter` turns every metadata value into its `String.valueOf(value)` form, so a `UUID` or
    an `Instant` arrives in its usual text form. A nested map or list, and an application class, arrive as their
    `toString()`, such as `{key=value}`, where the Jackson path renders maps and lists as JSON (ADR 005). The
    `XStreamConverter` serves Saga and deadline payloads, whose metadata holds plain values, so nested metadata is
    not expected.
- **Axon Framework 3.3 Quartz layout: not supported.** A Quartz job with the `serializedDeadlineMessage` key keeps
  failing with the `DeadlineException` of ADR 005. The converter has no alias for the whole message either.
- **XStream aliases: no lookup of stored type names.** The application's `XStream` instance applies its aliases to the
  content it converts. A stored payload type name that is an application alias (`xStream.alias("order", Order.class)`)
  is still loaded as a class before the converter runs, so that deadline fires with an `UnknownDeadlinePayload`. An
  aliased saga needs its saga type column rewritten first. Both are documented.
- **`String` and `byte[]` payloads are stored as Axon Framework 4 stored them (Options B and E).**
  `StoredDeadlineConverter` handles them for all three persistent backends:
  - JSON: written as a JSON string, Base64 for a `byte[]`;
  - XStream XML: written as a `<string>` or `<byte-array>` element;
  - any other format: scheduling such a deadline fails with a `DeadlineException`.

  Reading follows the stored form instead of the probe. It also maps the type names `string` and `byte-array` to these
  types. A stored payload of these types in any other form fails with a `DeadlineException` instead of reaching the
  handler. The type names stay as Axon Framework 5 derives them, `java.lang.String` and `[B`, which Axon Framework 4's
  `XStreamSerializer` resolves too.
- **Spring Boot: no auto-configuration for the converter.**
  - The JobRunr and db-scheduler deadline manager auto-configuration keeps wiring the `EventConverter`, and the saga
    store auto-configuration the `GeneralConverter`.
  - Both back off when the application defines its own `DeadlineManager` or `SagaStore` bean, which is how an XStream
    application uses the converter.
  - The auto-configured `deadlineDetailsTask` resolves the application's `DbSchedulerDeadlineManager` bean, so it
    fires the deadlines that manager stores.

ADR 005 and ADR 006 stay accepted. This decision makes one consequence of ADR 006 outdated: an application that relied
on `QuartzDeadlineManager`'s `XStreamSerializer` default can now read the jobs that default wrote, by passing an
`XStreamConverter` to `converter(...)`.

## Consequences

- Applications on XStream can roll an upgrade with Axon Framework 4 and 5 nodes side by side. This is verified against
  Axon Framework 4.13.2's own `XStreamSerializer` (configured through its builder, with an Axon Framework 4
  `ChainingConverter`):
  - **Deadlines, per persistent backend** (Quartz, JobRunr, binary and human-readable db-scheduler):
    - jobs fire in both directions;
    - stored scopes match for cancelling;
    - covered cases: a Saga and an aggregate scope, metadata with a string, number, boolean, `UUID` and nested map
      value (arriving as its `toString()`), no payload, a nested payload class, a `String` and a `byte[]` payload.
  - **Sagas, per store** (JPA, JDBC, Mongo): Axon Framework 4 reads a saga that was updated or inserted through the
    `XStreamConverter`.
- The compatibility tests run every case for Axon Framework 4's `JacksonSerializer`, its `Jackson3Serializer` with and
  without default typing, and its `XStreamSerializer`. They assert that each Axon Framework 4 object comes from the
  Axon Framework 4 class loader, so that a test cannot compare Axon Framework 5 with itself.
- Bypassing the `String` and `byte[]` handling fails all 64 `String` and `byte[]` compatibility cases.
- An XStream application that never configured a serializer has to configure one now. In Spring Boot that means
  defining its own deadline manager and saga store beans. The reference guide covers this on the deadline manager and
  saga infrastructure pages.
- Other JDK types stored by Axon Framework 4's `XStreamSerializer` under a built-in alias fire with an
  `UnknownDeadlinePayload`, as their type name does not resolve to a class. That covers payloads such as `int`, `uuid`
  or `list`. Such payloads are not expected for deadlines. If they occur, the alias mapping can grow.
- The payload format probe runs once per `StoredDeadlineConverter`, when the deadline manager is built. A converter that
  cannot convert a `Boolean` makes `String` and `byte[]` payloads fail. Every other payload is unaffected.
- [#5048](https://github.com/AxonIQ/AxonFramework/issues/5048) can rewrite `XStreamSerializer` builder calls, and
  Quartz builders that relied on the default, to an `XStreamConverter`. It leaves a marker for the type allowlist,
  which cannot be inferred at the call site.
- AxonFramework's `axon-5/api-changes/10-stored-format-changes.md` replaces "XStream-written data is not readable"
  with a pointer to the `XStreamConverter`.
