# ADR 005: Deadline managers keep Axon Framework 4's stored job format (issue [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005))

Date: 2026-10-05
Status: accepted
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#5004](https://github.com/AxonIQ/AxonFramework/issues/5004) (aggregate deadline to command), [#564](https://github.com/AxonIQ/axoniq-framework/issues/564) (Saga deadline delivery), [#5111](https://github.com/AxonIQ/AxonFramework/issues/5111) (tracing decorator), [ADR 002](adr-002-saga-deadline-scope-aware-delivery.md) (saga delivery), [ADR 004](adr-004-deadline-manager-tracing-decorator.md) (tracing), [ADR 006](adr-006-deadline-manager-builders-on-af5.md) (builders), [ADR 007](adr-007-aggregate-deadline-delivery-through-scope-aware.md) (aggregate deadline delivery), [#592](https://github.com/AxonIQ/axoniq-framework/issues/592) (XStream-backed `Converter`), [#595](https://github.com/AxonIQ/axoniq-framework/issues/595) (XStream `Converter` in the deadline managers)

## Context

Deadlines live for days, weeks or months. When an application moves from Axon Framework 4 to Axon Framework 5, its
Quartz, JobRunr and db-scheduler stores hold deadlines that Axon Framework 4 scheduled and that have not fired yet.
Keeping those firing is the purpose of the epic. During a rolling upgrade, Axon Framework 4 and Axon Framework 5 nodes
also run side by side on the same store. Each node then fires, and cancels, jobs the other version wrote.
`SimpleDeadlineManager` keeps its deadlines in memory and is not affected.

Axon Framework 4.13 stores each deadline as follows (`origin/axon-4.13.x`, `messaging/.../deadline/`):

| Backend      | Stored form                                                                                                                                                                                                                                                                                                                                     |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Quartz       | `JobDataMap` keys from `Headers`: `axon-deadline-name`, `axon-message-id`, `axon-message-timestamp` (ISO string, epoch millis in older jobs), `axon-serialized-message-payload`, `axon-message-type`, `axon-message-revision`, `axon-metadata`; plus `serializedDeadlineScope` and `serializedDeadlineScopeClassName`. Job class `DeadlineJob`. |
| JobRunr      | A job calling `JobRunrDeadlineManager.execute(String, String)` (older jobs: the deprecated `execute(String)`), whose first argument is a `DeadlineDetails` serialized to a `String`. Fields: `deadlineName`, `scopeDescriptor`, `scopeDescriptorClass`, `payload`, `payloadClass`, `payloadRevision`, `metaData`.                               |
| db-scheduler | Task `AxonDeadline` with a `DbSchedulerBinaryDeadlineDetails` (`d`, `s`, `sc`, `p`, `pc`, `r`, `m`) or `DbSchedulerHumanReadableDeadlineDetails` (the JobRunr field names), serialized by db-scheduler's own serializer, Java serialization by default. Neither class declares a `serialVersionUID`.                                            |

Payload, metadata and scope descriptor were written with one Axon Framework 4 `Serializer`: the one passed to the
manager's builder. Quartz defaulted to an `XStreamSerializer` when none was set. Axon Framework 4's Spring Boot
auto-configuration built the JobRunr and db-scheduler managers with the `eventSerializer` bean
(`AxonJobRunrAutoConfiguration`, `AxonDbSchedulerAutoConfiguration`), so for those applications the stored format is
whatever their event serializer was configured as. Without `axon.serializer.*` properties, that is the general
serializer, an `XStreamSerializer`. The payload type field holds the `SerializedType` name, which is the
payload's class name unless the application configured an XStream alias or another type mapping. A deadline without a
payload, scheduled with only a name, stores `SerializedType.emptyType()` instead: the type name `empty`, a `null`
revision, and the serialized `null`. Axon Framework 4's `deserialize(...)` checks for that type first and returns `null`
without loading a class. The revision is the `@Revision` value, usually `null`. Quartz still reads a layout from Axon
Framework 3.3: the whole `GenericDeadlineMessage` serialized under the deprecated `serializedDeadlineMessage` and
`serializedDeadlineMessageClassName` keys, which was replaced before 4.0.

Axon Framework 5 has no `Serializer` and no XStream support. Payloads are converted with a `Converter` (ADR 006), and
`Metadata` is a `Map<String, String>`.

The stash versions of the backends diverged from the Axon Framework 4.13 layout above during Axon Framework 5
development, most visibly with an added field holding the deadline's `MessageType` (`type`, or `t` in the binary
db-scheduler details) and `metaData` renamed to `metadata`. AxonFramework's
`axon-5/api-changes/10-stored-format-changes.md#deadlines` documents an earlier state of it. No release ever wrote that
format: from 5.0.0 to 5.1.2 the backends shipped only in the build-excluded `stash/todo` sources, and from 5.2.0 not at
all. So there are no stored jobs in it to keep readable, and every divergence can be reverted.

A fired Saga deadline never consults a message type. `AbstractSagaManager.send(...)` hands it to
`AnnotatedSaga.handle(...)`, which selects the handler members whose `canHandle(...)` accepts it. For a
`@DeadlineHandler` method that is two checks in sequence. `axoniq-legacy`'s `DeadlineMethodMessageHandlingMember` (in
`DeadlineMethodMessageHandlerDefinition`) requires the message to be a `DeadlineMessage` with a matching deadline name,
and then delegates to Axon Framework 5's `MethodInvokingMessageHandlingMember`. That one requires
`payloadType.isAssignableFrom(message.payloadType())`, where `payloadType` is the method's payload parameter type, or
`Object` without one, and that its parameter resolvers match. The payload therefore has to be an instance of its class
before delivery. A fired aggregate deadline is dispatched as a command (#5004), whose
name the `CommandGateway` resolves from the payload object itself.

## Options considered

**Option A: the stash format.** Write the `MessageType` into an extra field, and fall back to the payload class name for
Axon Framework 4 jobs that lack it. Every reader then has two paths. The extra field is never read when a deadline
fires, because delivery matches on the payload class. And it breaks rolling upgrades. An Axon Framework 4 node reads
the JobRunr `DeadlineDetails` with its configured `Serializer`. The extra field fails it under XStream, Axon Framework
4's default, and under a `JacksonSerializer` with a strict `ObjectMapper` (`UnrecognizedPropertyException`). Only a
lenient mapper, such as Spring Boot's, skips it. Renaming `metaData` to `metadata` fails it with every serializer,
because the metadata is then missing. In db-scheduler's Java-serialized details, any added field changes the class's
computed `serialVersionUID`.

**Option B: the exact Axon Framework 4.13 layout (chosen).** Read and write the fields above unchanged. Convert the
stored bytes or strings with the configured `Converter`.

## Decision

Option B.

- **Layout:** every key, field name, job signature and task name of Axon Framework 4.13 is kept. Every stash
  divergence from it is reverted.
- **Payload:** the stored type name `empty` means the deadline has no payload: the payload is `null` and no class is
  loaded, as in Axon Framework 4. Any other type name is loaded as a class, and the stored payload is converted into it
  with the configured `Converter`. A type name that is not a loadable class name behaves as in Axon Framework 4, whose
  serializers return an `UnknownSerializedType` for it: the deadline fires with an `UnknownDeadlinePayload` instead, and
  the job completes. It is a public record in `org.axonframework.deadline` (`axoniq-legacy`) holding the stored type
  name, the revision (nullable) and the raw data as stored (`byte[]` or `String`). It carries no `Converter`: a user who
  wants the content converts the data. Axon Framework 4's `UnknownSerializedType` is not ported, as no handler is
  expected to declare it as its payload type. A `@DeadlineHandler` without a payload parameter (payload type `Object`)
  handles it; for one that declares a payload type that does not match, the deadline is not handled. Both versions
  behave the same way, so during a rolling upgrade the outcome does not depend on the node that picks up the job. On the
  aggregate path (#5004) such a deadline is not dispatched: in Axon Framework 4 it only reached a `@DeadlineHandler`
  without a payload parameter, which has no command counterpart, and dispatching the `UnknownDeadlinePayload` would fail
  with a `NoHandlerForCommandException`, a transient exception that Quartz's default refire policy refires without
  limit. Instead, the `AggregateDeadlineCommandTranslator` (ADR 007) logs a warning naming the deadline, the stored
  type name and the aggregate scope, and the job completes.
- **Message type:** the fired deadline's `MessageType` is derived from the payload class (`AbstractDeadlineManager`'s
  class-based resolver); for an unknown type name, from `UnknownDeadlinePayload`. The type of a `Message` given to
  `schedule(...)` is not stored, and nothing on the firing path matches on a message type. The stored revision is read
  but not used. Nothing on the firing path upcasts or matches on a version.
- **Scope descriptor:** converted into the class named by its stored class name, with the same `Converter` as the
  payload, just as Axon Framework 4 used one serializer for both. `SagaScopeDescriptor` and `AggregateScopeDescriptor`
  keep their Axon Framework 4 class names, and are stored in whatever format the serializer the application gave its
  Axon Framework 4 deadline manager wrote. The converter therefore has to match that serializer, including its
  configuration, such as a `JacksonSerializer`'s `ObjectMapper`. Axon Framework 5 keeps Axon Framework 4's three levels,
  and all three are `Converter`s: the general serializer maps to the `GeneralConverter`, the message serializer to the
  `MessageConverter`, and the event serializer to the `EventConverter`. Spring Boot JobRunr and db-scheduler
  applications therefore pass the `EventConverter`, since Axon Framework 4 auto-configured those managers with the event
  serializer.
- **Metadata:** read untyped with the configured `Converter`, into a `Map<String, Object>`. With a Jackson-based
  converter, that yields only strings, numbers, booleans, maps and lists, whether it stores JSON or a binary Jackson
  format. Each value then becomes a `String`:
    - strings stay unchanged;
    - numbers and booleans become `String.valueOf(...)` (`"3"`, `"true"`);
    - maps and lists are rendered as JSON by a fixed, framework-internal JSON writer, not by the configured converter,
      so the form is the same for every Jackson-based format, and a binary converter, which has no text form, is not
      needed.

  The `XStreamConverter` (ADR 008) is the exception: it already turns every metadata value into its
  `String.valueOf(...)` form, so a nested map stored with XStream arrives as its `toString()`, such as `{key=value}`,
  not as JSON. That converter serves Saga and deadline payloads, whose metadata holds plain values; nested metadata is
  not expected with it.

  Axon Framework 4 metadata could hold any object, and Axon Framework 4's `JacksonSerializer` read a nested value back
  as an untyped `LinkedHashMap` (checked against 4.13.2), so an Axon Framework 4 node fires such a deadline. Failing the
  conversion instead, as Axon Framework 5's event storage does for the same Axon Framework 4 data, would make the
  outcome depend on the node during a rolling upgrade: an Axon Framework 4 node fires the deadline, while an Axon
  Framework 5 node fails it and leaves it to the scheduler's retries until no Axon Framework 4 node is left, after which
  it never fires. Rendering keeps the deadline firing on both versions and keeps the value's content.
- **Identifier and timestamp:** Quartz restores both from the job. JobRunr and db-scheduler never stored them and create
  fresh ones on firing, as Axon Framework 4 did. This qualifies ADR 004, which names the identifier among what a given
  `Message` donates: only Quartz keeps it. ADR 004's point is unaffected, as the tracing context travels in the
  metadata, which every backend stores.
- **Writing, readable by both versions:** the same layout, with the ISO timestamp and the metadata as its
  `Map<String, String>`.
    - **Type:** `Class.getName()` of the payload after the dispatch interceptors ran, and `empty` with the serialized
      `null` for a deadline without a payload, so Axon Framework 4 nodes read it back as `null`. Never the message's
      `MessageType`: a class-based `QualifiedName` is built from package and simple name and drops enclosing classes,
      so `com.example.OrderSaga$PaymentDue` becomes `com.example.PaymentDue`, which cannot be loaded and is not what
      Axon Framework 4 wrote. The `MessageType` only exists on the in-memory `DeadlineMessage`, for interceptors, and
      is never stored.
    - **Revision:** `null`. Axon Framework 4 writes the payload class's `@Revision` value (`AnnotationRevisionResolver`,
      applied while serializing), so `null` only for classes without that annotation. Axon Framework 5 cannot
      reproduce it in general: `@Revision` and the `RevisionResolver` are gone, and the migration recipe only turns
      `@Revision` into `@Event(version = ...)` on payloads of `@EventSourcingHandler` methods, which a deadline payload
      usually is not. `null` is Axon Framework 4's value for the common case of a class without `@Revision`. It is not
      `MessageType.version` either: that is the class-based resolver's constant `0.0.1`, the same for every payload,
      which carries no information and would differ from Axon Framework 4 even for classes without `@Revision`.

      The difference for classes with `@Revision` has no effect. Axon Framework 4 does read the revision back
      (`DeadlineJobDataBinder.deserializeDeadlinePayload(...)` passes it in the `SerializedType` to
      `serializer.deserialize(...)`), but its serializers ignore it: `JacksonSerializer.resolveClassName(...)` and
      `AbstractXStreamSerializer.classForType(...)` resolve the class from the type name alone, and the
      `RevisionResolver` is only used when serializing. Nor does anything upcast: Axon Framework 4 applies upcasters
      only where it reads stored events, as `EventUpcaster`s, while the deadline backends call
      `serializer.deserialize(...)` directly and the `Serializer` has no upcasting step. The rebuilt message does not
      keep the revision either. Only an application's own serializer that overrides `resolveClassName(...)` could have
      chosen a class by revision; Axon Framework 5's `Converter` has no such hook.
    - **Axon Framework 4 nodes:** fire a job written by Axon Framework 5 with their own `DeadlineJob`, `execute(...)` or
      task class, all at unchanged class names.
- **Scope descriptors serialize as Axon Framework 4's serializer did.** Matching a stored scope in
  `cancelAllWithinScope` depends on the backend:
    - Quartz deserializes each stored scope and compares it with `equals` to the given scope, round-tripped through the
      serializer. Both descriptors hold their identifier as an `Object`, which JSON reads back without its Java type: a
      `UUID` comes back as a `String`, a `Long` as an `Integer`, a typed identifier as a `Map`. Matching therefore needs
      two things:
        - The port keeps the round-trip, converting the given scope to a `String` and back to its own class, so that
          both sides lose their types the same way. Without it, a scope with a non-`String` identifier matches no job,
          and nothing is cancelled, without an error.
        - The converter writes the identifier in the same shape as the application's Axon Framework 4 serializer did,
          so that a stored identifier reads back to the same value as the round-tripped one. This is weaker than
          db-scheduler's byte-for-byte requirement below, but a customized Axon Framework 4 mapper, such as one that
          wrote `Long`s as strings or used default typing, still has to be mirrored.
    - db-scheduler serializes the given scope with the configured serializer and compares that `byte[]` or `String` with
      the stored form. The converter therefore has to produce exactly what the application's Axon Framework 4 serializer
      produced for the same descriptor, or a node of one version cannot cancel deadlines the other scheduled. That
      includes Axon Framework 5 cancelling deadlines Axon Framework 4 scheduled before the upgrade. For a
      `JacksonSerializer` with its default `ObjectMapper` that is `{"type":...,"identifier":...}`, which a
      `JacksonConverter` with its default mapper reproduces byte for byte (checked). A customized mapper has to be
      mirrored in the converter.
    - JobRunr does not cancel by scope: `cancelAll` and `cancelAllWithinScope` throw `UnsupportedOperationException` in
      Axon Framework 4 and here, because they need JobRunr Pro. The serialized scope only feeds the job's labels
      (`getCombinedLabel`, MD5-hashed above 44 characters), which keep the same value as long as the serialized form
      matches.

  Both descriptor classes also keep Axon Framework 4's Java serialization members, which a Jackson-based converter
  ignores but an XStream-backed one does not:
    - `ScopeDescriptor` extends `Serializable` again;
    - `SagaScopeDescriptor` declares `serialVersionUID` `4162755498638204691L`;
    - `AggregateScopeDescriptor` declares `serialVersionUID` `3584695571254668002L` and the private `writeObject(...)`
      that resolves the lazily supplied identifier before `defaultWriteObject()`.

  XStream writes a `Serializable` class with `writeObject(...)` in its custom-serialization form, and Axon Framework 4
  stored every `AggregateScopeDescriptor` that way. Without these members, an XStream-backed converter would write the
  plain field form, which differs byte for byte from what Axon Framework 4 stored, so db-scheduler would cancel
  nothing. Restoring them in the port keeps the descriptors' shape in one place. Having the XStream-backed converter
  write that form for this one class instead was rejected, as it would repeat Axon Framework 4's class shape in a
  second place.
- **JobRunr:** keeps `execute(String, String)` with that exact signature, and the deprecated `execute(String)`, so jobs
  of both generations still resolve.
- **db-scheduler:** both details classes declare the `serialVersionUID` that Java serialization computed for their Axon
  Framework 4.13 versions: `DbSchedulerBinaryDeadlineDetails` `-3092788086374166433L`,
  `DbSchedulerHumanReadableDeadlineDetails` `1231422756190086139L` (`serialver` against `axon-messaging` 4.13.2).
  Without them, Java serialization rejects every stored Axon Framework 4 task.
- **Failures are handled as in Axon Framework 4:** the backends read the job data before the unit of work and the
  refire policy apply, as Axon Framework 4 did, and leave a failure to the scheduler library. A job cannot be read
  when the converter does not match the stored format, for example for an XStream-written job. The outcome differs
  per backend:

  | Backend      | Job that cannot be read           | Handler fails                                                |
  |--------------|-----------------------------------|--------------------------------------------------------------|
  | Quartz       | deleted after one attempt         | refired at once, no delay or limit; deleted if non-transient |
  | JobRunr      | retried with back-off             | 10 retries with back-off, 3ⁿ seconds apart, then `FAILED`    |
  | db-scheduler | retried every 5 minutes, no limit | retried every 5 minutes, no limit                            |

  Quartz reports a read failure as a failure without refire, so the one-shot trigger, which cannot fire again, is
  deleted, and with it the non-durable job. JobRunr's `RetryFilter` and db-scheduler's default `OnFailureRetryLater`
  keep the job. Keeping a Quartz job that cannot be read, by rescheduling its trigger, was considered and rejected:
  it would differ from Axon Framework 4, where a read failure on Quartz loses the job on either version.
- **Axon Framework 3.3 layout:** a Quartz job with the `serializedDeadlineMessage` key fails with a `DeadlineException`
  saying it was written in the Axon Framework 3.3 format and must be rescheduled. As for any job that cannot be read,
  Quartz then deletes it, although an Axon Framework 4 node still reads that layout. Only XStream could write that
  layout: it is the object graph of the whole message. Serialized the same way with Axon Framework 4.13.2 (Axon
  Framework 3.3's own graph was not checked), it sits under XStream's `deadline` alias, with the payload behind an
  arbitrary `class` attribute and the timestamp inside a `CachingSupplier` in custom-serialization form. No Axon
  Framework 5 class has that shape. Reading it would need the XStream-backed converter deferred to #592 (see the
  consequences) plus a dedicated mapping of that graph onto an Axon Framework 5 `GenericDeadlineMessage`, for jobs
  pending since 2018. The converter alone would not be enough: the Quartz backend throws the `DeadlineException`
  when it finds the `serializedDeadlineMessage` key, before any converter runs, so the mapping would also change that
  branch of the backend. Whether to add it is left to the decision on #595, and is not in the scope of this ADR.

## Consequences

- An Axon Framework 4 job written with the `JacksonSerializer` fires unchanged with a `JacksonConverter`. This was
  checked against Axon Framework 4.13.2's own serializer output, in both the `byte[]` and the `String` form. It covered
  flat payloads, lists, maps, underscored field names, `Instant` (written by `JavaTimeModule` as epoch seconds), both
  scope descriptors, and metadata with string, number and boolean values. Metadata with object values fails a plain
  `Map<String, String>` conversion, hence the untyped read above.
- The converter has to read what the application's Axon Framework 4 serializer wrote. An application that customized its
  `ObjectMapper` (naming strategy, modules, visibility) configures the `JacksonConverter`'s mapper the same way.
- Axon Framework 4's `Jackson3Serializer` (4.13.0 and later, `axon.serializer.*=jackson3` in Spring Boot) is handled
  the same way. It uses a Jackson 3 `JsonMapper`, as the `JacksonConverter` does, so with its defaults it writes the
  same JSON. Its `defaultTyping()` option writes type information for `Object` fields and maps, such as a scope
  identifier or metadata values. The converter's `JsonMapper` then has to activate the same default typing, or it
  misreads them: Quartz's cancel by scope matches no job, and db-scheduler's serialized scopes differ. A
  `Jackson3SerializerCustomizer` is mirrored like a customized `ObjectMapper`. Unlike the `JacksonSerializer`, this is
  not checked yet; the compatibility tests cover it, with its defaults and with `defaultTyping()`.
- **XStream-written jobs are not supported for now.** That includes Quartz's Axon Framework 4 default, and the JobRunr
  and db-scheduler managers of Axon Framework 4 Spring Boot applications that did not configure `axon.serializer.*`,
  which used XStream. Axon Framework 5 has no XStream support, and this decision ships no `Converter` that reads XStream
  XML. XStream support is deferred to [#592](https://github.com/AxonIQ/axoniq-framework/issues/592): a `Converter` in
  `axoniq-legacy` that delegates to an application-configured `XStream` instance, an optional dependency. It is
  compatible by construction, including the application's aliases and converters, and would let XStream applications
  read their jobs and roll an upgrade. It brings back the dependency Axon Framework 5 dropped, with XStream's security
  history (a type allowlist is required) and possibly `--add-opens` on recent JDKs, which these applications already
  lived with on Axon Framework 4.
- **A Jackson `XmlMapper` is not a feasible workaround, and is not offered.** A `JacksonConverter` over a plain
  `XmlMapper` reads XStream XML only partly, and silently: escaped field names (`customer__ref`), map entries, `class`
  attributes and the custom-serialization form of `AggregateScopeDescriptor` are lost or fail. Since the scope
  descriptor and the metadata are among them, no XStream-written job fires correctly that way. A configured `XmlMapper`
  (a naming strategy for XStream's escaping, plus deserializers for map entries, `class` attributes and
  `AggregateScopeDescriptor`) read every tested case except XStream's object references (`reference="../a"`), but not
  application-specific aliases and converters or arbitrary `class` attributes. It only reads: Axon Framework 4.13.2's
  `XStreamSerializer` cannot read what that `XmlMapper` writes. Jackson names the root element after the simple class
  name, while XStream resolves the class from it, so every case failed. With the root element set to XStream's form of
  the class name (`$` escaped as `_-`), plain fields, `Instant`, enums and nested objects were read, but lists (item
  elements), maps and the metadata (`<entry>` pairs, `<meta-data>`) and `AggregateScopeDescriptor` (custom
  serialization) failed. A `SagaScopeDescriptor`'s identifier and other `Object`-typed fields were silently read as an
  empty `java.lang.Object`, for lack of a `class` attribute, so such a saga deadline would fire on an Axon Framework 4
  node and reach no saga. Full compatibility would rebuild XStream's object model: relative object references, `class`
  attributes written only where the runtime type differs from the declared one and named by XStream's aliases, a
  converter per JDK type, the custom-serialization form of every class with `writeObject` / `readObject`, field access
  without constructors, and each application's own aliases, omitted fields, implicit collections and converters. Every
  shape it misses fails, or is misread silently. Even a bounded profile (hand-written serializers for the metadata and
  both scope descriptors, plain payload classes of simple types, and a failure on anything outside) emulates a format
  someone has to maintain.
- A deadline payload is converted as it was stored, with no upcasting, exactly as in Axon Framework 4. A payload class
  changed incompatibly while deadlines scheduled with its old shape are still pending fails to convert on Axon Framework
  5, as it would have failed to deserialize on Axon Framework 4; only the converter's own tolerance (such as ignoring
  unknown properties) helps. The migration guide advises keeping deadline payload classes backward compatible, and
  not renaming or moving them in the release that upgrades, until those deadlines have fired. A node that lacks the
  stored class fires the deadline with an `UnknownDeadlinePayload`, so a typed `@DeadlineHandler` never sees it.
- A payload type name that is an XStream alias or a custom type mapping cannot be resolved either, and fires with an
  `UnknownDeadlinePayload`. Axon Framework 4's `XStreamSerializer` stored the alias as the type name
  (`xStream.getMapper().serializedClass(...)`), and the backends load the type name as a class before the converter
  runs. An XStream-backed converter alone therefore does not make aliased payloads resolve: that needs a lookup of
  stored type names that a converter can take over, which the legacy saga stores need for aliased saga types as well.
  It is left to the decision on [#595](https://github.com/AxonIQ/axoniq-framework/issues/595), and changes no stored
  format.
- AxonFramework's `axon-5/api-changes/10-stored-format-changes.md#deadlines` is corrected: the Axon Framework 4 layout
  is kept, no `QualifiedName` is stored, and XStream-written and Axon Framework 3.3 jobs are not readable.
- Each persistent backend is verified against job data built by Axon Framework 4.13.2's own classes. The test follows
  the existing `Af4SerializerCompatibilityTest` pattern: the `axon4.serializer.version` jar copy and a child-first class
  loader. Mutation check: adding a type field or renaming a key or field must fail it.
- **Rolling upgrades work in both directions** for applications on the Axon Framework 4 `JacksonSerializer`. This was
  checked: Axon Framework 4.13.2's `JacksonSerializer` read everything a `JacksonConverter` wrote (payloads, `Instant`,
  maps, underscored field names, metadata, both scope descriptors, as `byte[]` and as `String`). Both scope descriptors
  serialize to the same JSON in both versions. Jackson 3's differences did not matter for reading: properties are
  ordered alphabetically, and `Instant` is written as an ISO string, which Axon Framework 4's default serializer reads
  through its `JavaTimeModule`.
- Nested Axon Framework 4 metadata values reach Axon Framework 5 handlers as JSON strings, not as maps. Axon Framework
  5's `@MetadataValue` resolution does not convert: a required parameter of another type never matches a string, and an
  optional one fails on invocation. Turning such values back into structured objects would need a parameter resolver
  that converts metadata strings into the declared type. That applies to every Axon Framework 4 message with non-string
  metadata, not only to deadlines, and is not part of this decision. Keeping the content as text leaves it possible.
- An Axon Framework 5 consumer cannot tell a rendered nested value from a string that already contained JSON in Axon
  Framework 4.
- XML read untyped loses some shape: it does not distinguish a one-element list from a single value, and attributes
  become fields. The rendered JSON can then differ in structure from the original. Out of the box this does not arise,
  since XStream-written jobs are not supported. An XStream-backed converter
  ([#592](https://github.com/AxonIQ/axoniq-framework/issues/592)) does not read untyped: it turns each value into its
  `String.valueOf(...)` form, so a `UUID` or an `Instant` arrives as its usual text, and a nested map as its
  `toString()` rather than as JSON (see the Metadata decision above). Axon Framework 4 also reads
  stored metadata by XStream's root element, `<meta-data>`, which a plain map does not produce. How metadata is
  written for and read from such a converter is left to the decision on #595, and changes no stored format for
  Jackson-based converters.
- Jobs written by Axon Framework 4's deprecated `JavaSerializer` are not readable either: Axon Framework 5 has no
  converter for Java serialization.
- An Axon Framework 4 node sees the metadata of a job written by Axon Framework 5 as strings only (`"3"`, not `3`). This
  follows from Axon Framework 5's `Map<String, String>` metadata, and only affects Axon Framework 4 code that casts
  metadata values.
- Applications on XStream cannot upgrade with both versions running: Axon Framework 5 cannot read their jobs, and their
  Axon Framework 4 nodes cannot read the JSON Axon Framework 5 writes. A converter over a Jackson `XmlMapper`, as above,
  would not change that: Axon Framework 4 nodes cannot read its XML either, and a saga deadline written that way would
  fire on them without its saga identifier, reaching no saga. With Axon Framework 4's failure handling, a job that a
  node of the other version picks up is lost on Quartz after one attempt; on JobRunr it is retried for about a day, and
  on db-scheduler every 5 minutes, so it can still reach a node of its own version. Until an XStream-backed `Converter`
  exists ([#592](https://github.com/AxonIQ/axoniq-framework/issues/592)) and the deadline managers use it
  ([#595](https://github.com/AxonIQ/axoniq-framework/issues/595)), they let their deadlines fire, or reschedule
  them, before switching, and the migration guide states prominently that a rolling upgrade is not supported for them.
  Changing the Axon Framework 4 serializer to Jackson first does not help on its own, since the stored XStream jobs stay
  unreadable for the new serializer.
- Both versions share the scheduler library's own store. Axon Framework 4.13.0 to 4.13.2 were built against Quartz
  2.4.0, JobRunr 8.0.1 and db-scheduler 16.6.0. Later 4.13 releases use Quartz 2.4.1, JobRunr 8.7.1 (4.13.3) or 8.8.2
  (from 4.13.4), and db-scheduler 16.12.0. `axoniq-legacy` is the only module that uses these libraries and manages
  their versions itself (Quartz 2.5.2, JobRunr 8.8.2, db-scheduler 16.12.0, the same major versions). Axon Framework's
  `axon-parent` stops managing them, in a companion build change to
  [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005). Whether the library's store is compatible across
  those versions is the library's concern. The migration guide advises running the same library version on all nodes
  during the rollout.
- The compatibility tests cover both directions, per persistent backend: jobs built by Axon Framework 4.13.2's own
  classes fire on the ported backend, jobs written by the ported backend are read by Axon Framework 4.13.2's own
  `DeadlineJob`, `DeadlineDetails` and details classes, and a cancel from one version removes a deadline scheduled by
  the other. The cases include a deadline without a payload (type `empty`) and a payload that is a nested class, where
  `Class.getName()` and the class-based `QualifiedName` differ.
