# Message Types — Always Resolve Through a MessageTypeResolver

## Rule

**Never derive a `MessageType` or `QualifiedName` from a payload class yourself** — not with
`new MessageType(SomeClass.class)`, not with `new QualifiedName(SomeClass.class)`, not by hand-copying the values of an
`@Event` / `@Command` / `@Query` annotation into a string. Always resolve them through a `MessageTypeResolver`:

- in framework code and components: the application's `MessageTypeResolver` component
  (`config.getComponent(MessageTypeResolver.class)`), or a resolver passed in through configuration;
- in handlers: inject `MessageTypeResolver` as a parameter, or use an API that resolves for you
  (`EventGateway.publish(...)`, `EventSinks.publishPayloads(sink, context, typeResolver, payloads...)`);
- in tests that need a typed message for an annotated payload: `new AnnotationMessageTypeResolver().resolveOrThrow(payload)`.

APIs that accept event or message types should accept payload classes and resolve them, rather than (only) asking the
caller for a `QualifiedName`.

## Why

`MessageType(Class)` and `QualifiedName(Class)` use the class name and ignore `@Event`, `@Command` and `@Query`. A
message published with such a type silently misses every handler that expects the annotated type — nothing fails, the
message just never arrives.

## Allowed

Literal types that are not derived from a class are fine, e.g. a fixture for a record written by another system
(`new MessageType("com.example.LegacyParcel", "1.0.0")`), or decoding a type from stored or transported headers through
an `EventTypeResolver`.
