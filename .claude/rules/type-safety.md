# Type Safety Rules

## TypeReference for Generic Types

When using any API that accepts `Type`, **always use `TypeReference`** instead of raw `Class` when the target type is generic (e.g., `Map<String, String>`, `List<SomeType>`). This preserves full generic type information at runtime and avoids `@SuppressWarnings("unchecked")`.

### Pattern: Static TypeReference Constant

```java
// CORRECT - preserves Map<String, String> generic info, no unchecked warning
private static final TypeReference<HashMap<String, String>> METADATA_MAP_TYPE_REF = new TypeReference<>() {
};

Map<String, String> result = converter.convert(data, METADATA_MAP_TYPE_REF.getType());
```

When a Jackson `Converter` reads the result back with default typing enabled, use a **concrete** type like
`HashMap` for the `TypeReference`'s type argument instead of the `Map` interface: with default typing, a
non-concrete target type expects a type id in the payload, and writing a concretely-typed source object (e.g.
`Metadata`) never adds one.

The write side must match: convert `new HashMap<>(metadata)` instead of the `Metadata` instance itself. Under
`DefaultTyping.NON_FINAL`, writing a non-final source type (like `Metadata`) adds a type id to the payload, which
then fails to deserialize into the concrete `HashMap` target used on the read side.

```java
// WRONG - loses generic info, produces unchecked cast warning
@SuppressWarnings("unchecked")
Map<String, String> result = converter.convert(data, Map.class);
```

### Rules

- Define `TypeReference` constants as `private static final` fields when the type is reused
- Use `new TypeReference<TargetType>() {}` (anonymous subclass captures the generic type)
- Pass `.getType()` to APIs expecting `java.lang.reflect.Type`
- For simple non-generic types, `Class<T>` is fine (e.g., `converter.convert(data, String.class)`)

### Existing Examples in Codebase

- `JpaSequencedDeadLetterQueue`: `DIAGNOSTICS_MAP_TYPE_REF` for `HashMap<String, String>`
- `AggregateBasedJpaEventStorageEngine` (upstream Axon Framework, not this repo): `METADATA_MAP_TYPE_REF` still uses
  `Map<String, String>` — this is the pattern to avoid, not an example to follow
