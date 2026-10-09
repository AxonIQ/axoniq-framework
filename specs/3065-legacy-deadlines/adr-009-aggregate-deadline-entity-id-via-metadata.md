# ADR 009: Resolve a translated aggregate deadline's target entity from metadata, not the payload (issue [#5004](https://github.com/AxonIQ/AxonFramework/issues/5004))

Date: 2026-10-08

Status: accepted

Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [ADR 001](adr-001-aggregate-deadline-command-translation.md) (aggregate deadlines as commands), [ADR 007](adr-007-aggregate-deadline-delivery-through-scope-aware.md) (aggregate deadline delivery through `ScopeAware`)

## Context

ADR 001 and ADR 007 both state that `AggregateDeadlineCommandTranslator` dispatches the deadline's payload
unchanged, and that the dispatched command's routing key — the `@TargetEntityId`-annotated field `AnnotationBasedEntityIdResolver`
reads to decide which entity instance to load — comes from the payload itself, not from the bridge: "the real
routing key for `CommandGateway`/`CommandBus` comes from `@TargetEntityId` on the payload class, not from metadata
supplied by this bridge... this bridge cannot inject one generically, since it has no knowledge of the payload's
shape."

That assumption does not hold in practice. In Axon Framework 4, the target aggregate of a scheduled deadline was
identified entirely by the `AggregateScopeDescriptor` AF4's own deadline machinery built from the aggregate's
`@AggregateIdentifier` — independent of whatever payload object the developer happened to schedule alongside it. A
deadline payload therefore essentially never carries its own aggregate's identifier: there was no reason for AF4
code to duplicate "the ID of the aggregate this deadline fires inside" into the deadline's own payload. After
migration, even a payload class a user dutifully annotates with `@TargetEntityId` typically has that field unset on
an already-scheduled, deserialized deadline — the identifier that actually identifies the target entity is the one
`send(...)` already receives as `scopeDescription`, not anything recoverable from the payload.

### What was investigated

AF5 keeps two separate, independently pluggable concerns, both defaulting to reflecting over the command's
*payload*, never its metadata:

- **Entity-id resolution** (`EntityIdResolver`, default `AnnotationBasedEntityIdResolver`, reads `@TargetEntityId`) —
  decides which entity instance `@InjectEntity`/an autodetected `@CommandHandler` loads.
- **Command routing** (`RoutingStrategy`, default `AnnotationRoutingStrategy`, reads `@Command(routingKey=...)`) —
  decides `CommandBus` distribution and sequencing.

`RoutingStrategy` already ships a metadata-based alternative, `MetadataRoutingStrategy`. No equivalent
metadata-based `EntityIdResolver` existed in Axon Framework at the time of this ADR. Entity-id resolution for an
autodetected `@EventSourcedEntity` (plain Java) or `@EventSourced` (Spring) also had no application-wide override
point: `AnnotatedEventSourcedEntityModule` always reflectively instantiated whichever `EntityIdResolverDefinition`
class the entity's own annotation named (default `AnnotationBasedEntityIdResolverDefinition`), ignoring the
`Configuration` it was handed — even though `EntityIdResolverDefinition.createIdResolver(...)` already takes a
`Configuration` parameter, anticipating a configuration-aware implementation nothing yet provided.

The payload itself cannot be generically rewritten to carry the identifier, for the same reason ADR 001 already
gives: the dispatched object's Java type must stay exactly what the user's `@CommandHandler` declares, or `CommandBus`
routing by `QualifiedName` breaks.

## Decision

Axon Framework gains the missing building blocks (`org.axonframework.modelling`):

- `MetadataEntityIdResolver<ID>` — resolves the identifier from a named metadata entry instead of the payload.
- `FallbackEntityIdResolver<ID>` — tries a primary `EntityIdResolver`, falling back to a secondary one only if the
  primary fails to resolve an identifier.
- `AnnotatedEventSourcedEntityModule` now consults an optional `Configuration`-registered `EntityIdResolverDefinition`
  when an entity's own annotation leaves `entityIdResolverDefinition` at its default, before falling back to
  constructing `AnnotationBasedEntityIdResolverDefinition` directly. An entity naming an explicit, non-default
  definition keeps that choice untouched. This is purely additive: no application registers such a component today,
  so every existing application's behavior is unchanged; only an application that explicitly registers one — as
  `axoniq-legacy` does — sees different behavior, and only for entities left at the default.

`AggregateDeadlineCommandTranslator.send(...)` writes the `AggregateScopeDescriptor`'s identifier into the dispatched
command's metadata, under the public constant `AggregateDeadlineCommandTranslator.DESCRIPTOR_BASED_ID`.
`axoniq-legacy` also gains `AggregateDeadlineEntityIdResolverDefinition`, composing
`new FallbackEntityIdResolver<>(new AnnotationBasedEntityIdResolver<>(), new MetadataEntityIdResolver(AggregateDeadlineCommandTranslator.DESCRIPTOR_BASED_ID))` —
usable either named directly on a migrated entity's `@EventSourcedEntity(entityIdResolverDefinition = ...)` attribute,
or registered as the `Configuration`-level default for every entity reached through a translated deadline that does
not itself declare `@TargetEntityId`.

That `Configuration`-level registration does not need to be written by every migrating application: `axoniq-legacy`
also ships `AggregateDeadlineEntityIdResolverConfigurationEnhancer`, a `ConfigurationEnhancer` that registers
`AggregateDeadlineEntityIdResolverDefinition` as the application-wide `EntityIdResolverDefinition` default, guarded by
`!registry.hasComponent(EntityIdResolverDefinition.class)` so an application's own explicit registration — or another
enhancer's — always wins. `ConfigurationEnhancer`s are discovered through the JVM `ServiceLoader`
(`META-INF/services/org.axonframework.common.configuration.ConfigurationEnhancer`), so this is a genuine single spot
of configuration: an application gets the fallback purely by having `axoniq-legacy` on its classpath, with no
registration call of its own, matching the `ConfigurationEnhancer`-as-sensible-default convention the
`order()` javadoc itself describes (a low/negative order, here `Integer.MIN_VALUE`).

This keeps ADR 001's and ADR 007's core decisions (command dispatch, no `Repository`/`EntityMetamodel` involvement,
`ScopeAware` as the delivery mechanism) unchanged. It supersedes only the statement that the routing key "comes from
`@TargetEntityId` on the payload class, not from metadata supplied by this bridge" — the bridge now supplies it,
specifically so a migrated entity does not depend on its payload carrying an identifier it never had under Axon
Framework 4.

## Consequences

- A payload that *does* carry a populated `@TargetEntityId` field still resolves through it first: the fallback is
  only consulted when annotation-based resolution finds no identifier, so this is additive, not a behavior change
  for payloads that happen to carry their own identifier.
- A migrating entity needs no explicit opt-in: merely having `axoniq-legacy` on the classpath wires the metadata
  fallback for every entity left at its default `entityIdResolverDefinition`, through
  `AggregateDeadlineEntityIdResolverConfigurationEnhancer`. An entity (or application) that explicitly registers its
  own `EntityIdResolverDefinition` is unaffected — the enhancer's `hasComponent(...)` guard steps aside for it.
- **Known limitation**: the `Configuration`-registered default only benefits an `@EventSourcedEntity`/`@EventSourced`
  entity — `AnnotatedEventSourcedEntityModule` is the only place consulting it. A migrated state-stored aggregate (a
  typical Axon Framework 4 JPA aggregate) or a declaratively configured entity module does not consult this override
  point at all, so a deadline translated for either still fails with `EntityIdResolutionException` unless that
  entity's own command handler payload happens to carry a usable identifier. Closing this gap for state-stored or
  declarative entities needs its own override point in Axon Framework; none exists today.
- Tests: `MetadataEntityIdResolverTest` and `FallbackEntityIdResolverTest` (core Axon Framework), a focused
  `AnnotatedEventSourcedEntityModule` test proving the `Configuration` override is consulted only when the attribute
  is left at its default, and the `axoniq-legacy`/`axoniq-legacy-test` translator tests, whose entity-routing test
  uses a payload with no identifying field at all and an entity left entirely at its default resolver — the realistic
  migrated-deadline case — and is confirmed to fail without this wiring.
