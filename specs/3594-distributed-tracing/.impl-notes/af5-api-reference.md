# AF5 API Reference (for porting distributed tracing)

Source repo (AF5, `org.axonframework:*:5.2.0-SNAPSHOT`):
`/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework5`

Signatures below are copied verbatim from the AF5 clone. Where a method body
is irrelevant it is elided with `{ ... }`.

> IMPORTANT CONTEXT: AF5 **already contains a full tracing implementation** that
> mirrors the AF4 code we are porting. It lives under:
> - `messaging/.../org/axonframework/messaging/tracing/` (`SpanFactory`, `Span`, `SpanScope`, `NoOpSpanFactory`, `MultiSpanFactory`, `SpanUtils`, `HandlerSpanFactory`, `TracingHandlerEnhancerDefinition`, `SpanAttributesProvider`, `attributes/*`)
> - `messaging/.../commandhandling/tracing/` (`CommandBusSpanFactory`, `DefaultCommandBusSpanFactory`, `TracingCommandBus`)
> - `messaging/.../eventhandling/tracing/`, `.../queryhandling/tracing/`
> - `extensions/tracing/tracing-opentelemetry/.../extension/tracing/opentelemetry/` (`OpenTelemetrySpanFactory`, `OpenTelemetrySpan`, `MetadataContextSetter`, `MetadataContextGetter`)
>
> The new modules we are building define their **own** `SpanFactory`/`Span`/`SpanScope`
> and decorators. The signatures here are the AF5 component interfaces those
> decorators must wrap. Cross-check against the existing AF5 tracing code when porting.

---

## CommandBus

FQN: `org.axonframework.messaging.commandhandling.CommandBus`
Path: `messaging/src/main/java/org/axonframework/messaging/commandhandling/CommandBus.java`

`CommandBus extends CommandHandlerRegistry<CommandBus>, DescribableComponent`.

```java
public interface CommandBus extends CommandHandlerRegistry<CommandBus>, DescribableComponent {

    CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                     @Nullable ProcessingContext processingContext);
}
```

KEY DIFFERENCES vs AF4:
- `CommandMessage` and `CommandResultMessage` are **no longer generic** (`CommandMessage<?>` -> `CommandMessage`).
- `dispatch` returns `CompletableFuture<CommandResultMessage>` (async-first), and takes a nullable `ProcessingContext`.

### CommandHandlerRegistry

FQN: `org.axonframework.messaging.commandhandling.CommandHandlerRegistry`
Path: `messaging/src/main/java/org/axonframework/messaging/commandhandling/CommandHandlerRegistry.java`

```java
public interface CommandHandlerRegistry<S extends CommandHandlerRegistry<S>> {

    S subscribe(QualifiedName name,
                CommandHandler commandHandler);

    default S subscribe(Set<QualifiedName> names,
                        CommandHandler commandHandler) { ... }

    default S subscribe(CommandHandlingComponent handlingComponent) {
        return subscribe(handlingComponent.supportedCommands(), handlingComponent);
    }
}
```

### CommandHandlingComponent

FQN: `org.axonframework.messaging.commandhandling.CommandHandlingComponent`
Path: `messaging/src/main/java/org/axonframework/messaging/commandhandling/CommandHandlingComponent.java`

```java
public interface CommandHandlingComponent extends CommandHandler, DescribableComponent {

    Set<QualifiedName> supportedCommands();
}
```

### CommandHandler (functional interface)

FQN: `org.axonframework.messaging.commandhandling.CommandHandler`
Path: `messaging/src/main/java/org/axonframework/messaging/commandhandling/CommandHandler.java`

`CommandHandler extends MessageHandler` (the marker interface `org.axonframework.messaging.core.MessageHandler`).

```java
@FunctionalInterface
public interface CommandHandler extends MessageHandler {

    MessageStream.Single<CommandResultMessage> handle(CommandMessage command,
                                                      ProcessingContext context);
}
```

Note: handler returns `MessageStream.Single<CommandResultMessage>` (not a `CompletableFuture`).

---

## Message

FQN: `org.axonframework.messaging.core.Message`
Path: `messaging/src/main/java/org/axonframework/messaging/core/Message.java`

`Message` is **no longer generic** in AF5 (was `Message<T>` in AF4).

Relevant signatures (verbatim):

```java
public interface Message {

    Context.ResourceKey<Message> RESOURCE_KEY = Context.ResourceKey.withLabel("Message");

    static ProcessingContext addToContext(ProcessingContext context, Message message) { ... }
    static @Nullable Message fromContext(ProcessingContext context) { ... }

    String identifier();

    MessageType type();

    @Nullable Object payload();

    @Nullable <T> T payloadAs(Class<T> type, @Nullable Converter converter);
    @Nullable <T> T payloadAs(Class<T> type);
    @Nullable <T> T payloadAs(TypeReference<T> type, @Nullable Converter converter);
    @Nullable <T> T payloadAs(TypeReference<T> type);
    @Nullable <T> T payloadAs(Type type, @Nullable Converter converter);

    Class<?> payloadType();

    Metadata metadata();

    Message withMetadata(Map<String, String> metadata);

    Message andMetadata(Map<String, @Nullable String> metadata);

    default Message withConvertedPayload(Class<?> type, Converter converter) { ... }
    default Message withConvertedPayload(TypeReference<?> type, Converter converter) { ... }
    Message withConvertedPayload(Type type, Converter converter);
}
```

KEY DIFFERENCES vs AF4:
- Metadata accessor is now `metadata()` (NOT `getMetaData()`), returning `Metadata`.
- Copy-with-metadata: `withMetadata(Map<String, String>)` (replace) and
  `andMetadata(Map<String, @Nullable String>)` (merge). AF4 used `withMetaData` / `andMetaData`.
- Payload accessor is `payload()` / `payloadType()` (NOT `getPayload()`).
- Message identifier is `identifier()` (NOT `getIdentifier()`).
- The `RESOURCE_KEY` stores the current handler's `Message` in the `ProcessingContext`.

### CommandMessage

FQN: `org.axonframework.messaging.commandhandling.CommandMessage`
Path: `messaging/src/main/java/org/axonframework/messaging/commandhandling/CommandMessage.java`

`CommandMessage extends Message` (NOT generic).

```java
public interface CommandMessage extends Message {

    Optional<String> routingKey();

    OptionalInt priority();

    @Override CommandMessage withMetadata(Map<String, String> metadata);
    @Override CommandMessage andMetadata(Map<String, @Nullable String> metadata);
    @Override default CommandMessage withConvertedPayload(Class<?> type, Converter converter) { ... }
    @Override default CommandMessage withConvertedPayload(TypeReference<?> type, Converter converter) { ... }
    @Override CommandMessage withConvertedPayload(Type type, Converter converter);
}
```

### Metadata

FQN: `org.axonframework.messaging.core.Metadata`  (renamed from AF4's `MetaData`)
Path: `messaging/src/main/java/org/axonframework/messaging/core/Metadata.java`

`Metadata implements Map<String, @Nullable String>` — i.e. metadata is `Map<String,String>`
(both keys and values are `String`; values may be null). **NOT** `Map<String,?>`.
Immutable (`put`/`remove`/`clear`/`putAll` throw `UnsupportedOperationException`).

Relevant public methods (verbatim):

```java
public class Metadata implements Map<String, @Nullable String> {

    public Metadata(Map<String, String> items) { ... }

    public static Metadata emptyInstance() { ... }
    public static Metadata from(@Nullable Map<String, @Nullable String> metadataEntries) { ... }
    public static Metadata with(String key, @Nullable String value) { ... }

    public Metadata and(String key, @Nullable String value) { ... }
    public Metadata andIfNotPresent(String key, Supplier<String> value) { ... }
    public Metadata mergedWith(Map<String, @Nullable String> additionalEntries) { ... }
    public Metadata withoutKeys(Set<String> keys) { ... }
    public Metadata subset(String... keys) { ... }

    @Override @Nullable public String get(Object key) { ... }
    // Map operations: containsKey, keySet, values, entrySet, size, isEmpty all delegate to an unmodifiable map.
}
```

Reading metadata from a message: `message.metadata().get("key")`.
Producing a copy with added metadata:
`message.andMetadata(Map.of("key", "value"))` or `message.andMetadata(existing.metadata().and("key","value"))`.

---

## MessageType

FQN: `org.axonframework.messaging.core.MessageType`
Path: `messaging/src/main/java/org/axonframework/messaging/core/MessageType.java`

A `record MessageType(QualifiedName qualifiedName, String version)`.

```java
public record MessageType(QualifiedName qualifiedName, String version) {

    public static final String DEFAULT_VERSION = "0.0.1";

    public MessageType(QualifiedName name) { ... }            // version defaults to DEFAULT_VERSION
    public MessageType(String qualifiedName) { ... }
    public MessageType(String qualifiedName, String version) { ... }
    public MessageType(String namespace, String localName, String version) { ... }
    public MessageType(Class<?> clazz, String version) { ... }
    public MessageType(Class<?> clazz) { ... }

    public String name();                                     // == qualifiedName.toString()
    public QualifiedName qualifiedName();                     // record accessor
    public String version();                                  // record accessor
    public static MessageType fromString(String messageTypeString) { ... }
    @Override public String toString();                       // "<qualifiedName>#<version>"
}
```

### QualifiedName

FQN: `org.axonframework.messaging.core.QualifiedName`
Path: `messaging/src/main/java/org/axonframework/messaging/core/QualifiedName.java`

A `record QualifiedName(String name)`.

```java
public record QualifiedName(String name) {

    public QualifiedName(String namespace, String localName) { ... }
    public QualifiedName(Class<?> clazz) { ... }              // uses Class.getName()

    @Nullable public String namespace();                      // text before last '.'
    public String localName();                                // text after last '.'
    public String name();                                     // record accessor (full name)
    public String fullName();                                 // == name
    @Override public String toString();                       // == name
}
```

How to get a message's qualified name string:
`message.type().qualifiedName().toString()` or `message.type().name()`.
The version-less name is `message.type().qualifiedName().name()`.

---

## ProcessingContext / ProcessingLifecycle

FQN: `org.axonframework.messaging.core.unitofwork.ProcessingContext`
Path: `messaging/src/main/java/org/axonframework/messaging/core/unitofwork/ProcessingContext.java`

`ProcessingContext extends ProcessingLifecycle, ApplicationContext, Context`.

Resource API (mutable, from `ProcessingContext`):

```java
public interface ProcessingContext extends ProcessingLifecycle, ApplicationContext, Context {

    @Override default <T> ProcessingContext withResource(ResourceKey<T> key, T resource) { ... }
    <T> T putResource(ResourceKey<T> key, T resource);
    <T> T updateResource(ResourceKey<T> key, UnaryOperator<@Nullable T> resourceUpdater);
    <T> T putResourceIfAbsent(ResourceKey<T> key, T resource);
    <T> T computeResourceIfAbsent(ResourceKey<T> key, Supplier<T> resourceSupplier);
    <T> T removeResource(ResourceKey<T> key);
    <T> boolean removeResource(ResourceKey<T> key, T expectedResource);
}
```

Read-side resource API is inherited from `Context` (see below):
`getResource(ResourceKey)`, `containsResource(ResourceKey)`, `withResource(...)`.
There is **no** `Optional`-returning resource getter; `getResource` returns `@Nullable T`.

FQN: `org.axonframework.messaging.core.unitofwork.ProcessingLifecycle`
Path: `messaging/src/main/java/org/axonframework/messaging/core/unitofwork/ProcessingLifecycle.java`

Lifecycle registration (verbatim signatures):

```java
public interface ProcessingLifecycle {

    boolean isStarted();
    boolean isError();
    boolean isCommitted();
    boolean isCompleted();

    // Core registration: returns a CompletableFuture-carrying action in a phase.
    ProcessingLifecycle on(Phase phase, Function<ProcessingContext, CompletableFuture<?>> action);
    default ProcessingLifecycle runOn(Phase phase, Consumer<ProcessingContext> action) { ... }

    default ProcessingLifecycle onPreInvocation(Function<ProcessingContext, CompletableFuture<?>> action) { ... }
    default ProcessingLifecycle runOnPreInvocation(Consumer<ProcessingContext> action) { ... }
    default ProcessingLifecycle onInvocation(Function<ProcessingContext, CompletableFuture<?>> action) { ... }
    default ProcessingLifecycle runOnInvocation(Consumer<ProcessingContext> action) { ... }
    default ProcessingLifecycle onPostInvocation(Function<ProcessingContext, CompletableFuture<?>> action) { ... }
    default ProcessingLifecycle runOnPostInvocation(Consumer<ProcessingContext> action) { ... }
    default ProcessingLifecycle onPrepareCommit(Function<ProcessingContext, CompletableFuture<?>> action) { ... }
    default ProcessingLifecycle runOnPrepareCommit(Consumer<ProcessingContext> action) { ... }
    default ProcessingLifecycle onCommit(Function<ProcessingContext, CompletableFuture<?>> action) { ... }
    default ProcessingLifecycle runOnCommit(Consumer<ProcessingContext> action) { ... }
    default ProcessingLifecycle onAfterCommit(Function<ProcessingContext, CompletableFuture<?>> action) { ... }
    default ProcessingLifecycle runOnAfterCommit(Consumer<ProcessingContext> action) { ... }

    ProcessingLifecycle onError(ErrorHandler action);
    ProcessingLifecycle whenComplete(Consumer<ProcessingContext> action);
    default ProcessingLifecycle doFinally(Consumer<ProcessingContext> action) { ... }

    @FunctionalInterface
    interface ErrorHandler {
        void handle(ProcessingContext processingContext, Phase phase, Throwable error);
    }

    interface Phase {
        int order();
        default boolean isBefore(Phase other) { ... }
        default boolean isAfter(Phase other) { ... }
    }

    enum DefaultPhases implements Phase {
        PRE_INVOCATION(-10000),
        INVOCATION(0),
        POST_INVOCATION(10000),
        PREPARE_COMMIT(20000),
        COMMIT(30000),
        AFTER_COMMIT(40000);
        @Override public int order();
    }
}
```

Note: there is NO `onRollback`; error handling is `onError(ErrorHandler)`. `doFinally` runs on both error and completion.

---

## ResourceKey

FQN: `org.axonframework.messaging.core.Context.ResourceKey` (nested final class in `Context`).
Path: `messaging/src/main/java/org/axonframework/messaging/core/Context.java`

There is **no** `ResourceKey.create(...)`. Factory is `Context.ResourceKey.withLabel(...)`.

```java
final class ResourceKey<T> {

    public static <T> ResourceKey<T> withLabel(@Nullable String label) {
        return new ResourceKey<>(label);
    }

    public @Nullable String label();
    @Override public String toString();   // "ResourceKey@<hex>[label]"
}
```

Usage: `private static final Context.ResourceKey<MySpan> KEY = Context.ResourceKey.withLabel("mySpan");`

### Context (resource read API)

FQN: `org.axonframework.messaging.core.Context`
Path: `messaging/src/main/java/org/axonframework/messaging/core/Context.java`

```java
public interface Context {

    static <T> Context with(ResourceKey<T> key, T value) { ... }
    static Context empty() { ... }

    boolean containsResource(ResourceKey<?> key);
    @Nullable <T> T getResource(ResourceKey<T> key);
    <T> Context withResource(ResourceKey<T> key, T resource);

    @Internal Map<ResourceKey<?>, Object> resources();
}
```

---

## ComponentRegistry / ConfigurationEnhancer / Configuration

### ConfigurationEnhancer

FQN: `org.axonframework.common.configuration.ConfigurationEnhancer`
Path: `common/src/main/java/org/axonframework/common/configuration/ConfigurationEnhancer.java`

```java
@FunctionalInterface
public interface ConfigurationEnhancer {

    void enhance(ComponentRegistry registry);   // receives a ComponentRegistry

    default int order() { return 0; }
}
```

### ComponentRegistry

FQN: `org.axonframework.common.configuration.ComponentRegistry`
Path: `common/src/main/java/org/axonframework/common/configuration/ComponentRegistry.java`

`ComponentRegistry extends DescribableComponent`. Key methods (verbatim):

```java
public interface ComponentRegistry extends DescribableComponent {

    default <C> ComponentRegistry registerComponent(Class<C> type, ComponentBuilder<C> builder) { ... }
    default <C> ComponentRegistry registerComponent(Class<C> type, String name, ComponentBuilder<C> builder) { ... }
    <C> ComponentRegistry registerComponent(ComponentDefinition<? extends C> definition);

    default <C, D extends C> ComponentRegistry registerDecorator(Class<C> type, int order, ComponentDecorator<C, D> decorator) { ... }
    default <C, D extends C> ComponentRegistry registerDecorator(Class<C> type, String name, int order, ComponentDecorator<C, D> decorator) { ... }
    <C> ComponentRegistry registerDecorator(DecoratorDefinition<C, ? extends C> definition);

    ComponentRegistry registerEnhancer(ConfigurationEnhancer enhancer);

    // hasComponent(Class) / hasComponent(Class, String) for conditional registration.
}
```

### Configuration (accessors used inside an enhancer/decorator)

FQN: `org.axonframework.common.configuration.Configuration`
Path: `common/src/main/java/org/axonframework/common/configuration/Configuration.java`

`Configuration extends DescribableComponent`.

```java
public interface Configuration extends DescribableComponent {

    default <C> C getComponent(Class<C> type) { ... }
    default <C> C getComponent(Class<C> type, @Nullable String name) { ... }
    default <C> Optional<C> getOptionalComponent(Class<C> type) { ... }
    <C> Optional<C> getOptionalComponent(Class<C> type, @Nullable String name);

    default <C> C getComponent(TypeReference<C> typeReference) { ... }
    default <C> C getComponent(TypeReference<C> typeReference, @Nullable String name) { ... }
    default <C> Optional<C> getOptionalComponent(TypeReference<C> typeReference) { ... }
    // ... TypeReference variants
}
```

---

## DecoratorDefinition

FQN: `org.axonframework.common.configuration.DecoratorDefinition`
Path: `common/src/main/java/org/axonframework/common/configuration/DecoratorDefinition.java`

A `sealed interface DecoratorDefinition<C, D extends C>`. Builder API (verbatim):

```java
public sealed interface DecoratorDefinition<C, D extends C>
        permits DecoratorDefinition.CompletedDecoratorDefinition {

    static <C> PartialDecoratorDefinition<C> forType(Class<C> type) { ... }
    static <C> PartialDecoratorDefinition<C> forTypeAndName(Class<C> type, String name) { ... }

    DecoratorDefinition<C, D> order(int order);
    DecoratorDefinition<C, D> onStart(int phase, ComponentLifecycleHandler<D> handler);
    default DecoratorDefinition<C, D> onStart(int phase, Consumer<D> handler) { ... }
    DecoratorDefinition<C, D> onShutdown(int phase, ComponentLifecycleHandler<D> handler);
    default DecoratorDefinition<C, D> onShutdown(int phase, Consumer<D> handler) { ... }

    interface PartialDecoratorDefinition<C> {
        <D extends C> DecoratorDefinition<C, D> with(ComponentDecorator<C, D> decorator);
    }

    non-sealed interface CompletedDecoratorDefinition<C, D extends C> extends DecoratorDefinition<C, D> {
        int order();
        Component<C> decorate(Component<C> delegate);
        boolean matches(Component.Identifier<?> id);
    }
}
```

The decorator lambda — `ComponentDecorator<C, D>`:

FQN: `org.axonframework.common.configuration.ComponentDecorator`

```java
public interface ComponentDecorator<C, D> {

    D decorate(Configuration config,
               @Nullable String name,
               C delegate);
}
```

So the lambda is `(config, name, delegate) -> ...`. There is **no** `.build()`;
`.with(...)` produces the `DecoratorDefinition` directly, on which `.order(int)`,
`.onStart(...)`, `.onShutdown(...)` can be chained. Example from the Javadoc:

```java
DecoratorDefinition.forType(MyComponentInterface.class)
                   .with((config, name, delegate) -> new MyComponentWrapper(delegate, config.getComponent(MyDependency.class)))
                   .onStart(0, MyComponentWrapper::start)
                   .onShutdown(0, MyComponentWrapper::shutdown)
```

Register it via `componentRegistry.registerDecorator(definition)`, or use the shorthand
`registerDecorator(Class<C> type, int order, ComponentDecorator<C, D> decorator)`.

---

## ComponentDescriptor

FQN: `org.axonframework.common.infra.ComponentDescriptor`
Path: `common/src/main/java/org/axonframework/common/infra/ComponentDescriptor.java`

```java
public interface ComponentDescriptor {

    void describeProperty(String name, @Nullable Object object);
    void describeProperty(String name, @Nullable Collection<?> collection);
    void describeProperty(String name, @Nullable Map<?, ?> map);
    void describeProperty(String name, @Nullable String value);
    void describeProperty(String name, @Nullable Long value);
    void describeProperty(String name, @Nullable Boolean value);

    default void describeWrapperOf(Object delegate) {
        describeProperty("delegate", delegate);
    }
}
```

### DescribableComponent

FQN: `org.axonframework.common.infra.DescribableComponent`
Path: `common/src/main/java/org/axonframework/common/infra/DescribableComponent.java`

```java
@FunctionalInterface
public interface DescribableComponent {

    void describeTo(ComponentDescriptor descriptor);
}
```

A tracing decorator should implement `DescribableComponent` and, in `describeTo`,
call `descriptor.describeWrapperOf(delegate)` plus `describeProperty(...)` for its
own fields (e.g. the span factory).

---

## HandlerEnhancerDefinition / MessageHandlingMember

### HandlerEnhancerDefinition

FQN: `org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition`
Path: `messaging/src/main/java/org/axonframework/messaging/core/annotation/HandlerEnhancerDefinition.java`

```java
public interface HandlerEnhancerDefinition {

    <T> MessageHandlingMember<T> wrapHandler(MessageHandlingMember<T> original);
}
```

### MessageHandlingMember

FQN: `org.axonframework.messaging.core.annotation.MessageHandlingMember` (marked `@Internal`)
Path: `messaging/src/main/java/org/axonframework/messaging/core/annotation/MessageHandlingMember.java`

```java
@Internal
public interface MessageHandlingMember<T> {

    Class<?> payloadType();
    default int priority() { return 0; }

    boolean canHandle(Message message, ProcessingContext context);
    default boolean canHandleType(Class<?> payloadType) { return true; }
    boolean canHandleMessageType(Class<? extends Message> messageType);

    // DEPRECATED (forRemoval, since 5.2.0) — synchronous invocation:
    @Internal @Deprecated(forRemoval = true, since = "5.2.0")
    Object handleSync(Message message, ProcessingContext context, @Nullable T target) throws Exception;

    // Current async-first invocation:
    MessageStream<?> handle(Message message, ProcessingContext context, @Nullable T target);

    <HT> Optional<HT> unwrap(Class<HT> handlerType);
    default Class<?> declaringClass() { ... }          // via unwrap(Member.class)
    default String signature() { ... }                 // via unwrap(Executable.class), else "__unknown__"
    default <R> Optional<R> attribute(String attributeKey) { return Optional.empty(); }
}
```

KEY DIFFERENCES vs AF4:
- Invocation now returns `MessageStream<?>` via `handle(Message, ProcessingContext, T target)`.
- `handleSync(...)` is deprecated for removal in 5.2.0.
- Annotation attribute access is `attribute(String attributeKey)` returning `Optional<R>`.
  (Compare with `HandlerAttributes` for common attribute keys.)

---

## LegacyResources

FQN: `org.axonframework.messaging.core.LegacyResources`
Path: `messaging/src/main/java/org/axonframework/messaging/core/LegacyResources.java`

```java
public abstract class LegacyResources {

    public static final Context.ResourceKey<String> AGGREGATE_IDENTIFIER_KEY =
            Context.ResourceKey.withLabel("aggregateIdentifier");

    public static final Context.ResourceKey<String> AGGREGATE_TYPE_KEY =
            Context.ResourceKey.withLabel("aggregateType");

    public static final Context.ResourceKey<Long> AGGREGATE_SEQUENCE_NUMBER_KEY =
            Context.ResourceKey.withLabel("aggregateSequenceNumber");
}
```

Note types: identifier and type are `ResourceKey<String>`, sequence number is `ResourceKey<Long>`.
Read from a context: `context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)` (returns `@Nullable String`).

---

## Secondary component interfaces

### EventSink

FQN: `org.axonframework.messaging.eventhandling.EventSink`
Path: `messaging/src/main/java/org/axonframework/messaging/eventhandling/EventSink.java`

`EventSink extends DescribableComponent`.

```java
CompletableFuture<Void> publish(@Nullable ProcessingContext context, List<? extends EventMessage> events);
default CompletableFuture<Void> publish(@Nullable ProcessingContext context, EventMessage... events) { ... }
```
(primary method: `publish(ProcessingContext, List<? extends EventMessage>)`)

### EventHandlingComponent

FQN: `org.axonframework.messaging.eventhandling.EventHandlingComponent`
Path: `messaging/src/main/java/org/axonframework/messaging/eventhandling/EventHandlingComponent.java`

`EventHandlingComponent extends EventHandler, ResetHandler, ReplayStatusChangedHandler, DescribableComponent`.

```java
Set<QualifiedName> supportedEvents();
default boolean supports(QualifiedName eventName) { ... }
Object sequenceIdentifierFor(EventMessage event, ProcessingContext context);
// handle(...) inherited from EventHandler returns MessageStream.Empty<?> / MessageStream
```
(AF5 already provides `TracingEventHandlingComponent` decorating this.)

### QueryBus

FQN: `org.axonframework.messaging.queryhandling.QueryBus`
Path: `messaging/src/main/java/org/axonframework/messaging/queryhandling/QueryBus.java`

`QueryBus extends QueryHandlerRegistry<QueryBus>, DescribableComponent`.

```java
MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context);
```
(AF5 already provides `TracingQueryBus`.)

### QueryUpdateEmitter

FQN: `org.axonframework.messaging.queryhandling.QueryUpdateEmitter`
Path: `messaging/src/main/java/org/axonframework/messaging/queryhandling/QueryUpdateEmitter.java`

`QueryUpdateEmitter extends DescribableComponent`.

```java
ResourceKey<QueryUpdateEmitter> RESOURCE_KEY = ResourceKey.withLabel("QueryUpdateEmitter");

// emit(...) overloads with (Class<Q> queryType, Predicate, update) and (QualifiedName queryName, Predicate, update)
<Q> void complete(Class<Q> queryType, Predicate<? super Q> filter);
void complete(QualifiedName queryName, Predicate<Object> filter);
```

### Repository

FQN: `org.axonframework.modelling.repository.Repository`
Path: `modelling/src/main/java/org/axonframework/modelling/repository/Repository.java`

`Repository<ID, E> extends DescribableComponent` (sealed; `LifecycleManagement` sub-interface).

```java
Class<E> entityType();
Class<ID> idType();
// load / loadOrCreate / persist operations return CompletableFuture<ManagedEntity<ID,E>>
non-sealed interface LifecycleManagement<ID, E> extends Repository<ID, E> {
    ManagedEntity<ID, E> attach(ManagedEntity<ID, E> entity, ProcessingContext processingContext);
}
```

### StateManager

FQN: `org.axonframework.modelling.StateManager`
Path: `modelling/src/main/java/org/axonframework/modelling/StateManager.java`

```java
public interface StateManager {
    <ID, T> StateManager register(Repository<ID, T> repository);
    // loadEntity / loadManagedEntity return CompletableFuture<...>
    Set<Class<?>> registeredEntities();
    Set<Class<?>> registeredIdsFor(Class<?> entityType);
    <ID, T> Repository<ID, T> repository(Class<T> entityType, Class<ID> idType);
}
```

### SnapshotStore

FQN: `org.axonframework.eventsourcing.snapshot.store.SnapshotStore`
Path: `eventsourcing/src/main/java/org/axonframework/eventsourcing/snapshot/store/SnapshotStore.java`

```java
public interface SnapshotStore {
    CompletableFuture<Void> store(QualifiedName qualifiedName, Object identifier, Snapshot snapshot);
    CompletableFuture<@Nullable Snapshot> load(QualifiedName qualifiedName, Object identifier);
}
```

---

## MessageStream

FQN: `org.axonframework.messaging.core.MessageStream`
Path: `messaging/src/main/java/org/axonframework/messaging/core/MessageStream.java`

`MessageStream<M extends Message>`. Key static factories and transforms (verbatim
signatures, used to wrap a returned stream and open/close a span around it):

```java
public interface MessageStream<M extends Message> {

    // ---- static factories ----
    static <M extends Message> Single<M> fromFuture(CompletableFuture<? extends @Nullable M> future);
    static <M extends Message> Single<M> fromFuture(CompletableFuture<? extends @Nullable M> future,
                                                    Function<M, Context> contextSupplier);
    static <M extends Message> Single<M> just(@Nullable M message);
    static <M extends Message> Single<M> just(@Nullable M message, Function<M, Context> contextSupplier);
    static <M extends Message> Empty<M> failed(Throwable failure);
    static <M extends Message> Empty<M> empty();
    // (also fromIterable / fromStream factories nearby that build IteratorMessageStream)

    // ---- consumption ----
    Optional<Entry<M>> next();
    Optional<Entry<M>> peek();
    default Single<M> first();                          // TruncateFirstMessageStream

    // ---- transforms (defaults) ----
    default <RM extends Message> MessageStream<RM> map(Function<Entry<M>, Entry<RM>> mapper);
    default <RM extends Message> MessageStream<RM> mapMessage(Function<M, RM> mapper);
    default MessageStream<M> onNext(Consumer<Entry<M>> onNext);
    default MessageStream<M> onErrorContinue(Function<Throwable, MessageStream<? extends M>> onError);
    default MessageStream<M> filter(Predicate<Entry<M>> filter);
    default <T extends Message> MessageStream<T> cast();
    // reduce(...) with BiFunction accumulator also present

    // ---- entry ----
    interface Entry<M extends Message> extends Context {
        M message();
        <RM extends Message> Entry<RM> map(Function<M, RM> mapper);
        <T> Entry<M> withResource(ResourceKey<T> key, T resource);
    }

    // ---- single-element stream ----
    interface Single<M extends Message> extends MessageStream<M> {
        @Override default Single<M> first();
        @Override default <RM extends Message> Single<RM> map(Function<Entry<M>, Entry<RM>> mapper);
        @Override default <RM extends Message> Single<RM> mapMessage(Function<M, RM> mapper);
        @Override default Single<M> filter(Predicate<Entry<M>> filter);
        @Override default Single<M> onNext(Consumer<Entry<M>> onNext);
        default Single<M> onComplete(Runnable completeHandler);
        @Override default <R extends Message> Single<R> cast();
        default CompletableFuture<@Nullable Entry<M>> asCompletableFuture();
    }

    interface Empty<M extends Message> extends Single<M> { }
}
```

NOTE: there is no `whenComplete(...)` on `MessageStream` itself; `Single` has
`onComplete(Runnable)`. Use `onNext` / `onComplete` / `onErrorContinue` to hook
span open/close around stream consumption, and `Single.asCompletableFuture()` to
bridge to a future when needed.

---

## @Internal annotation

FQN: `org.axonframework.common.annotation.Internal`
Path: `common/src/main/java/org/axonframework/common/annotation/Internal.java`

```java
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE})
public @interface Internal {
}
```
