# ADR 002: Event scheduler builders take a `UnitOfWorkFactory`, a `Converter` and an `EventSink` (issue [#701](https://github.com/AxonIQ/axoniq-framework/issues/701))

Date: 2026-10-23
Status: accepted
Related: [#3065 ADR 006](../3065-legacy-deadlines/adr-006-deadline-manager-builders-on-af5.md) (deadline manager builders), [ADR 001](adr-001-event-scheduler-packages-and-stored-format.md) (stored format), [ADR 003](adr-003-fired-event-message-type.md) (message type), [ADR 005](adr-005-axon-server-event-scheduler.md) (Axon Server)

## Context

Axon Framework 4.13's event scheduler builders take:

| Scheduler | Builder parameters |
|---|---|
| `SimpleEventScheduler` | `scheduledExecutorService` (required), `eventBus` (required), `transactionManager` |
| `QuartzEventScheduler` | `scheduler`, `eventBus`, `serializer` (defaults to XStream), `transactionManager`, `jobDataBinder`, `spanFactory` |
| `JobRunrEventScheduler` | `jobScheduler`, `eventBus`, `serializer` (required), `transactionManager`, `spanFactory` |
| `DbSchedulerEventScheduler` | `scheduler`, `eventBus`, `serializer` (required), `transactionManager`, `spanFactory`, `useBinaryPojo`, `startScheduler`, `stopScheduler` |
| `AxonServerEventScheduler` | `connectionManager`, `eventSerializer` (defaults to XStream), `requestTimeout`, `defaultContext` |

#3065 ADR 006 settled the same question for the deadline managers: a required `unitOfWorkFactory(...)` replaces
`transactionManager(...)`, a required `converter(...)` replaces `serializer(...)` for the persistent backends, and
`spanFactory(...)` and `messageNameResolver(...)` are not ported. The event schedulers differ in three ways:

- they publish rather than deliver to a scope, so they need a publishing target, which Axon Framework 4 typed as
  `EventBus`. In Axon Framework 5, the `EventBus` and the `EventStore` are both `EventSink`s, and the configuration
  registers the event store as the `EventSink` when there is one;
- two builders, Quartz's and Axon Server's, fell back to XStream when no serializer was given;
- a fired event is published and stored, so its message type matters (ADR 003), unlike a deadline's.

## Options considered

**Option A: keep the Axon Framework 4 builders.** Not possible: `TransactionManager` units of work, `Serializer` and
`SpanFactory` do not exist in Axon Framework 5, as #3065 ADR 006 describes.

**Option B: mirror #3065 ADR 006, keep `eventBus(EventBus)`.** Publishing on the `EventBus` alone would bypass the
event store, so a fired event would reach subscribed handlers but not be stored or streamed.

**Option C: mirror #3065 ADR 006, with `eventSink(EventSink)` as target, a required converter for every scheduler that
stores events, including Axon Server, and no XStream fallback (chosen).**

**Option D: Option C with an `eventGateway(EventGateway)` as target instead of the `EventSink` and the
`MessageTypeResolver`.** The `EventGateway` is the publishing path the deprecation points users to, and the
configuration's `DefaultEventGateway` wraps the same `EventSink` and `MessageTypeResolver` components, so a fired
payload would get the gateway's type by construction. But it does not fit what a fired event needs:

- `EventGateway.publish(...)` takes no metadata: a plain payload gets empty metadata, and an `EventMessage` is passed
  on as it is. The Quartz, JobRunr and db-scheduler jobs hold payload and metadata, so these schedulers would build the
  `EventMessage` themselves, which needs a `MessageType`, and thus their own `MessageTypeResolver` next to the
  gateway's, which is not reachable;
- a fired event is a new message, with a new identifier and timestamp, as in Axon Framework 4, and the gateway passes
  a scheduled `EventMessage` on unchanged, so the in-memory scheduler would rebuild it anyway;
- dispatch interceptors and the event store sit behind the configuration's `EventSink` on both paths.

Only the in-memory scheduler would drop a builder parameter, and its builder would then differ from the other three.

## Decision

Option C.

- Every scheduler that fires events locally takes a required `unitOfWorkFactory(UnitOfWorkFactory)`;
  `transactionManager(...)` is removed. A fired event is published in a unit of work from that factory.
- The publishing target is `eventSink(EventSink)`. The method takes the `EventSink` type, so the configuration's event
  store can be passed. Spring Boot and the configuration wire the configuration's `EventSink` component.
- Every scheduler that stores events, Quartz, JobRunr, db-scheduler and Axon Server, takes a required
  `converter(Converter)` in place of `serializer(...)`/`eventSerializer(...)`. There is no XStream fallback: an
  application that relied on it passes an `XStreamConverter` (#3065 ADR 008) explicitly.
- A `messageTypeResolver(MessageTypeResolver)` decides the type of a fired event (ADR 003).
- `spanFactory(...)` is not ported. The Axon Server scheduler takes no `unitOfWorkFactory` and no `eventSink`, as
  nothing fires locally.
- `SimpleEventScheduler`'s `scheduledExecutorService` gets a default, a single-thread executor, as the ported
  `SimpleDeadlineManager` has.
- Spring factory beans (`SimpleEventSchedulerFactoryBean`, `QuartzEventSchedulerFactoryBean`) are not ported; they only
  helped wire what the builders now cover.

## Consequences

- Every Axon Framework 4 builder call changes: it adds `unitOfWorkFactory(...)`, swaps `eventBus(...)` for
  `eventSink(...)` and `serializer(...)` for `converter(...)`, and drops `transactionManager(...)` and
  `spanFactory(...)`. The OpenRewrite recipe can remove calls
  and map known serializers; it leaves a marker for the factory and unknown serializers.
- A fired event is stored like any published event when the application has an event store.
- An application that relied on the XStream default of the Quartz or Axon Server scheduler has to configure an
  `XStreamConverter` to keep reading its stored events.
