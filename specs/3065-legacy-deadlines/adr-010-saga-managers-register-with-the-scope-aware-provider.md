# ADR 010: Saga managers register themselves with the configuration's `ScopeAwareProvider` (issues [#564](https://github.com/AxonIQ/axoniq-framework/issues/564), [#565](https://github.com/AxonIQ/axoniq-framework/issues/565))

Date: 2026-10-09
Status: proposed
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005) (deadline backends), [#5004](https://github.com/AxonIQ/AxonFramework/issues/5004) (aggregate deadlines as commands), [ADR 002](adr-002-saga-deadline-scope-aware-delivery.md) (saga deadline delivery), [ADR 007](adr-007-aggregate-deadline-delivery-through-scope-aware.md) (aggregate deadline delivery)

## Context

[ADR 002](adr-002-saga-deadline-scope-aware-delivery.md) delivers fired saga deadlines through `ScopeAware`, and
builds the `ScopeAwareProvider` from a list of saga managers the application supplies (Option B2). It rejected
discovery (Option B1) because no mechanism existed to discover saga managers, and left wiring saga autodiscovery into
the provider to [#565](https://github.com/AxonIQ/axoniq-framework/issues/565).

Since then:
- Spring Boot discovers `@Saga` beans (`SpringSagaLookup`, `SagaProcessorConfigurer`). A Spring application has no
  reference to the saga managers it could list.
- The JobRunr and db-scheduler auto-configurations of [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005)
  need a `ScopeAwareProvider`, and configure no deadline manager without one.
- Every saga manager, configured by hand or discovered by Spring, is built by `SagaConfigurer.build(Configuration)`.

`Configuration.getComponents(Class)` can enumerate components, including those of modules, but not by the saga
manager's type. An event processor module registers each saga manager as an `EventHandlingComponent`, wrapped in a
`SequenceCachingEventHandlingComponent` and decorated with an `InterceptingEventHandlingComponent`. A provider could
enumerate every `EventHandlingComponent` and `unwrap(AbstractSagaManager.class)` each, but only once the event
processors have built them, and it would walk every module's components to find them. A manager registering itself
when it is built needs neither.

Components are created lazily. A saga manager is built when its event processor is, in the processor's start handler
in `Phase.INBOUND_EVENT_CONNECTORS`. A persistent backend can fire an overdue deadline before that, for example after
a deployment:
- db-scheduler, which Spring Boot's db-scheduler starter starts when it creates the scheduler bean, unless
  `db-scheduler.delay-startup-until-context-ready` is set;
- db-scheduler started by the configuration, in the same phase as the event processors;
- a scheduler that a non-Spring application starts before the configuration.

Spring Boot's Quartz `SchedulerFactoryBean` starts in phase `Integer.MAX_VALUE`, after the event processors. A
provider that is asked before the saga managers exist finds no `ScopeAware`, and the backend treats the deadline as
delivered.

## Options considered

| | Option A: application lists the managers ([ADR 002](adr-002-saga-deadline-scope-aware-delivery.md))                      | Option B: Spring assembles the list | Option C: saga managers register themselves |
|---|--------------------------------------------------------------------------------------------------------------------------|---|---|
| **How** | `new LegacyScopeAwareProvider(sagaManager1, ...)`, also in Spring.                                                       | `SagaProcessorConfigurer` registers each discovered saga manager as a named component, and a provider built from those names. Without Spring, the application keeps listing the managers. | The configuration holds a `LegacyScopeAwareProvider`; `SagaConfigurer.build(...)` adds each saga manager it builds to it. |
| **Pros** | No new mechanism.                                                                                                        | Managers are known at configuration time. | One mechanism with and without Spring; no wiring for the application. |
| **Cons** | A Spring application cannot reach the discovered managers; the autoconfigured backends from #5005 would get no provider. | Two mechanisms; the application still lists managers without Spring. | The provider's content grows while the configuration starts, so it has to wait until the configuration has started. |

## Decision: Option C, with a readiness wait

`axoniq-legacy` delivers deadlines to two kinds of `ScopeAware`: saga managers and `AggregateDeadlineCommandTranslator`
([#5004](https://github.com/AxonIQ/AxonFramework/issues/5004),
[ADR 007](adr-007-aggregate-deadline-delivery-through-scope-aware.md)). Both are registered by the configuration, so
an application does not define a `ScopeAwareProvider`.

- `LegacyScopeAwareProvider` (`org.axonframework.messaging`) streams the registered `ScopeAware` components that
  `canResolve(...)` a descriptor; `register(ScopeAware)` adds one.
- A `ConfigurationEnhancer` in `axoniq-legacy` registers a `LegacyScopeAwareProvider` as the `ScopeAwareProvider`
  component, with a start handler in a phase after `Phase.INBOUND_EVENT_CONNECTORS` that marks it ready, and a
  shutdown handler that releases waiting callers.
- `SagaConfigurer.build(Configuration)` registers the saga manager it builds with the configuration's
  `LegacyScopeAwareProvider`.
- A decorator on the `ScopeAwareProvider` component registers one `AggregateDeadlineCommandTranslator` over the
  configuration's `CommandGateway`. The translator only resolves an `AggregateScopeDescriptor`, so it does no harm in
  an application without aggregate deadlines. The provider rejects a second translator, so that no aggregate deadline
  is dispatched twice.
- The provider waits for its start handler before it answers: Axon runs the phases in order, so by then every event
  processor has built its saga managers. It waits at most a timeout, 30 seconds by default, and then throws a
  transient exception, so the backend retries the deadline. The timeout is a setting of the configuration, which
  Spring Boot binds from a property.
- Deadline managers use the configuration's provider: autowired as the `ScopeAwareProvider` bean in Spring, or
  `configuration.getComponent(ScopeAwareProvider.class)` without Spring. The JobRunr and db-scheduler
  auto-configurations drop `ScopeAwareProvider` from their `@ConditionalOnBean`, which is evaluated before
  enhancer-registered components become beans.

This replaces Option B2 of [ADR 002](adr-002-saga-deadline-scope-aware-delivery.md). Its delivery through `ScopeAware`
stays.

## Consequences

- Neither Spring nor non-Spring applications list their saga managers or add the translator. Saga and aggregate
  deadlines reach their target without any wiring, as Axon Framework 4's `ConfigurationScopeAwareProvider` found saga
  managers and aggregate repositories by itself. The Spring Boot deadline managers of #5005 configure themselves when
  the application provides the scheduler.
- Applications do not create a `ScopeAwareProvider`; `LegacyScopeAwareProvider`'s constructor is not public. A
  deadline manager the application defines itself takes the configuration's provider.
- This changes [ADR 007](adr-007-aggregate-deadline-delivery-through-scope-aware.md)'s premise that the application
  adds the translator to the provider's list. The translator, its decorator and the check that only one is registered
  come with #5004; this decision provides `register(ScopeAware)` and the waiting provider they build on.
- A deadline that fires before the configuration has started waits for the `ScopeAwareProvider` to be ready, with all
  `ScopeAware` components registered. Until then it holds a scheduler thread. If many deadlines are overdue, a Quartz
  thread pool shared with other jobs is busy during that time.
- After the readiness timeout, the backend's failure handling applies: Quartz refires immediately, JobRunr and
  db-scheduler retry later.
- Applications avoid the wait by starting their scheduler after the configuration: in Spring Boot with
  `db-scheduler.delay-startup-until-context-ready=true`, without Spring by starting it after
  `configuration.start()`. The documentation says so.
- The provider's content is mutable. Saga managers and the translator are only added while the configuration starts,
  never removed.
