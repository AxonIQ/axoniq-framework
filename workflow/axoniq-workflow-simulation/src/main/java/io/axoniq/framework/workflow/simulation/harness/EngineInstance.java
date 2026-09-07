/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.simulation.harness;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowCustomization;
import io.axoniq.framework.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.fakes.MutableClock;
import io.axoniq.framework.workflow.runtime.test.fakes.SeededWorkflowIdGenerator;
import io.axoniq.framework.workflow.simulation.workflow.AnyMatchNoMatchWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.BackoffCancelWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.BackoffOverflowWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CancellingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.CustomNamedWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.DriftWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.FailingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.HookWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.LoopingPollWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.MigratingOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.OnRetryFires;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.PayloadOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.ReducerWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.RetryTimingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.RetryResumeWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.RetryingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.RollingDeployWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SagaOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.StartOnlyWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SubscriptionRenewalWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.StatusHookFires;
import io.axoniq.framework.workflow.simulation.workflow.TimeoutWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.VersionedOrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.VersioningEdgesWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.AnyMatchNoMatchRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BackoffCancelRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BackoffOverflowRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CancelRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CombinatorReplayRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CombinatorRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedWaitRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CustomNamedRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DriftRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ExternalCancelRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.FailExhaustionRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.FailRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.HookRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.LoopCounterPollRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.LoopReusedPollRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.MigrateRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PayloadOrderRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ReducerNullEdgeRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ReducerRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ReducerThrowingModifierRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryEdgesRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryTimeoutRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RollingDeployOrderEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SagaDefaultTimeoutOrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SagaOrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SagaRetryCompOrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.StartOnlyRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.SubscriptionStartedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.TimeoutRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersionedOrderRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.VersioningEdgesRequestedEvent;
import java.util.List;
import java.util.function.Consumer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;

import java.time.Clock;

import static io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;

/**
 * One running workflow-engine process in the simulated world.
 * <p>
 * It builds a real {@code WorkflowConfigurer} wired exactly like {@code WorkflowReplayPreparedStateTest}: the durable
 * substrate ({@link ControllableEventStorageEngine} + {@link DurableTokenStore}) is shared and survives crashes,
 * while the deterministic Phase-3 seams are registered so the engine's time, scheduling, and ids are fully driven by
 * the simulator:
 * <ul>
 *   <li>{@link WorkflowScheduler} &rarr; {@link io.axoniq.framework.workflow.runtime.test.fakes.ManualWorkflowScheduler} (virtual time);</li>
 *   <li>{@link Clock} &rarr; {@link MutableClock} (advanced in lock-step with the scheduler);</li>
 *   <li>unit-of-work names &rarr; {@link SeededWorkflowIdGenerator}.</li>
 * </ul>
 * The body {@code ExecutorService} ({@code WORKFLOW_ENGINE_EXECUTOR}) is left at its default (virtual-thread-per-task)
 * <strong>on purpose</strong>: the engine's body blocks on its per-instance task queue ({@code awaitStateChange} →
 * {@code taskQueue.take()}) waiting for events that the processor thread must deliver, so the body and the
 * event-delivery thread must be distinct. A literal same-thread executor deadlocks the body against its own queue
 * (it cannot deliver to itself the event it is parked on).
 * <p>
 * <strong>What a seed does and does not fix.</strong> It fixes every fault choice (one seeded {@code Random}), every
 * workflow id ({@link SeededWorkflowIdGenerator}) and every timer (virtual time). It does <strong>not</strong> fix
 * the verdict. The event processor runs one work package per segment and the engine defaults to more than one, so
 * several delivery threads race the body threads; the per-instance FIFO task queue still evolves one instance's state
 * in delivery order, but which deliveries land, and when, varies between runs of the same seed. This is measured, not
 * assumed: one run of {@code DstSmokeTest} left three different sets of instances non-terminal on seed 1. Pinning
 * this harness's {@link DurableTokenStore} to a single segment was measured as the remedy and rejected — it leaves
 * more tests red, not fewer. (See the simulation README "Determinism level".)
 * <p>
 * The processor {@link org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore} is the
 * world's {@link DurableTokenStore} — it <strong>is</strong> shared across a restart, because the processor token is
 * the engine's recovery anchor: on start the workflow event-processing enhancer resets to the earliest stored segment
 * token and replays from it.
 * <p>
 * "Crashing" an instance is {@link #stop()} (drop all volatile engine state); "recovering" is constructing a new
 * {@code EngineInstance} over the same durable substrate.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class EngineInstance implements AutoCloseable {

    private final AxonConfiguration configuration;
    private final WorkflowEngine workflowEngine;
    private final EventSink eventSink;
    private final MessageTypeResolver messageTypeResolver;
    private final EventConverter eventConverter;
    private final MutableWorkflowHistoryRepository historyRepository;

    /**
     * Builds and starts an engine driving the default {@link OrderWorkflow} (the fuzz/scenario workhorse) over the
     * given durable substrate and deterministic seams.
     *
     * @param eventStorageEngine durable, shared event store (survives crashes).
     * @param tokenStore         durable, shared processor token store (survives crashes).
     * @param historyRepository  shared history read-model (survives crashes; rebuilt by replay on restart).
     * @param scheduler          virtual-time scheduler shared with the simulator.
     * @param clock              mutable clock advanced in lock-step with the scheduler.
     * @param idGenerator        seeded id generator.
     * @param effects            counting side-effect registry the test workflow records into.
     */
    public EngineInstance(ControllableEventStorageEngine eventStorageEngine,
                          DurableTokenStore tokenStore,
                          MutableWorkflowHistoryRepository historyRepository,
                          WorkflowScheduler scheduler,
                          MutableClock clock,
                          SeededWorkflowIdGenerator idGenerator,
                          CountingEffects effects) {
        this(eventStorageEngine, tokenStore, historyRepository, scheduler, clock, idGenerator,
             orderWorkflow(effects));
    }

    /**
     * Builds and starts an engine driving the workflow described by {@code registration}. The default constructor
     * delegates here with {@link #orderWorkflow(CountingEffects)}; the INV-7 ({@code TerminalIsFinal}) scenario uses
     * {@link #cancellingWorkflow(CountingEffects)} so a workflow that actually reaches a terminal (CANCELLED) status is
     * exercised. All infrastructure wiring (durable substrate, deterministic seams, non-durable per-process processor
     * token) is identical regardless of which workflow is registered.
     *
     * @param eventStorageEngine durable, shared event store (survives crashes).
     * @param tokenStore         durable, shared processor token store (survives crashes).
     * @param historyRepository  shared history read-model (survives crashes; rebuilt by replay on restart).
     * @param scheduler          virtual-time scheduler shared with the simulator.
     * @param clock              mutable clock advanced in lock-step with the scheduler.
     * @param idGenerator        seeded id generator.
     * @param registration       which workflow definition to register (name, start event, id-prefix, body).
     */
    public EngineInstance(ControllableEventStorageEngine eventStorageEngine,
                          DurableTokenStore tokenStore,
                          MutableWorkflowHistoryRepository historyRepository,
                          WorkflowScheduler scheduler,
                          MutableClock clock,
                          SeededWorkflowIdGenerator idGenerator,
                          WorkflowRegistration registration) {
        this(eventStorageEngine, tokenStore, historyRepository, scheduler, clock, idGenerator,
             List.of(registration));
    }

    /**
     * Builds and starts an engine registering <strong>one or more</strong> workflow definitions, all of the same
     * {@link SimpleWorkflowContext} type, into a SINGLE {@code WorkflowModule} so they share one engine, registry and
     * repository — exactly how {@code MultiVersionRoutingDeclarativeTest} registers several definitions. This is the
     * multi-version path INV-11 ({@code VersionRoutingSound}) needs: pass two {@link WorkflowRegistration}s sharing a
     * {@code workflowName} + start event but differing in {@link WorkflowRegistration#workflowVersion()} and the engine
     * spawns a fresh start at the highest version while keeping the older definition registered for replay routing. A
     * single-element list reproduces the original single-definition behaviour byte-for-byte (each existing factory still
     * wires exactly one registration).
     *
     * @param eventStorageEngine durable, shared event store (survives crashes).
     * @param tokenStore         durable, shared processor token store (survives crashes).
     * @param historyRepository  shared history read-model (survives crashes; rebuilt by replay on restart).
     * @param scheduler          virtual-time scheduler shared with the simulator.
     * @param clock              mutable clock advanced in lock-step with the scheduler.
     * @param idGenerator        seeded id generator.
     * @param registrations      one or more workflow definitions to register into the single module; all share the same
     *                           context type. The module name is the first registration's {@code workflowName} (every
     *                           version of one logical workflow shares the name, matching production grouping).
     */
    public EngineInstance(ControllableEventStorageEngine eventStorageEngine,
                          DurableTokenStore tokenStore,
                          MutableWorkflowHistoryRepository historyRepository,
                          WorkflowScheduler scheduler,
                          MutableClock clock,
                          SeededWorkflowIdGenerator idGenerator,
                          List<WorkflowRegistration> registrations) {
        this(eventStorageEngine, tokenStore, historyRepository, scheduler, clock, idGenerator, registrations, null);
    }

    /**
     * Builds and starts an engine as {@link #EngineInstance(ControllableEventStorageEngine, DurableTokenStore,
     * MutableWorkflowHistoryRepository, WorkflowScheduler, MutableClock, SeededWorkflowIdGenerator, List)}, but with an
     * OPTIONAL body-executor override. When {@code bodyExecutorOverride} is {@code null} (every existing call) the engine
     * keeps its default {@code WORKFLOW_ENGINE_EXECUTOR} (virtual-thread-per-task) <strong>byte-for-byte</strong> — so the
     * fuzz/scenario wiring is unchanged. The INV-23 ({@code EngineSelfProtection}) nested-primitive probe passes a
     * {@code SameThreadExecutorService} here to characterize the OTHER half of that self-protection surface: with a
     * single-threaded body executor the §3.1-forbidden nested primitive DEADLOCKS the per-instance task queue (the inner
     * action cannot get a thread to run on while the outer action's {@code awaitStateChange} owns the only thread), the
     * documented "blocks forever" case. The override is registered the same injectable way the production seam is
     * ({@code registerComponent(ExecutorService.class, WORKFLOW_ENGINE_EXECUTOR, ...)}) — no production code changes.
     *
     * @param eventStorageEngine   durable, shared event store (survives crashes).
     * @param tokenStore           durable, shared processor token store (survives crashes).
     * @param historyRepository    shared history read-model (survives crashes; rebuilt by replay on restart).
     * @param scheduler            virtual-time scheduler shared with the simulator.
     * @param clock                mutable clock advanced in lock-step with the scheduler.
     * @param idGenerator          seeded id generator.
     * @param registrations        one or more workflow definitions to register into the single module.
     * @param bodyExecutorOverride optional body {@code ExecutorService} to register under {@code WORKFLOW_ENGINE_EXECUTOR};
     *                             {@code null} keeps the engine's default virtual-thread executor (every existing path).
     */
    public EngineInstance(ControllableEventStorageEngine eventStorageEngine,
                          DurableTokenStore tokenStore,
                          MutableWorkflowHistoryRepository historyRepository,
                          WorkflowScheduler scheduler,
                          MutableClock clock,
                          SeededWorkflowIdGenerator idGenerator,
                          List<WorkflowRegistration> registrations,
                          java.util.concurrent.@org.jspecify.annotations.Nullable ExecutorService bodyExecutorOverride) {
        if (registrations.isEmpty()) {
            throw new IllegalArgumentException("At least one workflow registration is required");
        }
        this.historyRepository = historyRepository;
        String moduleName = registrations.get(0).workflowName();

        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(cr -> {
            cr.registerComponent(EventStorageEngine.class, cfg -> eventStorageEngine)
              .registerComponent(MutableWorkflowHistoryRepository.class, cfg -> historyRepository)
              // The processor picks its token store up through the UNNAMED component
              // (AllEventEventHandlingComponent.anyEventInSegments), while the replay-reset hook reads the NAMED one
              // (WorkflowEventProcessingRegistrationEnhancer). Register the durable store under both so the running
              // processor and the reset hook agree on one durable recovery anchor.
              .registerComponent(TokenStore.class, cfg -> tokenStore)
              .registerComponent(TokenStore.class,
                                 "TokenStore[" + WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME + "]",
                                 cfg -> tokenStore)
              .registerComponent(WorkflowScheduler.class, cfg -> scheduler)
              .registerComponent(Clock.class, cfg -> clock)
              .registerModule(buildModule(moduleName, registrations));
            if (bodyExecutorOverride != null) {
                cr.registerComponent(java.util.concurrent.ExecutorService.class,
                                     WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR,
                                     cfg -> bodyExecutorOverride);
            }
        });

        this.configuration = configurer.start();
        this.workflowEngine = configuration.getComponent(WorkflowEngine.class);
        this.eventSink = configuration.getComponent(EventSink.class);
        this.messageTypeResolver = configuration.getComponent(MessageTypeResolver.class);
        this.eventConverter = configuration.getComponent(EventConverter.class);
    }

    /**
     * Builds a single {@code WorkflowModule} holding every given registration as its own definition (chaining
     * {@link WorkflowModule#definition} for the second and later ones), each pinned to its
     * {@link WorkflowRegistration#workflowVersion()} via {@code WorkflowCustomization.workflowVersion(...)} and keyed by
     * the same {@code orderId}-derived id provider.
     */
        private static WorkflowModule<SimpleWorkflowContext> buildModule(
            String moduleName, List<WorkflowRegistration> registrations) {
        WorkflowModule<SimpleWorkflowContext> module =
                WorkflowModule.defaults(moduleName, SimpleWorkflowContext.class)
                              .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                              .definition(d -> definitionOf(d, registrations.get(0)));
        for (int i = 1; i < registrations.size(); i++) {
            var registration = registrations.get(i);
            module = module.definition(d -> definitionOf(d, registration));
        }
        return module;
    }

    /**
     * Configures one workflow definition from a {@link WorkflowRegistration}: declarative body, name, start-event
     * trigger, the {@code orderId}-prefixed id provider, and the pinned {@code workflowVersion}.
     */
        private static WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext> definitionOf(
            WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext> phase,
            WorkflowRegistration registration) {
        return phase
                .declarative(c -> registration.body()::accept)
                .workflowName(registration.workflowName())
                // start-event-driven; id from the "orderId" payload attribute.
                .on(EventConditions.fromType(registration.startEventClass()))
                .customized((c, w) -> {
                    w.workflowVersion(registration.workflowVersion())
                     .workflowIdProvider(fromPayloadAttribute(
                             c, "orderId", id -> registration.idPrefix() + id));
                    // Optional extra customization (e.g. status-change listeners for INV-17); a no-op for every existing
                    // single-arg registration, so their wiring is byte-for-byte unchanged.
                    registration.customizer().accept(w);
                    return w;
                });
    }

    /**
     * Describes which workflow definition an {@link EngineInstance} should register: its logical name, the start-event
     * class it triggers on, the prefix the id provider applies to the {@code orderId} payload attribute, the pinned
     * {@code workflowVersion}, and the body. Keeping this a small value lets the same engine wiring drive
     * {@link OrderWorkflow} (the fuzz workhorse), {@link CancellingWorkflow} (the INV-7 terminal-path scenario), or two
     * coexisting versions of {@link VersionedOrderWorkflow} (the INV-11 multi-version routing scenario) without
     * duplicating any infrastructure.
     *
     * @param workflowName    the logical workflow name (also the module name). Every version of one logical workflow
     *                        shares this name.
     * @param startEventClass the event type that starts an instance.
     * @param idPrefix        prefix applied to the {@code orderId} payload value to form the workflow id.
     * @param workflowVersion the semver version this definition is registered under (pinned via
     *                        {@code WorkflowCustomization.workflowVersion}); the engine spawns a fresh start at the
     *                        highest registered version and stamps every emitted event's {@code MessageType.version()}
     *                        with the routed definition's version.
     * @param body            the workflow body (re-run on every (re)execution).
     * @param customizer      optional extra customization applied inside {@code .customized(...)} after the version and
     *                        id provider are set — used by the INV-17 {@link #hookWorkflow} factory to register a
     *                        counting status-change listener. A no-op {@link Consumer} for every other registration, so
     *                        the existing single-definition wiring is byte-for-byte unchanged.
     */
    public record WorkflowRegistration(String workflowName,
                                       Class<?> startEventClass,
                                       String idPrefix,
                                       String workflowVersion,
                                       Consumer<SimpleWorkflowContext> body,
                                       Consumer<WorkflowCustomization> customizer) {

        /**
         * No-op extra customization: the default for every registration that does not register status-change listeners.
         */
        private static final Consumer<WorkflowCustomization> NO_EXTRA_CUSTOMIZATION = w -> {
        };

        /**
         * Full constructor with an explicit version and a no-op extra customizer (the INV-11 multi-version path).
         *
         * @param workflowName    the logical workflow name.
         * @param startEventClass the event type that starts an instance.
         * @param idPrefix        prefix applied to the {@code orderId} payload value to form the workflow id.
         * @param workflowVersion the semver version this definition is registered under.
         * @param body            the workflow body.
         */
        public WorkflowRegistration(String workflowName, Class<?> startEventClass,
                                    String idPrefix, String workflowVersion,
                                    Consumer<SimpleWorkflowContext> body) {
            this(workflowName, startEventClass, idPrefix, workflowVersion, body, NO_EXTRA_CUSTOMIZATION);
        }

        /**
         * Convenience constructor for a single-version definition pinned to {@link MessageType#DEFAULT_VERSION}
         * ({@code "0.0.1"}) — the version the existing single-definition factories (order/cancelling/retrying/
         * timeout/start-only workflows) registered under implicitly before INV-11 added the explicit version field.
         *
         * @param workflowName    the logical workflow name.
         * @param startEventClass the event type that starts an instance.
         * @param idPrefix        prefix applied to the {@code orderId} payload value to form the workflow id.
         * @param body            the workflow body.
         */
        public WorkflowRegistration(String workflowName, Class<?> startEventClass,
                                    String idPrefix, Consumer<SimpleWorkflowContext> body) {
            this(workflowName, startEventClass, idPrefix, MessageType.DEFAULT_VERSION, body, NO_EXTRA_CUSTOMIZATION);
        }

        /**
         * Convenience constructor for a single-version definition pinned to {@link MessageType#DEFAULT_VERSION} with an
         * extra customizer (e.g. the INV-17 status-change-listener registration).
         *
         * @param workflowName    the logical workflow name.
         * @param startEventClass the event type that starts an instance.
         * @param idPrefix        prefix applied to the {@code orderId} payload value to form the workflow id.
         * @param body            the workflow body.
         * @param customizer      extra customization applied inside {@code .customized(...)}.
         */
        public WorkflowRegistration(String workflowName, Class<?> startEventClass,
                                    String idPrefix, Consumer<SimpleWorkflowContext> body,
                                    Consumer<WorkflowCustomization> customizer) {
            this(workflowName, startEventClass, idPrefix, MessageType.DEFAULT_VERSION, body, customizer);
        }
    }

    /**
     * The default registration: the {@link OrderWorkflow} workhorse driven by {@code OrderPlacedEvent}, ids prefixed
     * {@code order-}.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the order-workflow registration.
     */
        public static WorkflowRegistration orderWorkflow(CountingEffects effects) {
        var workflow = new OrderWorkflow(effects);
        return new WorkflowRegistration(OrderWorkflow.WORKFLOW_NAME, OrderPlacedEvent.class, "order-",
                                        workflow::execute);
    }

    /**
     * The INV-7 registration: the {@link CancellingWorkflow} (runs one step then {@code ctx.cancel()}), driven by
     * {@code CancelRequestedEvent}, ids prefixed {@code cancel-}. Used by the {@code TerminalIsFinal} scenario to drive
     * a genuine terminal (CANCELLED) workflow status.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the cancelling-workflow registration.
     */
        public static WorkflowRegistration cancellingWorkflow(CountingEffects effects) {
        var workflow = new CancellingWorkflow(effects);
        return new WorkflowRegistration(CancellingWorkflow.WORKFLOW_NAME, CancelRequestedEvent.class, "cancel-",
                                        workflow::execute);
    }

    /**
     * The INV-8 registration: the {@link RetryingWorkflow} (one always-failing step under
     * {@code RetryPolicy.maxRetries(k)}), driven by {@code RetryRequestedEvent}, ids prefixed {@code retry-}. Used by
     * the {@code RetryBound} scenario to drive a step to retry exhaustion and assert the attempt-record count stays
     * within the policy bound.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the retrying-workflow registration.
     */
        public static WorkflowRegistration retryingWorkflow(CountingEffects effects) {
        var workflow = new RetryingWorkflow(effects);
        return new WorkflowRegistration(RetryingWorkflow.WORKFLOW_NAME, RetryRequestedEvent.class, "retry-",
                                        workflow::execute);
    }

    /**
     * The at-most-once <strong>retry-resume</strong> registration: a {@link RetryResumeWorkflow} (a single retried,
     * succeeding {@code execute} step), driven by {@code RetryRequestedEvent}, ids prefixed {@code retryresume-}. Used by
     * the dedicated retry-resume test to verify that a first attempt interrupted by a crash (write-then-vanish on its
     * {@code COMPLETED}) is turned into a {@code RETRYING} transition + a fresh attempt — not re-run in place — and that
     * the step then completes. Scenario-pinned (never in {@code defaultRegistrations()}).
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the retry-resume-workflow registration.
     */
        public static WorkflowRegistration retryResumeWorkflow(CountingEffects effects) {
        var workflow = new RetryResumeWorkflow(effects);
        return new WorkflowRegistration(RetryResumeWorkflow.WORKFLOW_NAME, RetryRequestedEvent.class, "retryresume-",
                                        workflow::execute);
    }

    /**
     * The INV-16 <strong>no-retry</strong> registration: a {@link FailingWorkflow} pinned to {@code maxRetries(0)} (one
     * always-throwing step awaited via the blocking {@code awaitExecute}, so its failure propagates to a terminal FAILED
     * workflow status immediately), driven by {@code FailRequestedEvent}, ids prefixed {@code fail-}. Used by the
     * {@code FailurePropagation} scenario to drive a step's uncaught exception to a FAILED terminus and assert the
     * failure propagated (failing step recorded terminally-failed, workflow FAILED, no post-failure step ran).
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the no-retry failing-workflow registration.
     */
        public static WorkflowRegistration failingWorkflowNoRetry(CountingEffects effects) {
        var workflow = new FailingWorkflow(effects, 0);
        return new WorkflowRegistration(FailingWorkflow.FAILING_WORKFLOW_NAME_NO_RETRY, FailRequestedEvent.class, "fail-",
                                        workflow::execute);
    }

    /**
     * The INV-16 <strong>retry-exhaustion</strong> registration: a {@link FailingWorkflow} pinned to
     * {@code maxRetries(FAIL_EXHAUSTION_MAX_RETRIES)} (the always-throwing step exhausts its retry policy, then the
     * failure propagates to a terminal FAILED workflow status), driven by {@code FailExhaustionRequestedEvent}, ids
     * prefixed {@code failretry-}. Used by the {@code FailurePropagation} scenario to drive a step to retry exhaustion
     * and assert the failure <em>propagates</em> to FAILED — distinct from INV-8 ({@code RetryBound}), which bounds the
     * attempt-record count on the same shape (and whose {@link RetryingWorkflow} absorbs the failure to COMPLETED).
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the retry-exhaustion failing-workflow registration.
     */
        public static WorkflowRegistration failingWorkflowRetryExhaustion(CountingEffects effects) {
        var workflow = new FailingWorkflow(effects, FAIL_EXHAUSTION_MAX_RETRIES);
        return new WorkflowRegistration(FailingWorkflow.FAILING_WORKFLOW_NAME_RETRY_EXHAUSTION,
                                        FailExhaustionRequestedEvent.class, "failretry-", workflow::execute);
    }

    /**
     * The {@code maxRetries} the INV-16 retry-exhaustion {@link FailingWorkflow} carries: the failing step records
     * {@code STARTED + RETRYING×this + FAILED}, then the workflow propagates to FAILED. Small so the deterministic
     * scenario stays fast.
     */
    public static final int FAIL_EXHAUSTION_MAX_RETRIES = 2;

    /**
     * The INV-9 registration: the {@link TimeoutWorkflow} (one step then a {@code waitForEvent} whose event is never
     * delivered, under a short timeout), driven by {@code TimeoutRequestedEvent}, ids prefixed {@code timeout-}. Used by
     * the {@code TimeoutsFire} scenario to drive a wait step past its configured timeout (via virtual time) and assert
     * the step records a {@code TIMED_OUT} outcome.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the timeout-workflow registration.
     */
        public static WorkflowRegistration timeoutWorkflow(CountingEffects effects) {
        var workflow = new TimeoutWorkflow(effects);
        return new WorkflowRegistration(TimeoutWorkflow.WORKFLOW_NAME, TimeoutRequestedEvent.class, "timeout-",
                                        workflow::execute);
    }

    /**
     * The INV-21 <strong>retry-edges</strong> registration: the {@link RetryTimingWorkflow#executeRetryEdges} body
     * (three backoff-strategy steps fixed/linear/exponential, a {@code retryWhile}-bounded step, and {@code onRetry}
     * fire counting), driven by {@code RetryEdgesRequestedEvent}, ids prefixed {@code retryedge-}. Used by the
     * {@code RetryTimingAndExhaustionEdges} scenario to drive backoff timing across crash/replay, the {@code retryWhile}
     * predicate bounding the attempt records, and the {@code onRetry}-not-re-fired probe.
     *
     * @param effects      counting side-effect registry the workflow records attempts into (survives crashes).
     * @param onRetryFires counting registry the {@code onRetry} handlers record into (survives crashes, like effects).
     * @return the retry-edges registration.
     */
        public static WorkflowRegistration retryTimingRetryEdgesWorkflow(CountingEffects effects,
                                                                     OnRetryFires onRetryFires) {
        var workflow = new RetryTimingWorkflow(effects, onRetryFires, new java.util.concurrent.CountDownLatch(1));
        return new WorkflowRegistration(RetryTimingWorkflow.RETRY_EDGES_WORKFLOW_NAME, RetryEdgesRequestedEvent.class,
                                        "retryedge-", workflow::executeRetryEdges);
    }

    /**
     * The INV-21 <strong>per-attempt-execute-timeout</strong> registration: the
     * {@link RetryTimingWorkflow#executeTimeoutEdge} body (a slow {@code execute} whose action blocks on the given
     * never-released {@code slowExecuteLatch} past its per-attempt {@code timeout}), driven by
     * {@code RetryTimeoutRequestedEvent}, ids prefixed {@code retrytimeout-}. Used by the
     * {@code RetryTimingAndExhaustionEdges} scenario to drive the per-attempt {@code execute} timeout (the D5 residual
     * INV-9 left to the wait path) to a terminal {@code TIMED_OUT} via the wall-clock {@code orTimeout}, advanced by the
     * harness's lock-step {@code MutableClock}.
     *
     * @param effects          counting side-effect registry the workflow records into (survives crashes).
     * @param onRetryFires     counting registry the {@code onRetry} handlers record into (survives crashes).
     * @param slowExecuteLatch a latch the slow {@code execute} action blocks on indefinitely (never released by the
     *                         scenario), so the per-attempt timeout window deterministically elapses.
     * @return the per-attempt-execute-timeout registration.
     */
        public static WorkflowRegistration retryTimingTimeoutEdgeWorkflow(CountingEffects effects,
                                                                      OnRetryFires onRetryFires,
                                                                      java.util.concurrent.CountDownLatch slowExecuteLatch) {
        var workflow = new RetryTimingWorkflow(effects, onRetryFires, slowExecuteLatch);
        return new WorkflowRegistration(RetryTimingWorkflow.TIMEOUT_EDGE_WORKFLOW_NAME,
                                        RetryTimeoutRequestedEvent.class, "retrytimeout-", workflow::executeTimeoutEdge);
    }

    /**
     * The INV-10 registration: the {@link StartOnlyWorkflow} (one step then a never-arriving {@code waitForEvent} that
     * keeps the instance LIVE), driven by {@code StartOnlyRequestedEvent}, ids prefixed {@code start-}. Used by the
     * {@code OneInstancePerStart} scenario to deliver a duplicate START while the instance is live and assert the engine
     * dedups it (one instance, no second STARTED).
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the start-only-workflow registration.
     */
        public static WorkflowRegistration startOnlyWorkflow(CountingEffects effects) {
        var workflow = new StartOnlyWorkflow(effects);
        return new WorkflowRegistration(StartOnlyWorkflow.WORKFLOW_NAME, StartOnlyRequestedEvent.class, "start-",
                                        workflow::execute);
    }

    /**
     * The INV-23 registration: the {@link NestedPrimitiveWorkflow} (a DELIBERATELY-MISUSED workflow whose outer
     * {@code execute} action lambda calls another primitive — the §3.1-forbidden nested-primitive anti-pattern), driven
     * by {@code NestedPrimitiveRequestedEvent}, ids prefixed {@code selfprot-}. Used by the {@code EngineSelfProtection}
     * scenario to PROBE the engine's self-protection at the nested-primitive failure surface: the nested call deadlocks
     * the per-instance single-threaded task queue, so the instance reaches NO terminal status and is observed as stuck by
     * the harness's SHORT wall-clock deadline. Never folded into the always-on fuzz (a stuck instance would deliberately
     * stall non-terminally, which the liveness horizon check would read as a hang).
     *
     * @param effects counting side-effect registry the (unreachable-completion) action bodies record into.
     * @return the nested-primitive-workflow registration.
     */
        public static WorkflowRegistration nestedPrimitiveWorkflow(CountingEffects effects) {
        var workflow = new io.axoniq.framework.workflow.simulation.workflow.NestedPrimitiveWorkflow(effects);
        return new WorkflowRegistration(
                io.axoniq.framework.workflow.simulation.workflow.NestedPrimitiveWorkflow.WORKFLOW_NAME,
                io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.NestedPrimitiveRequestedEvent.class,
                "selfprot-", workflow::execute);
    }

    /**
     * The INV-11 registrations: <strong>two</strong> definitions of {@link VersionedOrderWorkflow} sharing one
     * {@code workflowName} and start event ({@code VersionedOrderRequestedEvent}) but registered under different
     * versions ({@link VersionedOrderWorkflow#VERSION_V1 v1} runs the v1 body, {@link VersionedOrderWorkflow#VERSION_V2
     * v2} runs the v2 body), ids prefixed {@code vorder-}. Used by the {@code VersionRoutingSound} scenario and folded
     * into the fuzz set so a fresh start spawns at the highest version and every event of the instance carries exactly
     * that one version, deterministically across crash/replay. Returns a list (vs. a single registration) because both
     * versions register into the one module.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the two versioned registrations (v1 then v2), to register together in one module.
     */
        public static List<WorkflowRegistration> versionedOrderWorkflow(CountingEffects effects) {
        var workflow = new VersionedOrderWorkflow(effects);
        return List.of(
                new WorkflowRegistration(VersionedOrderWorkflow.WORKFLOW_NAME, VersionedOrderRequestedEvent.class,
                                         "vorder-", VersionedOrderWorkflow.VERSION_V1, workflow::executeV1),
                new WorkflowRegistration(VersionedOrderWorkflow.WORKFLOW_NAME, VersionedOrderRequestedEvent.class,
                                         "vorder-", VersionedOrderWorkflow.VERSION_V2, workflow::executeV2));
    }

    /**
     * The INV-12 registration: a <strong>single</strong> definition of {@link MigratingOrderWorkflow} pinned to
     * {@link MigratingOrderWorkflow#INITIAL_VERSION} (so a fresh start spawns there and the body then migrates forward),
     * driven by {@code MigrateRequestedEvent}, ids prefixed {@code vmig-}. Used by the {@code MigrateVersionContract}
     * scenario and folded into the fuzz set so the in-body {@code ctx.migrateVersion} record is driven through the
     * crash/restart/reorder faults — exactly where a replay-stability bug in the migration record would show. Unlike
     * {@link #versionedOrderWorkflow} (two definitions, multi-version routing), this is one definition that performs an
     * in-body version migration via the {@code migrateVersion} primitive.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the migrating-workflow registration.
     */
        public static WorkflowRegistration migratingOrderWorkflow(CountingEffects effects) {
        var workflow = new MigratingOrderWorkflow(effects);
        return new WorkflowRegistration(MigratingOrderWorkflow.WORKFLOW_NAME, MigrateRequestedEvent.class, "vmig-",
                                        MigratingOrderWorkflow.INITIAL_VERSION, workflow::execute);
    }

    /**
     * The INV-20 registrations: <strong>three</strong> definitions of {@link VersioningEdgesWorkflow} sharing one
     * {@code workflowName} and start event ({@code VersioningEdgesRequestedEvent}) but registered under three different
     * versions ({@link VersioningEdgesWorkflow#VERSION_LOW 1.0.0}, {@link VersioningEdgesWorkflow#VERSION_MID 1.5.0},
     * {@link VersioningEdgesWorkflow#VERSION_HIGH 2.0.0}), ids prefixed {@code vedge-}. All three register the
     * <em>same</em> body, so a fresh start (spawned at the highest version) and any closest-sibling re-route replay the
     * recorded steps cleanly (no drift). Used by the {@code VersioningEdges} scenario and folded into the fuzz set so the
     * downgrade-rejection + multi-{@code changeId} migration edges ride the same crash/restart/reorder faults as the
     * order workhorse. Returns a list (vs. a single registration) because all three versions register into one module.
     * <p>
     * Deeper than {@link #versionedOrderWorkflow} (two versions, INV-11) and {@link #migratingOrderWorkflow} (one
     * in-body migration, INV-12): this combines a 3-version registry with a multi-{@code changeId} migrating body whose
     * third migration is a rejected downgrade.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the three versioned registrations (low, mid, high), to register together in one module.
     */
        public static List<WorkflowRegistration> versioningEdgesWorkflow(CountingEffects effects) {
        var workflow = new VersioningEdgesWorkflow(effects);
        return List.of(
                new WorkflowRegistration(VersioningEdgesWorkflow.WORKFLOW_NAME, VersioningEdgesRequestedEvent.class,
                                         "vedge-", VersioningEdgesWorkflow.VERSION_LOW, workflow::execute),
                new WorkflowRegistration(VersioningEdgesWorkflow.WORKFLOW_NAME, VersioningEdgesRequestedEvent.class,
                                         "vedge-", VersioningEdgesWorkflow.VERSION_MID, workflow::execute),
                new WorkflowRegistration(VersioningEdgesWorkflow.WORKFLOW_NAME, VersioningEdgesRequestedEvent.class,
                                         "vedge-", VersioningEdgesWorkflow.VERSION_HIGH, workflow::execute));
    }

    /**
     * The INV-20 <strong>reduced</strong> registry: the {@link VersioningEdgesWorkflow} registered at only its two LOWER
     * versions ({@link VersioningEdgesWorkflow#VERSION_LOW 1.0.0} and {@link VersioningEdgesWorkflow#VERSION_MID 1.5.0}),
     * with the highest version ({@link VersioningEdgesWorkflow#VERSION_HIGH 2.0.0}) <em>dropped</em>. Used by the
     * {@code VersioningEdges} scenario's deeper-routing facet via {@link SimulationWorld#crashAndRecoverWith(List)}: an
     * instance recorded at {@code 2.0.0} (the highest version it freshly spawned + migrated to) recovers under this
     * registry, so the 4/5-pass lookup ({@code resolveDefinitionForReplay}) must route it to the closest registered
     * sibling {@code <= 2.0.0} — {@code 1.5.0} (mid), the highest registered version not exceeding the recorded state —
     * not 0 (stranded) and not 2 (double-handled). All three versions share one body, so replaying the recorded steps
     * under the routed mid definition is drift-free, letting the resumed final step complete observably.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the two lower versioned registrations (low, mid), the highest version intentionally dropped.
     */
        public static List<WorkflowRegistration> versioningEdgesWorkflowReducedRegistry(CountingEffects effects) {
        var workflow = new VersioningEdgesWorkflow(effects);
        return List.of(
                new WorkflowRegistration(VersioningEdgesWorkflow.WORKFLOW_NAME, VersioningEdgesRequestedEvent.class,
                                         "vedge-", VersioningEdgesWorkflow.VERSION_LOW, workflow::execute),
                new WorkflowRegistration(VersioningEdgesWorkflow.WORKFLOW_NAME, VersioningEdgesRequestedEvent.class,
                                         "vedge-", VersioningEdgesWorkflow.VERSION_MID, workflow::execute));
    }

    /**
     * The INV-13 registration: the {@link PayloadOrderWorkflow} (every step writes a distinct payload key — three
     * {@code execute} + {@code CombineGlobalAndLocalPayloadReducer} merges then one {@code modifyPayload} replace),
     * driven by {@code PayloadOrderRequestedEvent}, ids prefixed {@code payload-}. Used by the
     * {@code NoLostPayloadWrites} scenario and folded into the fuzz set so every payload write is driven through the
     * crash/restart/reorder faults — exactly where a lost write (a crash between a step's effect and its COMPLETED
     * commit, or a replay that rebuilds the payload) would show. The instance self-completes via execute/modifyPayload
     * steps, so a stuck one is a liveness failure rather than a silent pass.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the payload-order-workflow registration.
     */
        public static WorkflowRegistration payloadOrderWorkflow(CountingEffects effects) {
        var workflow = new PayloadOrderWorkflow(effects);
        return new WorkflowRegistration(PayloadOrderWorkflow.WORKFLOW_NAME, PayloadOrderRequestedEvent.class, "payload-",
                                        workflow::execute);
    }

    /**
     * The F-7 registration: the {@link io.axoniq.framework.workflow.simulation.workflow.DuplicatePayloadWorkflow} (a
     * {@code modifyPayload} step that is NOT the last step, followed by a never-arriving {@code waitForEvent} that keeps
     * the instance LIVE), driven by {@code DuplicatePayloadRequestedEvent}, ids prefixed {@code duppay-}. Used by the
     * {@code DuplicateTerminalPayloadRecordScenario} to settle the S-1 candidate (INV-2 {@code AtMostOnceRecording} at the
     * {@code modifyPayload}-on-post-crash-live-re-run surface). <strong>Scenario-only</strong>: deliberately NOT folded
     * into {@link SimulationWorld#defaultRegistrations()} so the always-on fuzz instance counts
     * (DstChaosFuzzTest {@code .hasSize(19)}, DstSmokeTest {@code .hasSize(12)}) are unperturbed — and because its body
     * trips INV-2 across a crash/recover by design, which the always-on per-step {@code assertAtMostOnceRecording} would
     * (correctly) flag.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the duplicate-payload-workflow registration.
     */
        public static WorkflowRegistration duplicatePayloadWorkflow(CountingEffects effects) {
        var workflow = new io.axoniq.framework.workflow.simulation.workflow.DuplicatePayloadWorkflow(effects);
        return new WorkflowRegistration(
                io.axoniq.framework.workflow.simulation.workflow.DuplicatePayloadWorkflow.WORKFLOW_NAME,
                io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DuplicatePayloadRequestedEvent.class,
                "duppay-", workflow::execute);
    }

    /**
     * The F-8 <strong>typed-{@code awaitExecute}</strong> registration: a
     * {@link io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow} whose typed
     * {@code awaitExecute(stepName, Class, Supplier)} blocks on the given never-released {@code blockingLatch} past its
     * (default 5s) per-attempt {@code execute} timeout, driven by {@code BlockingAwaitExecuteTimeoutRequestedEvent}, ids
     * prefixed {@code awaitexec-}. Used by the {@code BlockingAwaitTimeoutSurfaceScenario} to settle the S-2 candidate
     * (finding F-8 — the blocking-convenience timeout-surfacing asymmetry). <strong>Scenario-only</strong>: deliberately
     * NOT folded into {@link SimulationWorld#defaultRegistrations()} so the always-on fuzz instance counts
     * (DstChaosFuzzTest {@code .hasSize(19)}, DstSmokeTest {@code .hasSize(12)}) are unperturbed — and because a
     * deliberately-timing-out blocking convenience call rides the {@code orTimeout} residual kept out of the per-step
     * fuzz set (Phase-3 D5), exactly like INV-9.
     *
     * @param workflow the (caller-held) workflow whose {@link BlockingAwaitTimeoutWorkflow#capturedSurface()} the
     *                 scenario reads after the run; its {@code execute}-path action blocks on a never-released latch the
     *                 caller constructed it with, so the per-attempt timeout window deterministically elapses.
     * @return the typed-{@code awaitExecute} F-8 registration.
     */
        public static WorkflowRegistration blockingAwaitExecuteTimeoutWorkflow(
            io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow workflow) {
        return new WorkflowRegistration(
                io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow.AWAIT_EXECUTE_WORKFLOW_NAME,
                io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BlockingAwaitExecuteTimeoutRequestedEvent.class,
                "awaitexec-", workflow::executeTimeoutAwaitExecute);
    }

    /**
     * The F-8 <strong>untyped-{@code awaitExecute}</strong> registration: a
     * {@link io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow} whose untyped
     * {@code awaitExecute(stepName, Map, processor, customizer)} action blocks on the given never-released
     * {@code blockingLatch} past a short explicit {@code timeout}, driven by
     * {@code BlockingUntypedAwaitExecuteTimeoutRequestedEvent}, ids prefixed {@code awaitexecu-}. Scenario-only (see
     * {@link #blockingAwaitExecuteTimeoutWorkflow}).
     *
     * @param workflow the (caller-held) workflow whose {@link BlockingAwaitTimeoutWorkflow#capturedSurface()} the
     *                 scenario reads after the run; its {@code execute}-path action blocks on a never-released latch.
     * @return the untyped-{@code awaitExecute} F-8 registration.
     */
        public static WorkflowRegistration blockingUntypedAwaitExecuteTimeoutWorkflow(
            io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow workflow) {
        return new WorkflowRegistration(
                io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow.UNTYPED_AWAIT_EXECUTE_WORKFLOW_NAME,
                io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BlockingUntypedAwaitExecuteTimeoutRequestedEvent.class,
                "awaitexecu-", workflow::executeTimeoutUntypedAwaitExecute);
    }

    /**
     * The F-8 <strong>untyped-{@code awaitEvent}</strong> registration: a
     * {@link io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow} whose untyped
     * {@code awaitEvent(stepName, EventCondition)} ({@code EventConditions.never()}) times out, driven by
     * {@code BlockingUntypedAwaitEventTimeoutRequestedEvent}, ids prefixed {@code awaitevtu-}. Scenario-only (see
     * {@link #blockingAwaitExecuteTimeoutWorkflow}).
     *
     * @param workflow the (caller-held) workflow whose {@link BlockingAwaitTimeoutWorkflow#capturedSurface()} the
     *                 scenario reads after the run (the {@code awaitEvent} paths never run the {@code execute}-path
     *                 action, so the latch is irrelevant).
     * @return the untyped-{@code awaitEvent} F-8 registration.
     */
        public static WorkflowRegistration blockingUntypedAwaitEventTimeoutWorkflow(
            io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow workflow) {
        return new WorkflowRegistration(
                io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow.UNTYPED_AWAIT_EVENT_WORKFLOW_NAME,
                io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BlockingUntypedAwaitEventTimeoutRequestedEvent.class,
                "awaitevtu-", workflow::executeTimeoutUntypedAwaitEvent);
    }

    /**
     * The F-8 <strong>typed-{@code awaitEvent}</strong> CONTRAST registration: a
     * {@link io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow} whose typed
     * {@code awaitEvent(stepName, Class, conditions, customizer)} event is never delivered and times out — the path that
     * correctly surfaces {@code StepTimedOutException}, driven by {@code BlockingTypedAwaitEventTimeoutRequestedEvent},
     * ids prefixed {@code awaitevt-}. Scenario-only (see {@link #blockingAwaitExecuteTimeoutWorkflow}).
     *
     * @param workflow the (caller-held) workflow whose {@link BlockingAwaitTimeoutWorkflow#capturedSurface()} the
     *                 scenario reads after the run.
     * @return the typed-{@code awaitEvent} F-8 contrast registration.
     */
        public static WorkflowRegistration blockingTypedAwaitEventTimeoutWorkflow(
            io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow workflow) {
        return new WorkflowRegistration(
                io.axoniq.framework.workflow.simulation.workflow.BlockingAwaitTimeoutWorkflow.TYPED_AWAIT_EVENT_WORKFLOW_NAME,
                io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BlockingTypedAwaitEventTimeoutRequestedEvent.class,
                "awaitevt-", workflow::executeTimeoutTypedAwaitEvent);
    }

    /**
     * The INV-14 registration: the {@link CombinatorWorkflow} (three parallel {@code execute} branches folded through
     * all three combinators — {@code anyMatch}/{@code allMatch}/{@code noneMatch}), driven by
     * {@code CombinatorRequestedEvent}, ids prefixed {@code comb-}. Used by the {@code CombinatorConsistency} scenario
     * and folded into the fuzz set so each combinator's decision is driven through the crash/restart/reorder faults —
     * exactly where a replay-stability bug (a combinator resolving a different decision after replay) would show. The
     * instance self-completes via execute steps, so a stuck one is a liveness failure rather than a silent pass.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the combinator-workflow registration.
     */
        public static WorkflowRegistration combinatorWorkflow(CountingEffects effects) {
        var workflow = new CombinatorWorkflow(effects);
        return new WorkflowRegistration(CombinatorWorkflow.WORKFLOW_NAME, CombinatorRequestedEvent.class, "comb-",
                                        workflow::execute);
    }

    /**
     * The candidate-finding S-3 registration: the {@link BackoffOverflowWorkflow} (a single always-failing
     * {@code execute} step under a LARGE {@code maxRetries} with {@code BackoffStrategy.exponential}), driven by
     * {@code BackoffOverflowRequestedEvent}, ids prefixed {@code boverflow-}. Used by the
     * {@code BackoffOverflowScenario} to settle the S-3 candidate (the {@code BackoffStrategy.exponential}
     * shift/{@code Duration} overflow at large attempt counts — an INV-8/INV-21 backoff-arithmetic edge): the engine
     * retries the step until the exponential factor overflows {@code Duration} and the resulting {@code ArithmeticException}
     * (computed on the workflow thread in {@code RetryableExecuteDelegate.handleAttemptFailure}) wedges the instance
     * non-terminally (the {@code handleWorkflowException} {@code default}-branch sink, the same wedge as F-6/S-4).
     * <strong>Scenario-only</strong>: deliberately NOT folded into {@link SimulationWorld#defaultRegistrations()} so the
     * always-on fuzz instance counts (DstChaosFuzzTest {@code .hasSize(19)}, DstSmokeTest {@code .hasSize(12)}) are
     * unperturbed — and because a deliberately-wedging body leaves an instance non-terminal, which the always-on
     * liveness-horizon check would (correctly) read as a hang.
     *
     * @param effects counting side-effect registry the failing step records attempts into (survives crashes).
     * @return the backoff-overflow-workflow registration.
     */
        public static WorkflowRegistration backoffOverflowWorkflow(CountingEffects effects) {
        var workflow = new BackoffOverflowWorkflow(effects);
        return new WorkflowRegistration(BackoffOverflowWorkflow.WORKFLOW_NAME, BackoffOverflowRequestedEvent.class,
                                        "boverflow-", workflow::execute);
    }

    /**
     * The candidate-finding S-5 registration: the {@link AnyMatchNoMatchWorkflow} (three {@code execute} branches that
     * ALL complete but NONE satisfies the combinator predicate, then reads the {@code anyMatch} winner-derived
     * accessors), driven by {@code AnyMatchNoMatchRequestedEvent}, ids prefixed {@code anynomatch-}. Used by the
     * {@code AnyMatchNoMatchWinnerScenario} to settle the S-5 candidate and extend INV-14 ({@code CombinatorConsistency})
     * coverage: it characterizes the {@code anyMatch} winner-result semantics on the no-predicate-match-but-all-completed
     * path ({@code AnyMatchCombinatorDelegate} sets {@code fallback.orElse(results[0])} as the winner, so the
     * winner-derived {@code success()}/{@code result()}/{@code resultAs()} read the first completed branch). The
     * predicate-level decision — {@code matched()} empty — is itself CORRECT (consistent with INV-14); only the
     * winner-derived accessors on the no-match path are the gap. <strong>Scenario-only</strong>: deliberately NOT folded
     * into {@link SimulationWorld#defaultRegistrations()} so the always-on fuzz instance counts (DstChaosFuzzTest
     * {@code .hasSize(19)}, DstSmokeTest {@code .hasSize(12)}) are unperturbed.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the any-match-no-match-workflow registration.
     */
        public static WorkflowRegistration anyMatchNoMatchWorkflow(CountingEffects effects) {
        var workflow = new AnyMatchNoMatchWorkflow(effects);
        return new WorkflowRegistration(AnyMatchNoMatchWorkflow.WORKFLOW_NAME, AnyMatchNoMatchRequestedEvent.class,
                                        "anynomatch-", workflow::execute);
    }

    /**
     * The INV-19 registration: the {@link ReducerWorkflow} (steps that deterministically exercise all three payload
     * reducers — a {@code combine} seed, a {@code local_only} {@code modifyPayload} replace, a {@code combine} that must
     * appear, a {@code global_only} {@code execute} whose result must be discarded, and a
     * {@code parameterPayloadReducer(combine)} step demonstrating the parameter-side input view), driven by
     * {@code ReducerRequestedEvent}, ids prefixed {@code reducer-}. Used by the {@code PayloadReducerSemantics} scenario
     * and folded into the fuzz set so each reducer's documented merge is driven through the crash/restart/reorder faults
     * — exactly where a reducer mis-application or a replay that rebuilds a different payload would show. The instance
     * self-completes via execute/modifyPayload steps, so a stuck one is a liveness failure rather than a silent pass.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the reducer-workflow registration.
     */
        public static WorkflowRegistration reducerWorkflow(CountingEffects effects) {
        var workflow = new ReducerWorkflow(effects);
        return new WorkflowRegistration(ReducerWorkflow.WORKFLOW_NAME, ReducerRequestedEvent.class, "reducer-",
                                        workflow::execute);
    }

    /**
     * The INV-19 null-value-edge registration: the {@link ReducerWorkflow}'s {@code nullEdge} body (a {@code combine}
     * step whose result map carries a {@code null} value — edge (a)), driven by {@code ReducerNullEdgeRequestedEvent},
     * ids prefixed {@code reducer-} (same scope as the standard reducer workflow, a distinct workflow name). Used by the
     * {@code PayloadReducerSemantics} scenario to observe and characterize the engine's actual {@code null}-under-combine
     * handling against the documented fold; <strong>not</strong> folded into the always-on fuzz, so a serialization
     * quirk on a {@code null} map value cannot flake the 1000-seed sweep.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the reducer null-edge-workflow registration.
     */
        public static WorkflowRegistration reducerNullEdgeWorkflow(CountingEffects effects) {
        var workflow = new ReducerWorkflow(effects);
        return new WorkflowRegistration(ReducerWorkflow.WORKFLOW_NAME + "NullEdge", ReducerNullEdgeRequestedEvent.class,
                                        "reducer-", workflow::nullEdge);
    }

    /**
     * The candidate-finding F-6 (S-4 generalization) registration: the {@link ReducerWorkflow}'s
     * {@code throwingModifier} body (a {@code modifyPayload} step whose modifier lambda throws a plain
     * {@code RuntimeException} between primitives), driven by {@code ReducerThrowingModifierRequestedEvent}, ids
     * prefixed {@code reducer-} (same scope as the standard reducer workflow, a distinct workflow name). Used by the
     * {@code PayloadReducerSemantics} scenario to observe and characterize that ANY unexpected runtime exception from a
     * between-primitives user lambda hits the same {@code handleWorkflowException} {@code default}-branch wedge as F-6's
     * null-payload trigger — the instance is left non-terminal (a liveness stall), the exception only logged, and replay
     * re-hits it (recovery-unsafe). <strong>Scenario-only</strong>: deliberately NOT folded into
     * {@link SimulationWorld#defaultRegistrations()} so the always-on fuzz instance counts (DstChaosFuzzTest
     * {@code .hasSize(19)}, DstSmokeTest {@code .hasSize(12)}) are unperturbed — and because a deliberately-wedging body
     * leaves an instance non-terminal, which the always-on liveness-horizon check would (correctly) read as a hang.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the reducer throwing-modifier-workflow registration.
     */
        public static WorkflowRegistration reducerThrowingModifierWorkflow(CountingEffects effects) {
        var workflow = new ReducerWorkflow(effects);
        return new WorkflowRegistration(ReducerWorkflow.WORKFLOW_NAME + "ThrowingModifier",
                                        ReducerThrowingModifierRequestedEvent.class, "reducer-",
                                        workflow::throwingModifier);
    }

    /**
     * The INV-15 registration: the {@link CorrelatedWaitWorkflow} (a setup {@code execute} step then a
     * {@code waitForEvent} correlated on the instance's <strong>own</strong> per-instance key, then records the matched
     * event's key), driven by {@code CorrelatedWaitRequestedEvent}, ids prefixed {@code corr-}. Used by the
     * {@code EventCorrelationExact} scenario and folded into the fuzz set so the {@code waitForEvent} correlation surface
     * is driven through the crash/restart/reorder faults — exactly where a cross-wakeup (an associated event waking a
     * non-matching waiter) or a duplicate-driven second wait completion would show. Each instance correlates on a
     * distinct key, so a {@code CorrelatedSignalEvent} delivered for one key must wake only that instance.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the correlated-wait-workflow registration.
     */
        public static WorkflowRegistration correlatedWaitWorkflow(CountingEffects effects) {
        var workflow = new CorrelatedWaitWorkflow(effects);
        return new WorkflowRegistration(CorrelatedWaitWorkflow.WORKFLOW_NAME, CorrelatedWaitRequestedEvent.class,
                                        "corr-", workflow::execute);
    }

    /**
     * The external step-cancellation registration: the {@link ExternalCancelCompensationWorkflow} (suspends on a
     * blocking {@code awaitEvent} whose signal is never published, catches the cancellation an external
     * {@code cancelRunningStep(...)} surfaces, compensates, completes), driven by {@code ExternalCancelRequestedEvent},
     * ids prefixed {@code extcancel-}. Scenario-pinned only — the blocking wait never resolves without an external
     * cancel, so folding it into the default fuzz set would (correctly) trip the liveness asserts.
     *
     * @param effects counting side-effect registry the compensation body records into.
     * @return the external-cancel workflow registration.
     */
        public static WorkflowRegistration externalCancelWorkflow(CountingEffects effects) {
        var workflow = new ExternalCancelCompensationWorkflow(effects);
        return new WorkflowRegistration(ExternalCancelCompensationWorkflow.WORKFLOW_NAME,
                                        ExternalCancelRequestedEvent.class,
                                        "extcancel-", workflow::execute);
    }

    /**
     * The backoff-cancellation registration: the {@link BackoffCancelWorkflow} (a retrying {@code execute} step parked
     * in a long fixed-backoff window while the body waits on a correlated signal; the {@code mode} payload field
     * selects an in-body {@code ctx.cancel(...)} or a normal completion for the external-cancel drive), driven by
     * {@code BackoffCancelRequestedEvent}, ids prefixed {@code backoffcancel-}. Scenario-pinned only — the wait needs
     * the scenario's signal and the backoff needs an explicit virtual-time advance, so folding it into the default
     * fuzz set would (correctly) trip the liveness asserts.
     *
     * @param effects counting side-effect registry the flaky action records its attempts into.
     * @return the backoff-cancel workflow registration.
     */
        public static WorkflowRegistration backoffCancelWorkflow(CountingEffects effects) {
        var workflow = new BackoffCancelWorkflow(effects);
        return new WorkflowRegistration(BackoffCancelWorkflow.WORKFLOW_NAME,
                                        BackoffCancelRequestedEvent.class,
                                        "backoffcancel-", workflow::execute);
    }

    /**
     * The INV-17 registration: the {@link HookWorkflow} (runs one step then completes, reaching STARTED then a terminal
     * COMPLETED workflow status), driven by {@code HookRequestedEvent}, ids prefixed {@code hook-}. A counting
     * {@code WorkflowStatusChangeListener} is registered on STARTED and on COMPLETED via the {@code .customized(...)}
     * registration path (the optional {@link WorkflowRegistration#customizer()} step), recording each fire into the
     * crash-surviving {@link StatusHookFires} counter — exactly the way {@link CountingEffects} makes {@code execute}
     * side effects observable for F-0. Used by the {@code StatusHookFiresOncePerStatus} scenario to drive the instance to
     * a terminal status, then crash + replay, and check whether a registered status's hook re-fires (the lifecycle-hook
     * analogue of INV-6 / F-0). The listener fires from the engine's {@code EventSourcedWorkflowState#setStatus}, which
     * is reached on both the live event-evolution and the recovery replay paths, so a crash + replay that re-evolves the
     * committed STARTED/COMPLETED status events is exactly where a re-fire would show.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @param fires   counting status-hook-fire registry the listener records into (survives crashes, like {@code effects}).
     * @return the hook-workflow registration with the counting status-change listener wired via the customizer.
     */
        public static WorkflowRegistration hookWorkflow(CountingEffects effects, StatusHookFires fires) {
        var workflow = new HookWorkflow(effects);
        // WorkflowStatusChangeListener.onWorkflowStatus is a GENERIC method (<C extends WorkflowContext>), so it cannot
        // be a lambda target — use an anonymous class. context.workflowId() identifies the instance, so the
        // per-(workflowId, status) fire counter survives crashes the same way CountingEffects does.
        WorkflowStatusChangeListener countingListener = new WorkflowStatusChangeListener() {
            @Override
            public <C extends WorkflowContext> void onWorkflowStatus(WorkflowStatus status,
                                                                     C context) {
                fires.record(context.workflowId(), status);
            }
        };
        Consumer<WorkflowCustomization> listenerRegistration = customization ->
                // Registered for STARTED and COMPLETED — the two statuses HookWorkflow reaches.
                customization
                        .registerWorkflowStatusChangeListener(WorkflowStatus.STARTED, countingListener)
                        .registerWorkflowStatusChangeListener(WorkflowStatus.COMPLETED, countingListener);
        return new WorkflowRegistration(HookWorkflow.WORKFLOW_NAME, HookRequestedEvent.class, "hook-",
                                        workflow::execute, listenerRegistration);
    }

    /**
     * The INV-22 registration: the {@link CustomNamedWorkflow} (two {@code execute} steps then completes), driven by
     * {@code CustomNamedRequestedEvent}, ids prefixed {@code named-}, registered with a custom
     * {@code eventNameCustomizer} ({@link CustomNamedWorkflow#customizer()}: a custom namespace + workflow base name + a
     * {@code stepCompleted} status-suffix override). The customizer is wired via the {@code .customized(...)}
     * registration path's optional {@link WorkflowRegistration#customizer()} step (the same hook INV-17's status-change
     * listener uses) — {@code WorkflowCustomization.eventNameCustomizer(...)} — so the engine inherits it per step and
     * names every emitted step/status event through it ({@code EventMessageUtils} stamps the resulting
     * {@code QualifiedName} on each event's {@code MessageType}). Used by the {@code EventNameCustomizationSound} scenario
     * and folded into the fuzz set so the customized-name surface is driven through the crash/restart/reorder faults —
     * exactly where a customized name NOT applied, or a replay producing a DIFFERENT name, would show. The instance
     * self-completes via execute steps, so a stuck one is a liveness failure rather than a silent pass.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the custom-named-workflow registration with the custom event-name customizer wired via the customizer.
     */
        public static WorkflowRegistration customNamedWorkflow(CountingEffects effects) {
        var workflow = new CustomNamedWorkflow(effects);
        Consumer<WorkflowCustomization> customizerRegistration =
                customization -> customization.eventNameCustomizer(CustomNamedWorkflow.customizer());
        return new WorkflowRegistration(CustomNamedWorkflow.WORKFLOW_NAME, CustomNamedRequestedEvent.class, "named-",
                                        workflow::execute, customizerRegistration);
    }

    /**
     * The Phase-1 production-realism <strong>saga</strong> registration (no-compensation-retry variant): the
     * {@link SagaOrderWorkflow} order-fulfillment saga (reserve stock → charge payment with retries → await an external
     * fulfillment confirmation under a timeout → notify), whose catch branches run <strong>compensation steps</strong>
     * (release stock / refund payment, NO retry policy) before terminating via {@code ctx.cancel()} (timeout branch) or
     * {@code ctx.fail(...)} (charge-failure branch). Driven by {@code SagaOrderPlacedEvent}, ids prefixed {@code saga-}.
     * Used by the saga-compensation scenarios to probe crash windows in and around the compensation chain.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the no-compensation-retry saga registration.
     */
        public static WorkflowRegistration sagaOrderWorkflow(CountingEffects effects) {
        var workflow = SagaOrderWorkflow.withoutCompensationRetry(effects);
        return new WorkflowRegistration(SagaOrderWorkflow.WORKFLOW_NAME_NO_COMP_RETRY, SagaOrderPlacedEvent.class,
                                        "saga-", workflow::execute);
    }

    /**
     * The Phase-1 production-realism <strong>saga</strong> registration (compensation-retry variant): the same
     * {@link SagaOrderWorkflow} shape with {@code maxRetries(SagaOrderWorkflow.COMPENSATION_MAX_RETRIES)} on both
     * compensation steps — the recommended production authoring the scenarios contrast against the fragile no-retry
     * variant under the same crash window. Driven by {@code SagaRetryCompOrderPlacedEvent}, ids prefixed
     * {@code sagarc-}.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the compensation-retry saga registration.
     */
        public static WorkflowRegistration sagaRetryCompOrderWorkflow(CountingEffects effects) {
        var workflow = SagaOrderWorkflow.withCompensationRetry(effects);
        return new WorkflowRegistration(SagaOrderWorkflow.WORKFLOW_NAME_COMP_RETRY, SagaRetryCompOrderPlacedEvent.class,
                                        "sagarc-", workflow::execute);
    }

    /**
     * The Phase-1 <strong>doomed-attempt probe</strong> registration: the {@link SagaOrderWorkflow} with the engine's
     * DEFAULT 5s per-attempt timeout pinned on every {@code execute} step (and no compensation retry). Used by the
     * doomed-compensation scenario: after the fulfillment wait's timeout fires (which requires the era-crossing
     * virtual advance), the compensation step is entered with its deadline already elapsed — exposing
     * {@code ExecuteDelegate}'s dispatch-before-deadline-check behaviour (the action runs, its result is discarded,
     * the step records TIMED_OUT). Driven by {@code SagaDefaultTimeoutOrderPlacedEvent}, ids prefixed {@code sagadt-}.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the default-timeout saga registration.
     */
        public static WorkflowRegistration sagaDefaultTimeoutOrderWorkflow(CountingEffects effects) {
        var workflow = SagaOrderWorkflow.withDefaultTimeouts(effects);
        return new WorkflowRegistration(SagaOrderWorkflow.WORKFLOW_NAME_DEFAULT_TIMEOUT,
                                        SagaDefaultTimeoutOrderPlacedEvent.class, "sagadt-", workflow::execute);
    }

    /**
     * The Phase-2 production-realism <strong>long-parked</strong> registration: the
     * {@link SubscriptionRenewalWorkflow} (register, then park up to 30 days on an external renewal decision; decision
     * → renew → COMPLETED, window elapsed → expire → CANCELLED). Driven by {@code SubscriptionStartedEvent}, ids
     * prefixed {@code subs-}. Used by the parked-subscription scenarios to probe the lost-wake crash window, parked
     * survival under churn + restarts + duplicate signals, timer fidelity across restarts, and live-id start storms.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the subscription-renewal registration.
     */
        public static WorkflowRegistration subscriptionRenewalWorkflow(CountingEffects effects) {
        var workflow = new SubscriptionRenewalWorkflow(effects);
        return new WorkflowRegistration(SubscriptionRenewalWorkflow.WORKFLOW_NAME, SubscriptionStartedEvent.class,
                                        "subs-", workflow::execute);
    }

    /**
     * The combinator <strong>replay-determinism</strong> probe registration: the {@link CombinatorReplayWorkflow}
     * (mode-selected anyMatch/allMatch/noneMatch probes that snapshot what each body run observed from the combinator
     * into per-run effect counters, then park on a gate across the scenario's crash window). Driven by
     * {@code CombinatorReplayRequestedEvent}, ids prefixed {@code creplay-}. Scenario-only (its body-level counters
     * deliberately re-record on every recovered re-run).
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the combinator-replay registration.
     */
        public static WorkflowRegistration combinatorReplayWorkflow(CountingEffects effects) {
        var workflow = new CombinatorReplayWorkflow(effects);
        return new WorkflowRegistration(CombinatorReplayWorkflow.WORKFLOW_NAME, CombinatorReplayRequestedEvent.class,
                                        "creplay-", workflow::execute);
    }

    /**
     * The Phase-4 <strong>reused-names retry-loop</strong> registration: the {@link LoopingPollWorkflow} body that
     * reuses the same step names every iteration (the canonical-example authoring). Driven by
     * {@code LoopReusedPollRequestedEvent}, ids prefixed {@code loopr-}. Used by the live-lock probe.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the reused-names loop registration.
     */
        public static WorkflowRegistration loopingPollReusedNamesWorkflow(CountingEffects effects) {
        var workflow = new LoopingPollWorkflow(effects);
        return new WorkflowRegistration(LoopingPollWorkflow.WORKFLOW_NAME_REUSED, LoopReusedPollRequestedEvent.class,
                                        "loopr-", workflow::executeReusedNames);
    }

    /**
     * The Phase-4 <strong>counter-names retry-loop</strong> registration: the {@link LoopingPollWorkflow} body with
     * per-iteration step names (the documented correct authoring). Driven by {@code LoopCounterPollRequestedEvent},
     * ids prefixed {@code loopc-}. Used as the healthy contrast to the live-lock probe.
     *
     * @param effects counting side-effect registry the workflow records into.
     * @return the counter-names loop registration.
     */
        public static WorkflowRegistration loopingPollCounterNamesWorkflow(CountingEffects effects) {
        var workflow = new LoopingPollWorkflow(effects);
        return new WorkflowRegistration(LoopingPollWorkflow.WORKFLOW_NAME_COUNTER, LoopCounterPollRequestedEvent.class,
                                        "loopc-", workflow::executeCounterNames);
    }

    /**
     * The Phase-3 <strong>v1-only</strong> registration of the {@link RollingDeployWorkflow} (the originally-shipped
     * deployment): the v1 body at {@link RollingDeployWorkflow#VERSION_V1}, driven by {@code RollingDeployOrderEvent},
     * ids prefixed {@code deploy-}. The rolling-deploy scenarios start instances here, then recover under a changed
     * registry ({@link #rollingDeployWorkflowV1V2}, {@link #rollingDeployWorkflowV2Only},
     * {@link #rollingDeployWorkflowV2BadOnly}).
     *
     * @param effects counting side-effect registry (shared across the registry swaps so counts survive deploys).
     * @return the v1-only registration.
     */
        public static WorkflowRegistration rollingDeployWorkflowV1(CountingEffects effects) {
        var workflow = new RollingDeployWorkflow(effects);
        return new WorkflowRegistration(RollingDeployWorkflow.WORKFLOW_NAME, RollingDeployOrderEvent.class, "deploy-",
                                        RollingDeployWorkflow.VERSION_V1, workflow::executeV1);
    }

    /**
     * The Phase-3 <strong>rolling-deploy</strong> registry: v1 retained alongside the correctly-authored v2
     * ({@code migrateVersion}-gated new step) — the recommended deploy. Existing v1 instances route to the v1 body
     * (closest sibling ≤ recorded); fresh starts spawn at v2.
     *
     * @param effects counting side-effect registry (shared across the registry swaps).
     * @return the v1+v2 registrations, to register together in one module.
     */
        public static List<WorkflowRegistration> rollingDeployWorkflowV1V2(CountingEffects effects) {
        var workflow = new RollingDeployWorkflow(effects);
        return List.of(
                new WorkflowRegistration(RollingDeployWorkflow.WORKFLOW_NAME, RollingDeployOrderEvent.class, "deploy-",
                                         RollingDeployWorkflow.VERSION_V1, workflow::executeV1),
                new WorkflowRegistration(RollingDeployWorkflow.WORKFLOW_NAME, RollingDeployOrderEvent.class, "deploy-",
                                         RollingDeployWorkflow.VERSION_V2, workflow::executeV2));
    }

    /**
     * The Phase-3 <strong>premature-removal</strong> registry: ONLY the correctly-authored v2 — the operator removed
     * v1 while v1-recorded instances were still live. Recovered v1 instances route to v2 via the
     * closest-higher-sibling pass and fork mid-flight through the {@code migrateVersion} gate.
     *
     * @param effects counting side-effect registry (shared across the registry swaps).
     * @return the v2-only registration.
     */
        public static WorkflowRegistration rollingDeployWorkflowV2Only(CountingEffects effects) {
        var workflow = new RollingDeployWorkflow(effects);
        return new WorkflowRegistration(RollingDeployWorkflow.WORKFLOW_NAME, RollingDeployOrderEvent.class, "deploy-",
                                        RollingDeployWorkflow.VERSION_V2, workflow::executeV2);
    }

    /**
     * The Phase-3 <strong>bad-deploy</strong> registry: ONLY the mis-authored v2 (the structural change WITHOUT
     * {@code migrateVersion}). Used to characterize the drift guard's parked-instance blind spot and the poisoned
     * rollback it produces.
     *
     * @param effects counting side-effect registry (shared across the registry swaps).
     * @return the bad-v2-only registration.
     */
        public static WorkflowRegistration rollingDeployWorkflowV2BadOnly(CountingEffects effects) {
        var workflow = new RollingDeployWorkflow(effects);
        return new WorkflowRegistration(RollingDeployWorkflow.WORKFLOW_NAME, RollingDeployOrderEvent.class, "deploy-",
                                        RollingDeployWorkflow.VERSION_V2, workflow::executeV2Bad);
    }

    /**
     * The INV-18 <strong>v1</strong> ("old code") registration: the {@link DriftWorkflow#executeV1} body (records
     * {@code reserveInventory} then {@code chargePayment}, then suspends on a never-arriving wait, leaving
     * {@code chargePayment} durably COMPLETED while the instance stays LIVE), driven by {@code DriftRequestedEvent}, ids
     * prefixed {@code drift-}, pinned to {@link DriftWorkflow#DRIFT_VERSION}. Used by the {@code DriftGuardPausesCleanly}
     * scenario to record an instance to the state a divergent v2 body drifts against.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the drift v1 registration.
     */
        public static WorkflowRegistration driftWorkflowV1(CountingEffects effects) {
        var workflow = new DriftWorkflow(effects);
        return new WorkflowRegistration(DriftWorkflow.WORKFLOW_NAME, DriftRequestedEvent.class, "drift-",
                                        DriftWorkflow.DRIFT_VERSION, workflow::executeV1);
    }

    /**
     * The INV-18 <strong>v2</strong> ("new code", deployed WITHOUT {@code ctx.migrateVersion}) registration: the
     * {@link DriftWorkflow#executeV2} body (replays {@code reserveInventory} cached, then reaches the NEW
     * {@code repackage} step, SKIPPING the recorded-terminal {@code chargePayment}). It shares the same
     * {@code workflowName}, start event, id prefix and {@link DriftWorkflow#DRIFT_VERSION} as {@link #driftWorkflowV1}, so
     * a recovered engine built with this registration re-routes the SAME v1-recorded instance to the divergent body —
     * tripping {@code guardAgainstReplayDrift} on {@code repackage}. Used by the {@code DriftGuardPausesCleanly} scenario
     * via {@link SimulationWorld#crashAndRecoverWith(List)} to replay the recorded instance under the divergent
     * definition.
     *
     * @param effects counting side-effect registry the workflow records into (survives crashes).
     * @return the drift v2 (divergent) registration.
     */
        public static WorkflowRegistration driftWorkflowV2(CountingEffects effects) {
        var workflow = new DriftWorkflow(effects);
        return new WorkflowRegistration(DriftWorkflow.WORKFLOW_NAME, DriftRequestedEvent.class, "drift-",
                                        DriftWorkflow.DRIFT_VERSION, workflow::executeV2);
    }

    /**
     * Publishes an external (non-workflow) event into the engine, the way real producers do — the engine routes it to
     * the start handler and to any instance waiting for it.
     *
     * @param event the event payload object (e.g. an {@code OrderPlacedEvent} or {@code PaymentConfirmedEvent}).
     */
    public void publish(Object event) {
        var eventMessage = new GenericEventMessage(messageTypeResolver.resolveOrThrow(event), event)
                .withConverter(eventConverter);
        eventSink.publish(null, eventMessage);
    }

    /**
     * Re-publishes an already-constructed {@link EventMessage} (used by the message-duplication fault to redeliver a
     * previously delivered external event verbatim).
     *
     * @param eventMessage the event message to deliver again.
     */
    public void publishRaw(EventMessage eventMessage) {
        eventSink.publish(null, eventMessage);
    }

    /**
     * Returns the workflow ids the engine currently holds as live (non-terminal) executions.
     *
     * @return sorted live workflow ids.
     */
        public List<String> liveWorkflowIds() {
        return workflowEngine.workflowExecutions().stream()
                             .map(WorkflowExecution::workflowId)
                             .sorted()
                             .toList();
    }

    /**
     * Cancels a running step of the LIVE execution with the given {@code workflowId} from the CALLER's thread — by
     * construction a thread other than the workflow's own control thread, which is exactly the external
     * ({@code cancelRunningStep} off the control thread) surface the external step-cancellation scenario drives. A
     * no-op when the execution is not currently held or the step has no running future (idempotent, so a scenario can
     * retry the call around the wait step's registration window).
     *
     * @param workflowId the workflow id whose live execution's step to cancel.
     * @param stepName   the running step to cancel.
     * @param cause      the cancellation cause the step future is completed with.
     */
    public void cancelRunningStepOf(String workflowId, String stepName, Throwable cause) {
        workflowEngine.workflowExecutions().stream()
                      .filter(e -> workflowId.equals(e.workflowId()))
                      .findFirst()
                      .ifPresent(e -> ((SimpleWorkflowExecution) e).workflowCancellation()
                                                               .requestStepCancellation(stepName, cause));
    }

    /**
     * Returns the LIVE execution the engine holds for {@code workflowId}, if it still holds one.
     * <p>
     * A scenario needs this to see what the engine's own projected state says while the durable log already says
     * something else — the window between a terminal event committing and that event being delivered back to the
     * execution that wrote it — and to interrupt the driver inside it.
     *
     * @param workflowId the instance whose live execution to look up.
     * @return the live execution, or empty when the engine no longer holds one.
     */
        public java.util.Optional<WorkflowExecution> liveExecution(String workflowId) {
        return workflowEngine.workflowExecutions().stream()
                             .filter(execution -> workflowId.equals(execution.workflowId()))
                             .findFirst();
    }

    /**
     * Returns this instance's history read-model.
     *
     * @return the shared, durable history repository.
     */
        public MutableWorkflowHistoryRepository historyRepository() {
        return historyRepository;
    }

    /**
     * Returns the engine's own reconstructed final payload per {@code workflowId}, for the instances whose id starts
     * with {@code idPrefix}. The payload comes from the workflow-history read-model's {@code state().payload()} — the
     * {@code WorkflowHistoryProjector} maintains it by consuming the event stream, an evolution path independent of any
     * raw committed-log fold, which is exactly what INV-13 ({@code NoLostPayloadWrites}) cross-checks against the log.
     *
     * @param idPrefix the workflow-id prefix to include (e.g. {@code payload-}).
     * @return the reconstructed payload per matching {@code workflowId} (empty if none have been projected yet).
     */
        public java.util.Map<String, java.util.Map<String, Object>> reconstructedPayloads(String idPrefix) {
        var payloads = new java.util.LinkedHashMap<String, java.util.Map<String, Object>>();
        for (var history : historyRepository.findAll()) {
            if (history.workflowId().startsWith(idPrefix)) {
                payloads.put(history.workflowId(), history.state().payload());
            }
        }
        return payloads;
    }

    /**
     * Resolves the {@link MessageType} the engine would assign to a payload type, for constructing raw event messages.
     *
     * @param payload the payload object.
     * @return its resolved message type.
     */
        public MessageType messageTypeOf(Object payload) {
        return messageTypeResolver.resolveOrThrow(payload);
    }

    /**
     * Crashes this instance: shuts down the engine and its configuration, dropping all volatile state (live
     * executions, the per-process processor token, in-flight timers). The durable substrate is untouched.
     */
    public void stop() {
        workflowEngine.shutdown();
        configuration.shutdown();
    }

    @Override
    public void close() {
        stop();
    }
}
