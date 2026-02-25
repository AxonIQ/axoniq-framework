/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.PayloadModification;
import io.axoniq.workflow.runtime.api.PayloadProcessor;
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import io.axoniq.workflow.runtime.engine.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.configuration.WorkflowEnhancer.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;
import static io.axoniq.workflow.runtime.engine.util.MetadataUtils.getStepName;

/**
 * Workflow instance implementation.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 1.0.0
 */
public class WorkflowInstance implements WorkflowState, WorkflowContext {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowInstance.class);

    // primitive implementations
    private final ExecuteDelegate executeDelegate;
    private final WaitForDelegate waitForDelegate;
    private final TerminateDelegate terminateDelegate;

    private final BlockingQueue<Consumer<WorkflowState>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME size
    // State variables
    private final Map<String, WorkflowStep> steps = new ConcurrentHashMap<>();
    private final EventWaitConditions eventWaitConditions = new EventWaitConditions();
    private final RunningSteps runningFutures = new RunningSteps();
    private final ProcessingContext processingContext;
    private final String workflowId;
    private WorkflowStatus status = WorkflowStatus.NONE;
    private boolean executable = false;
    private Map<String, Object> payload;

    // Services
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Clock clock;
    private final Executor executor;
    private final EventSink eventSink;
    private volatile EventNameCustomizer configurationCustomizer;
    private volatile String resolvedWorkflowName;

    /**
     * Constructs new instance.
     *
     * @param workflowId        workflow id.
     * @param initial           initial payload of workflow instance.
     * @param processingContext processing context.
     * @param parentCustomizer  event name customizer.
     */
    public WorkflowInstance(@Nonnull String workflowId,
                            @Nonnull Map<String, Object> initial,
                            @Nonnull ProcessingContext processingContext,
                            @Nonnull EventNameCustomizer parentCustomizer) {
        this.workflowId = Objects.requireNonNull(workflowId, "Workflow id must not be null");
        this.payload = Objects.requireNonNull(initial, "Payload must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "Processing context is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(processingContext.component(UnitOfWorkFactory.class),
                                                        "Could not retrieve UoW factory");
        this.clock = Objects.requireNonNull(processingContext.component(Clock.class), "Could not retrieve Clock");
        this.executor = Objects.requireNonNull(processingContext.component(Executor.class, WORKFLOW_ENGINE_EXECUTOR),
                                               "Could not retrieve EventSink");
        this.eventSink = Objects.requireNonNull(processingContext.component(EventSink.class),
                                                "Could not retrieve EventSink");
        var stepParent = parentCustomizer.forStepInheritance();
        this.executeDelegate = new ExecuteDelegate(this,
                                                   this,
                                                   stepParent,
                                                   clock,
                                                   unitOfWorkFactory,
                                                   eventSink,
                                                   executor);
        this.waitForDelegate = new WaitForDelegate(this,
                                                   this,
                                                   stepParent,
                                                   clock,
                                                   unitOfWorkFactory,
                                                   eventSink,
                                                   executor);
        this.terminateDelegate = new TerminateDelegate(this,
                                                       this,
                                                       eventSink,
                                                       workflowId,
                                                       unitOfWorkFactory,
                                                       executor);
    }


    @Override
    @Nonnull
    public <T extends WorkflowContext> T execute(
            @Nonnull WorkflowConfiguration<T> configuration,
            @Nonnull WorkflowContext workflowContext
    ) {
        // TODO: discuss when we switch to the executable
        switchToExecutable();

        return ProcessingContextUtils
                .executeWithResult(
                        workflowId,
                        unitOfWorkFactory,
                        executor,
                        workflowContext.processingContext(),
                        pc -> {

                            @SuppressWarnings("unchecked")
                            var ctx = (T) workflowContext;
                            if (ctx.getStatus().isTerminal()) {
                                logger.trace("Workflow instance has reached terminal state {}, skipping execution.",
                                             ctx.getStatus());
                                return CompletableFuture.completedFuture(ctx);
                            }
                            var configuredName = Objects.requireNonNull(configuration.workflowName(),
                                                                        "Workflow name must not be null");
                            var workflowName = configuredName.isEmpty() ? workflowId : configuredName;
                            var customizer = configuration.eventNameCustomizer();
                            this.configurationCustomizer = customizer;
                            this.resolvedWorkflowName = workflowName;
                            if (ctx.getStatus() == WorkflowStatus.NONE) {
                                sendWorkflowEvent(startedWorkflow(workflowContext, workflowName, customizer),
                                                  pc).join(); // FIXME join without timeout?
                            }

                            try {
                                logger.trace("Executing workflow with initial payload {} from thread {}",
                                             workflowContext.getPayload(),
                                             Thread.currentThread());
                                configuration.workflowDefinition().accept(ctx);
                                logger.trace("Workflow executed. Resulting workflow payload {}.",
                                             workflowContext.getPayload());

                                sendWorkflowEvent(completedWorkflow(workflowContext, workflowName, customizer), pc).get(
                                        5,
                                        TimeUnit.SECONDS); // FIXME constant?
                            } catch (WorkflowFailedException | WorkflowCancelledException e) {
                                // Events already sent by TerminateDelegate, just let it propagate
                            } catch (Exception e) {
                                if (e instanceof TimeoutException) {
                                    sendWorkflowEvent(timeoutWorkflow(workflowContext,
                                                                      workflowName,
                                                                      clock.instant(),
                                                                      customizer),
                                                      processingContext()).join(); // FIXME join without timeout
                                } else if (e instanceof InterruptedException) {
                                    sendWorkflowEvent(cancelledWorkflow(workflowContext, workflowName, customizer),
                                                      pc).join(); // FIXME join without timeout
                                } else {
                                    logger.error("Error occurred in workflow {}", workflowId, e);
                                }
                            }

                            return CompletableFuture.completedFuture(ctx);
                        }
                ).thenApply(wc -> {
                    try {
                        // FIXME -> tell the coordinator to clean up and wait for terminal workflow status.
                        awaitStateChange(s -> s.getStatus().isTerminal());
                    } catch (Exception te) {
                        logger.error("Error waiting for workflow instance termination", te);
                    }
                    return wc;
                })
                .join();
    }

    @Override
    public void applyStateChange(
            @Nonnull EventMessage eventMessage,
            @Nonnull ProcessingContext processingContext
    ) {
        logger.trace("Applying event {}", eventMessage.type());
        Object eventPayload = eventMessage.payloadAs(Object.class);
        var metadata = eventMessage.metadata();
        // Apply step-level state changes
        MetadataUtils.getStepStatus(metadata).ifPresent(stepStatus -> {
            var stepName = getStepName(metadata);
            switch (stepStatus) {
                case STARTED:
                    addStep(WorkflowStep.started(stepName,
                                                 eventPayload,
                                                 eventMessage.timestamp(),
                                                 processingContext)); // TODO copy resources of the context
                    break;
                case FAILED:
                    addStep(WorkflowStep.failed(stepName,
                                                (Throwable) eventPayload,
                                                eventMessage.timestamp(),
                                                processingContext)); // TODO copy resources of the context
                    break;
                case TIMED_OUT:
                    addStep(WorkflowStep.timedOut(stepName,
                                                  eventPayload,
                                                  eventMessage.timestamp(),
                                                  processingContext)); // TODO copy resources of the context
                    break;
                case COMPLETED:
                    addStep(WorkflowStep.completed(stepName,
                                                   eventPayload,
                                                   eventMessage.timestamp(),
                                                   processingContext)); // TODO copy resources of the context
                    break;
                case CANCELLED:
                    addStep(WorkflowStep.cancelled(stepName,
                                                   eventMessage.timestamp(),
                                                   processingContext)); // TODO copy resources of the context
                    break;
                default:
                    break;
            }
        });
        // Apply workflow-level state changes
        MetadataUtils.getWorkflowStatus(metadata).ifPresent(status -> this.status = status
        );
    }

    @Override
    public void applyPayloadModification(
            @Nonnull PayloadModification payloadModification
    ) {
        this.payload = Objects.requireNonNull(
                payloadModification.apply(payload),
                "Payload must not be null"
        );
    }

    @Override
    public void awaitStateChange(
            @Nonnull Predicate<WorkflowState> predicate
    ) throws InterruptedException {
        do {
            taskQueue.take().accept(this);
        } while (!predicate.test(this));
    }

    @Override
    public void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
        eventWaitConditions.evaluateAndApply(eventMessage, waitForDelegate::eventReceived);
        appendTask(i -> i.applyStateChange(eventMessage, processingContext));
    }


    // delegation
    @Override
    @Nonnull
    public WorkflowStepResult execute(@Nonnull String stepName, @Nullable Map<String, Object> local,
                                      @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterMapping,
                                      @Nonnull PayloadReducer resultMapping, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        return executeDelegate.execute(stepName,
                                       local,
                                       action,
                                       parameterMapping,
                                       resultMapping,
                                       timeout,
                                       eventNameCustomizer);
    }

    @Override
    @Nonnull
    public WorkflowStepResult waitFor(@Nonnull String stepName, @Nonnull QualifiedName qualifiedName,
                                      @Nonnull Predicate<EventMessage> predicate, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        return waitForDelegate.waitFor(stepName, qualifiedName, predicate, timeout, eventNameCustomizer);
    }

    @Override
    public void terminate(boolean error, @Nullable Throwable cause,
                          @Nonnull EventNameCustomizer eventNameCustomizer) {
        var name = resolvedWorkflowName != null ? resolvedWorkflowName : workflowId;
        var parent = configurationCustomizer != null ? configurationCustomizer : eventNameCustomizer;
        terminateDelegate.terminate(error, cause, merge(parent, eventNameCustomizer), name);
    }

    @Override
    public void registerRunningFuture(@Nonnull String stepName, @Nonnull CompletableFuture<?> future) {
        runningFutures.register(stepName, future);
    }

    @Override
    public void removeRunningFuture(@Nonnull String stepName) {
        runningFutures.remove(stepName);
    }

    @Override
    public void cancelAllRunningSteps(@Nullable Throwable cause) {
        runningFutures.cancelAll(cause, cancelledSteps -> {
            if (cancelledSteps.isEmpty()) {
                return;
            }
            try {
                awaitStateChange(s -> cancelledSteps.stream()
                        .allMatch(stepName -> s.containsStep(stepName)
                                 && s.getStep(stepName).status().isTerminal()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Override
    public void cancelAndRemoveRunningFuture(@Nonnull String stepName, boolean mayInterruptIfRunning) {
        runningFutures.cancelAndRemove(stepName, mayInterruptIfRunning);
    }

    @Override
    @Nullable
    public Consumer<WorkflowState> getNextTask() {
        return this.taskQueue.poll(); // FIXME: forever?
    }

    @Override
    public void appendTask(@Nonnull Consumer<WorkflowState> task) {
        if (!this.taskQueue.offer(task)) {
            // whoops, we're overloading this workflow with events. STOP!!!
            throw new RuntimeException("Too many events for this workflow instance"); // FIXME <- task queue is full, backpressure?
        }
    }

    @Override
    @Nonnull
    public WorkflowStep getStep(@Nonnull String stepName) {
        return steps.get(stepName);
    }

    @Override
    public boolean containsStep(@Nonnull String stepName) {
        return steps.containsKey(stepName);
    }

    @Override
    public void addStep(@Nonnull WorkflowStep workflowStep) {
        this.steps.put(workflowStep.stepName(), workflowStep);
    }

    @Override
    public void registerWaitCondition(@Nonnull String stepName, @Nonnull QualifiedName qualifiedName,
                                      @Nonnull Predicate<EventMessage> predicate,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        eventWaitConditions.add(stepName, new EventCondition(qualifiedName, predicate), eventNameCustomizer);
    }

    @Override
    public void removeWaitCondition(@Nonnull String stepName) {
        eventWaitConditions.remove(stepName);
    }


    @Override
    @Nonnull
    public String getWorkflowId() {
        return this.workflowId;
    }

    @Override
    @Nonnull
    public Map<String, Object> getPayload() {
        return this.payload;
    }

    @Override
    @Nonnull
    public WorkflowStatus getStatus() {
        return this.status;
    }

    @Override
    @Nonnull
    public List<String> getStepHistory() {
        return new ArrayList<>(steps.keySet());
    }

    @Override
    public boolean hasTasks() {
        return this.taskQueue.isEmpty();
    }

    @Override
    public boolean isExecutable() {
        return executable;
    }

    public void switchToExecutable() {
        executable = true;
    }

    private CompletableFuture<Void> sendWorkflowEvent(EventMessage eventMessage, ProcessingContext processingContext) {
        // TODO: make sure the consistency marker is used
        return eventSink.publish(processingContext, eventMessage);
    }

    @Override
    @Nonnull
    public ProcessingContext processingContext() {
        return processingContext;
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        eventWaitConditions.describeTo(descriptor);
        runningFutures.describeTo(descriptor);
    }
}
