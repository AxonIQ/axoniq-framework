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
import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.SimpleWorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateProjector;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.configuration.WorkflowEnhancer.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;

/**
 * Workflow instance implementation.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 1.0.0
 */
public final class SimpleWorkflowExecution implements WorkflowExecution, WorkflowContext {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWorkflowExecution.class);

    // Attributes
    private final String workflowId;
    private final WorkflowConfiguration<?> workflowConfiguration;
    private final String workflowName;

    // primitive implementations
    private final ExecuteDelegate executeDelegate;
    private final WaitForDelegate waitForDelegate;
    private final TerminateDelegate terminateDelegate;

    // State variables
    private final WorkflowState workflowState;

    // Runtime
    private final WorkflowContext workflowContext;
    private final BlockingQueue<Consumer<WorkflowExecution>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME size
    private final EventWaitConditions eventWaitConditions = new EventWaitConditions();
    private final RunningSteps runningSteps = new RunningSteps();
    private final ProcessingContext processingContext;

    private boolean executable = false;
    private Map<String, Object> payload;

    // Services
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Clock clock;
    private final Executor executor;
    private final EventSink eventSink;


    /**
     * Constructs a new instance.
     *
     * @param workflowId            workflow id.
     * @param initial               initial payload of workflow instance.
     * @param processingContext     processing context.
     * @param workflowConfiguration workflow configuration.
     * @param workflowContext       outer workflow context.
     */
    public SimpleWorkflowExecution(@Nonnull String workflowId,
                                   @Nonnull Map<String, Object> initial,
                                   @Nonnull ProcessingContext processingContext,
                                   @Nonnull WorkflowConfiguration<?> workflowConfiguration,
                                   @Nullable WorkflowContext workflowContext
    ) {
        this.workflowId = Objects.requireNonNull(workflowId, "Workflow id must not be null");
        this.payload = Objects.requireNonNull(initial, "Payload must not be null");
        this.processingContext = Objects.requireNonNull(processingContext, "Processing context is mandatory");
        this.workflowContext = workflowContext != null ? workflowContext : this;
        this.unitOfWorkFactory = Objects.requireNonNull(processingContext.component(UnitOfWorkFactory.class),
                                                        "Could not retrieve UoW factory");
        this.clock = Objects.requireNonNull(processingContext.component(Clock.class), "Could not retrieve Clock");
        this.executor = Objects.requireNonNull(processingContext.component(Executor.class, WORKFLOW_ENGINE_EXECUTOR),
                                               "Could not retrieve EventSink");
        this.eventSink = Objects.requireNonNull(processingContext.component(EventSink.class),
                                                "Could not retrieve EventSink");
        this.workflowConfiguration = Objects.requireNonNull(workflowConfiguration,
                                                            "Workflow configuration must not be null");
        var stepParent = workflowConfiguration.eventNameCustomizer().forStepInheritance();
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

        var configuredName = Objects.requireNonNull(workflowConfiguration.workflowName(),
                                                    "Workflow name must not be null");
        this.workflowName = configuredName.isEmpty() ? workflowId : configuredName; // FIXME
        this.workflowState = new SimpleWorkflowState(this, workflowConfiguration.workflowStatusChangeListeners());
    }


    @Override
    @Nonnull
    public <T extends WorkflowContext> T execute() {
        // TODO: discuss when we switch to the executable
        switchToExecutable();
        //noinspection unchecked
        return (T) ProcessingContextUtils
                .executeWithResult(
                        workflowId,
                        unitOfWorkFactory,
                        executor,
                        this.processingContext(),
                        pc -> {

                            if (this.workflowStatus().isTerminal()) {
                                logger.trace("Workflow instance has reached terminal state {}, skipping execution.",
                                             this.workflowStatus());
                                return CompletableFuture.completedFuture(this);
                            }

                            if (this.workflowStatus() == WorkflowStatus.NONE) {
                                sendWorkflowEvent(startedWorkflow(this,
                                                                  workflowName,
                                                                  this.workflowConfiguration.eventNameCustomizer()),
                                                  pc).join(); // FIXME join without timeout?
                            }

                            try {
                                logger.trace("Executing workflow with initial payload {} from thread {}",
                                             this.workflowPayload(),
                                             Thread.currentThread());

                                workflowConfiguration.workflowDefinition().accept(this.workflowContext());
                                logger.trace("Workflow executed. Resulting workflow payload {}.",
                                             this.workflowPayload());

                                if (!this.workflowStatus().isTerminal()) {
                                    sendWorkflowEvent(completedWorkflow(this,
                                                                        workflowName,
                                                                        this.workflowConfiguration.eventNameCustomizer()),
                                                      pc).get(
                                            5,
                                            TimeUnit.SECONDS); // FIXME constant?
                                }
                            } catch (WorkflowFailedException | WorkflowCancelledException e) {
                                // Events already sent by TerminateDelegate, just let it propagate
                            } catch (Exception e) {
                                if (e instanceof TimeoutException) {
                                    sendWorkflowEvent(timeoutWorkflow(this,
                                                                      workflowName,
                                                                      clock.instant(),
                                                                      workflowConfiguration.eventNameCustomizer()),
                                                      processingContext()).join(); // FIXME join without timeout
                                } else if (e instanceof InterruptedException) {
                                    sendWorkflowEvent(cancelledWorkflow(this,
                                                                        workflowName,
                                                                        workflowConfiguration.eventNameCustomizer()),
                                                      pc).join(); // FIXME join without timeout
                                } else {
                                    logger.error("Error occurred in workflow {}", workflowId, e);
                                }
                            }

                            return CompletableFuture.completedFuture(this);
                        }
                ).thenApply(wc -> {
                    try {
                        // FIXME -> tell the coordinator to clean up and wait for terminal workflow status.
                        awaitStateChange(s -> s.state().workflowStatus().isTerminal());
                    } catch (Exception te) {
                        logger.error("Error waiting for workflow instance termination", te);
                    }
                    return wc;
                })
                .join();
    }

    @Override
    @Nonnull
    public WorkflowStatus workflowStatus() {
        return state().workflowStatus();
    }

    @Override
    public void applyPayloadModification(
            @Nonnull PayloadModification payloadModification
    ) {
        this.payload = Objects.requireNonNull(payloadModification.apply(payload), "Payload must not be null");
    }

    @Override
    public void awaitStateChange(
            @Nonnull Predicate<WorkflowExecution> predicate
    ) throws InterruptedException {
        do {
            taskQueue.take().accept(this);
        } while (!predicate.test(this));
    }

    @Override
    public void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
        eventWaitConditions.evaluateAndApply(eventMessage, waitForDelegate::eventReceived);
        appendTask(i -> WorkflowStateProjector.applyStateChange(eventMessage, processingContext, state()));
    }


    // delegation
    @Override
    @Nonnull
    public WorkflowStepResult execute(@Nonnull String stepName, @Nullable Map<String, Object> local,
                                      @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterMapping,
                                      @Nonnull PayloadReducer resultMapping, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        state().guardTerminalState();
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
    public WorkflowStepResult waitFor(@Nonnull String stepName,
                                      @Nonnull EventCondition eventCondition,
                                      @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        state().guardTerminalState();
        return waitForDelegate.waitFor(stepName, eventCondition, timeout, eventNameCustomizer);
    }

    @Override
    public void terminate(@Nonnull TerminateCommand command) {
        if (command.isStepCancellation()) {
            terminateDelegate.terminate(command);
            return;
        }
        state().guardTerminalState();
        terminateDelegate.terminate(new TerminateCommand(
                command.error(),
                command.cause(),
                merge(workflowConfiguration.eventNameCustomizer(), command.eventNameCustomizer()),
                workflowName,
                null
        ));
    }

    @Override
    public void registerRunningStep(@Nonnull String stepName, @Nonnull CompletableFuture<?> future) {
        runningSteps.register(stepName, future);
    }

    @Override
    public void removeRunningStep(@Nonnull String stepName) {
        runningSteps.remove(stepName);
    }

    @Override
    public boolean cancelRunningStep(@Nonnull String stepName, @Nullable Throwable cause) {
        boolean cancelled = runningSteps.cancelWithCause(stepName, cause);
        if (cancelled) {
            try {
                awaitStateChange(s -> s.state().containsStep(stepName)
                        && s.state().getStep(stepName).status().isTerminal());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return cancelled;
    }

    @Override
    public void cancelAllRunningSteps(@Nullable Throwable cause) {
        runningSteps.cancelAll(cause, cancelledSteps -> {
            if (cancelledSteps.isEmpty()) {
                return;
            }
            try {
                awaitStateChange(s -> cancelledSteps
                        .stream()
                        .allMatch(stepName -> s.state().containsStep(stepName)
                                && s.state().getStep(stepName).status().isTerminal()
                        )
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Override
    public void cancelAndRemoveRunningStep(@Nonnull String stepName, boolean mayInterruptIfRunning) {
        runningSteps.cancelAndRemove(stepName, mayInterruptIfRunning);
    }

    @Override
    @Nullable
    public Consumer<WorkflowExecution> getNextTask() {
        return this.taskQueue.poll(); // FIXME: forever?
    }

    @Override
    public void appendTask(@Nonnull Consumer<WorkflowExecution> task) {
        if (!this.taskQueue.offer(task)) {
            // whoops, we're overloading this workflow with events. STOP!!!
            throw new RuntimeException("Too many events for this workflow instance"); // FIXME <- task queue is full, backpressure?
        }
    }

    @Override
    public void registerWaitCondition(@Nonnull String stepName,
                                      @Nonnull EventCondition eventCondition,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        eventWaitConditions.add(stepName, eventCondition, eventNameCustomizer);
    }

    @Override
    public void removeWaitCondition(@Nonnull String stepName) {
        eventWaitConditions.remove(stepName);
    }


    @Override
    @Nonnull
    public String workflowId() {
        return this.workflowId;
    }

    @Override
    @Nonnull
    public Map<String, Object> workflowPayload() {
        return this.payload;
    }

    @Override
    @Nonnull
    public List<String> workflowStepNames() {
        return state().workflowStepNames();
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
    @Nonnull
    public WorkflowState state() {
        return workflowState;
    }

    @Override
    @Nonnull
    public <T extends WorkflowContext> T workflowContext() {
        //noinspection unchecked
        return (T) this.workflowContext;
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("workflowId", workflowId);
        descriptor.describeProperty("workflowName", workflowName);
        descriptor.describeProperty("executable", executable);
        descriptor.describeProperty("state", state());
        eventWaitConditions.describeTo(descriptor);
        runningSteps.describeTo(descriptor);
    }
}
