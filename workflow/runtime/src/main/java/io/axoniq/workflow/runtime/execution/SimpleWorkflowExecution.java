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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import io.axoniq.workflow.runtime.util.Version;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.*;
import static io.axoniq.workflow.runtime.util.ProcessingContextUtils.resolveRestartToken;
import static java.lang.Thread.currentThread;

/**
 * Workflow instance implementation.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 1.0.0
 */
public final class SimpleWorkflowExecution implements WorkflowExecution, WorkflowCancellationProvider {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWorkflowExecution.class);

    // State variables
    private final WorkflowState workflowState;
    // Attributes
    private final String workflowId;
    private final String workflowName;
    @Nullable
    private final TrackingToken restartToken;
    private final WorkflowConfiguration<?> workflowConfiguration;

    // Execution
    private final WorkflowContextDelegation contextDelegate;

    // Runtime
    private final BlockingQueue<Consumer<WorkflowExecution>> taskQueue = new ArrayBlockingQueue<>(1000); // FIXME size
    private final EventWaitConditions eventWaitConditions = new EventWaitConditions();
    private final RunningSteps runningSteps = new RunningSteps();
    private final WorkflowStepProgress workflowStepProgress = new WorkflowStepProgress();
    private final WorkflowCancellation workflowCancellation;

    private boolean executable = false;

    /**
     * Constructs a new instance.
     *
     * @param workflowId            workflow id.
     * @param initial               initial payload of workflow instance.
     * @param processingContext     processing context.
     * @param workflowConfiguration workflow configuration.
     * @param workflowContext       workflow context created by the factory.
     */
    public SimpleWorkflowExecution(@Nonnull String workflowId,
                                   @Nonnull Map<String, Object> initial,
                                   @Nonnull ProcessingContext processingContext,
                                   @Nonnull WorkflowConfiguration<?> workflowConfiguration,
                                   @Nonnull WorkflowContext workflowContext
    ) {
        this.workflowId = Objects.requireNonNull(workflowId, "Workflow id must not be null");
        this.workflowConfiguration = Objects.requireNonNull(workflowConfiguration,
                                                            "Workflow configuration must not be null");
        var configuredName = Objects.requireNonNull(workflowConfiguration.workflowName(),
                                                    "Workflow name must not be null");
        this.workflowName = configuredName.isEmpty() ? workflowId : configuredName; // FIXME
        this.restartToken = resolveRestartToken(processingContext);

        this.contextDelegate = new WorkflowContextDelegation(
                workflowConfiguration,
                workflowContext,
                this,
                runningSteps,
                eventWaitConditions,
                workflowStepProgress,
                this::beginTerminalTeardown,
                processingContext
        );
        this.workflowCancellation = new DefaultWorkflowCancellation(this, contextDelegate, runningSteps);
        this.workflowState = new EventSourcedWorkflowState(
                initial,
                workflowConfiguration.workflowVersion(),
                this.contextDelegate.typedWorkflowContext(),
                workflowConfiguration.workflowStatusChangeListeners()
        );
    }


    /**
     * {@inheritDoc}
     * <p>
     * The body runs inside a unit of work spanning the instance's entire lifetime. It is created from the
     * non-transactional {@link WorkflowContextDelegation#workflowBodyUnitOfWorkFactory()} so that no transactional
     * resources are held while the instance is parked.
     */
    @Override
    public void execute(@Nonnull Consumer<WorkflowExecution> terminationHandler) {
        this.executable = true;
        // run in a separate thread to avoid blocking the replay status change handler thread ( = WorkPackage)

        ProcessingContextUtils
                .executeWithResultInSeparateThread(
                        contextDelegate.workflowId(),
                        contextDelegate.workflowBodyUnitOfWorkFactory(),
                        contextDelegate.executorService(),
                        this.processingContext(),
                        ctx -> {

                            if (this.state().workflowStatus().isTerminal()) {
                                logger.trace(
                                        "Workflow instance has reached terminal state {}, skipping execution.",
                                        this.state().workflowStatus()
                                );
                                return CompletableFuture.completedFuture(this.contextDelegate);
                            }
                            logger.trace("Thread: {}, ProcessingContext {}", currentThread(), ctx);

                            publishStartWorkflow(ctx);

                            try {
                                executeWorkflow(ctx);
                            } catch (Throwable e) {
                                handleWorkflowException(ctx, e);
                            }
                            finishWorkflow(terminationHandler);
                            return CompletableFuture.completedFuture(null);
                        }
                );
    }


    /**
     * Emit start events if not already started.
     *
     * @param ctx processing context.
     */
    private void publishStartWorkflow(@Nonnull ProcessingContext ctx) {
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        if (this.state().workflowStatus() == WorkflowStatus.NONE) {
            // FIXME join without timeout?
            sendWorkflowEvent(startedWorkflow(this.workflowContext(), workflowName, eventNameCustomizer), ctx).join();
            try {
                awaitStateChange(s -> s.workflowStatus() == WorkflowStatus.STARTED);
            } catch (Exception e) {
                logger.error("Error waiting for start of workflow instance {}", workflowId, e);
            }
        }
    }

    /**
     * Executes the workflow.
     *
     * @param ctx processing context.
     * @throws Exception if something went wrong.
     */
    private void executeWorkflow(@Nonnull ProcessingContext ctx) throws Exception {
        var workflowPayload = this.workflowContext().workflowPayload();
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        logger.info("Executing workflow with initial payload {} from thread {}",
                    workflowPayload,
                    currentThread());

        // Reset the runtime "book" — step-reference tracker for the drift guard.
        this.workflowStepProgress.clear();

        // Dispatch to the definition matching state.workflowDefinitionVersion().
        var definition = WorkflowConfigurationRegistry.resolveOrFallback(
                ctx, workflowName, workflowId, this.state().workflowDefinitionVersion(), this.workflowConfiguration
        ).workflowDefinition();
        definition.accept(this.contextDelegate.typedWorkflowContext());

        if (!this.state().workflowStatus().isTerminal()) {
            // Whole-workflow terminal: publish only the workflow-level terminal event. Any async steps
            // still running are interrupted (no per-step terminal event) and the task queue is discarded before the
            // terminal event is published, so a queued retry-failure/launch task can never run. Running steps are
            // left in their last recorded (STARTED) state — single-step cancel is the way to get a step terminal.
            beginTerminalTeardown();

            sendWorkflowEvent(
                    completedWorkflow(this.workflowContext(),
                                      workflowName,
                                      eventNameCustomizer),
                    ctx).get(5, TimeUnit.SECONDS); // FIXME constant?
            try {
                awaitStateChange(s -> s.workflowStatus().isTerminal());
            } catch (Exception e) {
                logger.error("Error waiting for completion of workflow instance {}", workflowId, e);
            }
        }
        logger.info("Workflow executed. Resulting workflow payload {}.", this.workflowContext().workflowPayload());
    }

    /**
     * Handles exceptions thrown during workflow execution.
     *
     * @param ctx       processing context.
     * @param exception exception to handle.
     */
    private void handleWorkflowException(@Nonnull ProcessingContext ctx, @Nonnull Throwable exception) {
        var eventNameCustomizer = this.workflowConfiguration.eventNameCustomizer();
        switch (exception) {
            case WorkflowFailedException wfe -> {
                // if Events are already sent by TerminateDelegate, just let it propagate
                if (!this.state().workflowStatus().isTerminal()) {
                    // Whole-workflow terminal: interrupt running steps (no per-step terminal event) and
                    // discard the queue, then publish only the workflow-level FAILED event.
                    beginTerminalTeardown();
                    sendWorkflowEvent(failedWorkflow(
                                              this.workflowContext(),
                                              workflowName,
                                              wfe,
                                              eventNameCustomizer),
                                      ctx).join(); // FIXME join without timeout
                    try {
                        awaitStateChange(s -> s.workflowStatus().isTerminal());
                    } catch (Exception e) {
                        logger.error("Error waiting for termination of workflow instance {}", workflowId, e);
                    }
                }
            }
            case WorkflowCancelledException wce -> {
                // if Events are already sent by TerminateDelegate, just let it propagate
                if (!this.state().workflowStatus().isTerminal()) {
                    // Whole-workflow terminal: interrupt running steps (no per-step terminal event) and
                    // discard the queue, then publish only the workflow-level CANCELLED event.
                    beginTerminalTeardown();
                    sendWorkflowEvent(
                            cancelledWorkflow(this.workflowContext(),
                                              workflowName,
                                              wce,
                                              eventNameCustomizer),
                            ctx).join(); // FIXME join without timeout
                    try {
                        awaitStateChange(s -> s.workflowStatus().isTerminal());
                    } catch (Exception e) {
                        logger.error("Error waiting for termination of workflow instance {}", workflowId, e);
                    }
                }
            }
            case TimeoutException te -> {
                if (!this.state().workflowStatus().isTerminal()) {
                    // Whole-workflow terminal: interrupt running steps (no per-step terminal event) and
                    // discard the queue, then publish only the workflow-level TIMED_OUT event.
                    beginTerminalTeardown();
                    sendWorkflowEvent(timeoutWorkflow(
                                              this.workflowContext(),
                                              workflowName,
                                              contextDelegate.clock().instant(),
                                              eventNameCustomizer),
                                      ctx).join(); // FIXME join without timeout

                    try {
                        awaitStateChange(s -> s.workflowStatus().isTerminal());
                    } catch (Exception e) {
                        logger.error("Error waiting for termination of workflow instance {}", workflowId, e);
                    }
                }
            }
            case WorkflowReplayDriftException drift -> {
                logger.warn("Workflow {} paused due to replay drift: {}. "
                                    + "Revert the code change or wrap it in ctx.migrateVersion() and replay.",
                            workflowId, drift.getMessage());
                // Intentionally do NOT publish failedWorkflow / cancelledWorkflow events.
                // The workflow stays in its current (non-terminal) state; the next replay will try
                // again. If the developer reverts the offending code or adds ctx.migrateVersion(...), the
                // replay will run cleanly and the workflow continues normally.
            }
            case InterruptedException ie -> {
                Thread.currentThread().interrupt();
        /*
                // we agreed not to drive the workflow to terminal state on interrupted exception
                sendWorkflowEvent(
                    cancelledWorkflow(this.workflowContext(),
                                      workflowName,
                                      eventNameCustomizer),
                    ctx
                ).join(); // FIXME join without timeout

         */
            }
            default -> {
                logger.error("Error occurred in workflow {}", workflowId, exception);
                // we agreed not to drive the workflow to terminal state on any other exception
                /*
                sendWorkflowEvent(failedWorkflow(
                                          this.workflowContext(),
                                          workflowName,
                                          exception instanceof Exception ? (Exception) exception : new RuntimeException(exception),
                                          eventNameCustomizer),
                                  ctx).join(); // FIXME join without timeout

                 */
            }
        }
    }


    /**
     * Finish the workflow execution, clean up everything, and call the termination handler.
     *
     * @param terminationHandler termination handler to call.
     */
    private void finishWorkflow(Consumer<WorkflowExecution> terminationHandler) {
        this.executable = false; // mark we are done and are not executable anymore
        // TODO -> how do we recognize workflow executions which came to this point bit haven't reach the terminal states?
        this.taskQueue.clear();
        this.eventWaitConditions.clear();
        this.runningSteps.cancelAll(null, s -> {
        });
        terminationHandler.accept(this);
    }

    private void beginTerminalTeardown() {
        runningSteps.cancelAll(new StepInterruptedException("Workflow reached terminal state"), cancelled -> {
        });
        this.taskQueue.clear();
    }

    @Override
    public void awaitStateChange(
            @Nonnull Predicate<WorkflowState> predicate
    ) throws InterruptedException {
        do {
            var taken = taskQueue.take();
            taken.accept(this);
        } while (!predicate.test(this.state()));
    }

    @Override
    public void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
        if (executable) {
            // live mode

            eventWaitConditions.evaluateAndApply(eventMessage, processingContext, contextDelegate::eventReceived);
            appendTask(i -> state().evolve(eventMessage, processingContext));
        } else {
            // replay mode
            state().evolve(eventMessage, processingContext);
        }
    }

    @Override
    public void stopForShutdown() {
        runningSteps.cancelAll(new StepInterruptedException("Workflow engine shutdown"), s -> {
        });
        // Unblock the workflow driver thread parked on taskQueue.take() inside the current step's await() loop.
        // The task sets the driver thread's interrupt flag; the next taskQueue.take() observes it and throws
        // InterruptedException, propagating up so the driver thread exits cleanly.
        taskQueue.offer(i -> Thread.currentThread().interrupt());
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
            throw new RuntimeException("Too many tasks to perform workflow instance"); // FIXME <- task queue is full, backpressure?
        }
    }

    @Override
    public boolean hasTasks() {
        return this.taskQueue.isEmpty();
    }

    @Override
    public boolean isExecutable() {
        return executable;
    }

    private CompletableFuture<Void> sendWorkflowEvent(
            @Nonnull EventMessage eventMessage,
            @Nonnull ProcessingContext processingContext) {
        // TODO: make sure the consistency marker is used

        return ProcessingContextUtils
                .executeWithResult(
                        UUID.randomUUID().toString(),
                        processingContext.component(UnitOfWorkFactory.class),
                        processingContext.component(Executor.class, WORKFLOW_ENGINE_EXECUTOR),
                        processingContext,
                        childCtx -> {
                            logger.trace("Publishing workflow event {} from {}",
                                         eventMessage.type(),
                                         Thread.currentThread());
                            return contextDelegate.publishEvent(childCtx, eventMessage);
                        }
                );
    }

    @Override
    @Nonnull
    public ProcessingContext processingContext() {
        return contextDelegate.processingContext();
    }

    @Override
    @Nonnull
    public WorkflowState state() {
        return workflowState;
    }

    @Override
    @Nonnull
    public WorkflowContext workflowContext() {
        return this.contextDelegate;
    }

    @Nonnull
    @Override
    public String workflowName() {
        return this.workflowName;
    }

    @Nonnull
    @Override
    public String workflowId() {
        return this.workflowId;
    }

    @Override
    @Nullable
    public TrackingToken restartToken() {
        return restartToken;
    }

    @Nonnull
    @Override
    public WorkflowConfiguration<?> workflowConfiguration() {
        return this.workflowConfiguration;
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("delegate", contextDelegate);
        descriptor.describeProperty("executable", executable);
        descriptor.describeProperty("state", state());
        eventWaitConditions.describeTo(descriptor);
        runningSteps.describeTo(descriptor);
        workflowStepProgress.describeTo(descriptor);
    }

    @Nonnull
    @Override
    public WorkflowCancellation workflowCancellation() {
        return workflowCancellation;
    }
}
