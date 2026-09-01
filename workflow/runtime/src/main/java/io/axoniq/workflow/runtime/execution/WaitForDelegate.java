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

import org.jspecify.annotations.Nullable;

import io.axoniq.workflow.runtime.api.execution.FutureResolutionTimeoutException;
import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.util.FutureResolver;
import io.axoniq.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;


/**
 * Delegate implementing {@link WaitForPrimitive}.
 *
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class WaitForDelegate extends AbstractStepExecutor implements WaitForPrimitive {

    private static final Logger logger = LoggerFactory.getLogger(WaitForDelegate.class);
    private final EventWaitConditions eventWaitConditions;

    /**
     * Constructs the delegate.
     *
     * @param workflowContext            workflow context.
     * @param workflowExecution          workflow state.
     * @param runningSteps              running step registry
     * @param eventWaitConditions       event wait condition registry
     * @param reachedSteps              reached steps tracker
     * @param parentEventNameCustomizer parent event name customizer.
     * @param clock                     clock for time calculations.
     * @param unitOfWorkFactory         unit of work factory for creation of new process contexts.
     * @param eventSink                 event sink to publish events.
     * @param executor                  executor to offload threads from main thread.
     * @param timeoutScheduler          scheduler for workflow step timeouts
     */
    @Internal
    public WaitForDelegate(
            WorkflowContext workflowContext,
            WorkflowExecution workflowExecution,
            RunningSteps runningSteps,
            EventWaitConditions eventWaitConditions,
            ReachedSteps reachedSteps,
            EventNameCustomizer parentEventNameCustomizer,
            Clock clock,
            WorkflowScheduler timeoutScheduler
    ) {
        super(workflowContext,
              workflowExecution,
              runningSteps,
              reachedSteps,
              parentEventNameCustomizer,
              clock,
              timeoutScheduler);
        this.eventWaitConditions = Objects.requireNonNull(eventWaitConditions, "Event wait conditions are mandatory");
    }

    @Override
    public WorkflowStepResult waitForEvent(WaitForPrimitive.WaitForCommand command) {
        var stepName = command.stepName();
        var eventCondition = command.eventCondition();
        var resultPayloadReducer = command.resultPayloadReducer();
        var timeout = command.timeout();
        var eventNameCustomizer = command.eventNameCustomizer();
        logger.trace("WaitFor {} called from thread {}", stepName, Thread.currentThread());

        reachedSteps.record(stepName);

        acceptAllPendingTasksForStep(stepName);

        if (!workflowExecution.state().containsStep(stepName)) {
            reachedSteps.assertNoReplayDrift(workflowExecution.workflowId(), workflowExecution.state(), stepName);
            workflowExecution.appendTask(i ->
                                                 startedWaitForEvent(stepName,
                                                                     startedPayload(eventCondition,
                                                                                    clock.instant(),
                                                                                    timeout),
                                                                     eventNameCustomizer));
            try {
                workflowExecution.awaitStateChange(WorkflowStateUtils.stepStatus(stepName, StepStatus.STARTED));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return WorkflowStepResults.canceled(stepName);
            }
        }

        if (WorkflowStateUtils.isStepStatus(workflowExecution.state(), stepName, StepStatus.STARTED)) {
            var actualStartTime = workflowExecution.state().getStep(stepName).timestamp();
            var timeoutDeadline = actualStartTime.plus(timeout);
            var remainingTimeout = Duration.between(clock.instant(), timeoutDeadline);

            if (remainingTimeout.isNegative()) {
                workflowExecution.appendTask(i -> {
                    if (!WorkflowStateUtils.isStepTerminal(i.state(), stepName)) {
                        timedOutWaitForEvent(stepName, clock.instant(), eventNameCustomizer);
                    }
                });
            } else {
                // Register wait condition
                eventWaitConditions.add(stepName, eventCondition, resultPayloadReducer, eventNameCustomizer);
                var timeoutTask = timeoutScheduler.schedule(timeoutDeadline);
                timeoutTask.completion().thenRun(() -> workflowExecution.appendTask(i -> {
                            eventWaitConditions.remove(stepName);
                            runningSteps.remove(stepName);
                            if (WorkflowStateUtils.isStepActive(i.state(), stepName)) {
                                // Only timeout if the event has not already completed the step.
                                timedOutWaitForEvent(stepName, eventNameCustomizer);
                            }
                        }));
                registerParkedStep(stepName, timeoutTask.completion(), timeoutTask::cancel, eventNameCustomizer,
                                   () -> eventWaitConditions.remove(stepName));
            }
        }

        return stateBased(stepName, eventNameCustomizer, workflowExecution);
    }

    /**
     * Receives an event message (because of wait condition) to trigger the wait for continuation.
     *
     * @param awaited event arrival information.
     */
    void eventReceived(EventWaitConditions.Awaited awaited) {
        // Cancel the timeout future since the awaited event has arrived
        runningSteps.cancelAndRemove(awaited.stepName(), false);
        var payload = eventMessagePayload(awaited.eventMessage());
        workflowExecution.appendTask(state -> {
            try {
                FutureResolver.resolve(
                        workflowExecution.processingContext(),
                        completedWaitForEvent(awaited.stepName(),
                                              payload,
                                              awaited.payloadReducer().name(),
                                              awaited.eventNameCustomizer())
                );
            } catch (FutureResolutionTimeoutException timeout) {
                throw timeout;
            } catch (Exception e) {
                logger.warn("Failed to publish completed event for step '{}': {}",
                            awaited.stepName(),
                            e.getMessage());
            }
        });
    }

    private Map<String, @Nullable Object> startedPayload(EventCondition eventCondition,
                                               Instant startedAt,
                                               Duration timeout) {
        var payload = new LinkedHashMap<String, @Nullable Object>();
        payload.put("startTime", startedAt);
        payload.put("eventName", eventCondition.qualifiedName().toString());
        payload.put("associations", eventCondition.associations());
        payload.put("timeoutTime", startedAt.plus(timeout));
        return payload;
    }
}
