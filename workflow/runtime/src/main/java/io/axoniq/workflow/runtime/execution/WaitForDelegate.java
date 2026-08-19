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

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import jakarta.annotation.Nonnull;
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
     * @param workflowContext           workflow context.
     * @param workflowExecution         workflow state.
     * @param runningSteps              running step registry
     * @param eventWaitConditions       event wait condition registry
     * @param reachedSteps      reached steps tracker
     * @param parentEventNameCustomizer parent event name customizer.
     * @param clock                     clock for time calculations.
     * @param unitOfWorkFactory         unit of work factory for creation of new process contexts.
     * @param eventSink                 event sink to publish events.
     * @param executor                  executor to offload threads from main thread.
     * @param timeoutScheduler          scheduler for workflow step timeouts
     */
    @Internal
    public WaitForDelegate(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull RunningSteps runningSteps,
            @Nonnull EventWaitConditions eventWaitConditions,
            @Nonnull ReachedSteps reachedSteps,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor,
            @Nonnull WorkflowScheduler timeoutScheduler
    ) {
        super(workflowContext,
              workflowExecution, runningSteps, reachedSteps, parentEventNameCustomizer, clock, unitOfWorkFactory, eventSink, executor,
              timeoutScheduler);
        this.eventWaitConditions = Objects.requireNonNull(eventWaitConditions, "Event wait conditions are mandatory");
    }

    @Override
    @Nonnull
    public WorkflowStepResult waitForEvent(@Nonnull WaitForPrimitive.WaitForCommand command) {
        var stepName = command.stepName();
        var eventCondition = command.eventCondition();
        var resultPayloadReducer = command.resultPayloadReducer();
        var timeout = command.timeout();
        var eventNameCustomizer = command.eventNameCustomizer();
        logger.trace("WaitFor {} called from thread {}", stepName, Thread.currentThread());

        reachedSteps.record(stepName);

        acceptAllPendingTasksForStep(stepName);

        if (!workflowExecution.state().containsStep(stepName)) {
            reachedSteps.guardAgainstReplayDrift(workflowExecution.workflowId(), workflowExecution.state(), stepName);
            workflowExecution.appendTask(i ->
                                                 startedWaitForEvent(stepName,
                                                                     startedPayload(eventCondition, clock.instant(), timeout),
                                                                     eventNameCustomizer));
            try {
                workflowExecution.awaitStateChange(s -> s.containsStep(stepName)
                        && s.getStep(stepName).status() == StepStatus.STARTED);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return WorkflowStepResults.canceled(stepName);
            }
        }

        if (workflowExecution.state().getStep(stepName).status() == StepStatus.STARTED) {
            var actualStartTime = workflowExecution.state().getStep(stepName).timestamp();
            var timeoutDeadline = actualStartTime.plus(timeout);
            var remainingTimeout = Duration.between(clock.instant(), timeoutDeadline);

            if (remainingTimeout.isNegative()) {
                workflowExecution.appendTask(i -> {
                    if (!i.state().getStep(stepName).status().isTerminal()) {
                        // FIXME - This is where we should publish using an append condition
                        timedOutWaitForEvent(stepName, clock.instant(), eventNameCustomizer);
                    }
                });
            } else {
                // Register wait condition
                eventWaitConditions.add(stepName, eventCondition, resultPayloadReducer, eventNameCustomizer);
                var timeoutTask = timeoutScheduler.schedule(
                        timeoutDeadline,
                        () -> {
                            eventWaitConditions.remove(stepName);
                            runningSteps.remove(stepName);
                            workflowExecution.appendTask(i -> {
                                                             if (!i.state().getStep(stepName).status().isTerminal()) {
                                                                 // only timeout if we are not completed yet
                                                                 timedOutWaitForEvent(stepName, eventNameCustomizer);
                                                             }
                                                         }
                            );
                        }
                );
                registerParkedStep(stepName, timeoutTask.completion(), eventNameCustomizer,
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
    void eventReceived(@Nonnull EventWaitConditions.Awaited awaited) {
        // Cancel the timeout future since the awaited event has arrived
        runningSteps.cancelAndRemove(awaited.stepName(), false);
        var payload = eventMessagePayload(awaited.eventMessage());
        workflowExecution.appendTask(state -> {
            try {
                completedWaitForEvent(awaited.stepName(),
                                      payload,
                                      awaited.payloadReducer().name(),
                                      awaited.eventNameCustomizer()).join();
            } catch (Exception e) {
                logger.warn("Failed to publish completed event for step '{}': {}",
                            awaited.stepName(),
                            e.getMessage());
            }
        });
    }

    @Nonnull
    private Map<String, Object> startedPayload(@Nonnull EventCondition eventCondition,
                                               @Nonnull Instant startedAt,
                                               @Nonnull Duration timeout) {
        var payload = new LinkedHashMap<String, Object>();
        payload.put("startTime", startedAt);
        payload.put("eventName", eventCondition.qualifiedName().toString());
        payload.put("associations", eventCondition.associations());
        payload.put("timeoutTime", startedAt.plus(timeout));
        return payload;
    }
}
