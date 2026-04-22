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
import io.axoniq.workflow.runtime.api.execution.context.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


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

    /**
     * Constructs the delegate.
     *
     * @param workflowContext           workflow context.
     * @param workflowExecution         workflow state.
     * @param parentEventNameCustomizer parent event name customizer.
     * @param clock                     clock for time calculations.
     * @param unitOfWorkFactory         unit of work factory for creation of new process contexts.
     * @param eventSink                 event sink to publish events.
     * @param executor                  executor to offload threads from main thread.
     */
    @Internal
    public WaitForDelegate(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor
    ) {
        super(workflowContext,
              workflowExecution, parentEventNameCustomizer, clock, unitOfWorkFactory, eventSink, executor);
    }

    @Override
    @Nonnull
    public WorkflowStepResult waitFor(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        logger.trace("WaitFor {} called from thread {}", stepName, Thread.currentThread());

        acceptAllPendingTasksForStep(stepName);

        if (!workflowExecution.state().containsStep(stepName)) {
            workflowExecution.appendTask(i ->
                                                 started(stepName,
                                                         Map.of("startTime", clock.instant()),
                                                         eventNameCustomizer)
            );
            try {
                workflowExecution.awaitStateChange(s -> s.containsStep(stepName)
                        && s.getStep(stepName).status() == StepStatus.STARTED);
            } catch (InterruptedException e) {
                return WorkflowStepResults.failed(stepName, e);
            }
        }

        if (workflowExecution.state().getStep(stepName).status() == StepStatus.STARTED) {
            var actualStartTime = workflowExecution.state().getStep(stepName).timestamp();
            var remainingTimeout = Duration.between(clock.instant(),
                                                    actualStartTime.plus(timeout));

            if (remainingTimeout.isNegative()) {
                workflowExecution.appendTask(i -> {
                    if (!i.state().getStep(stepName).status().isTerminal()) {
                        // FIXME - This is where we should publish using an append condition
                        timedOut(stepName, clock.instant(), eventNameCustomizer);
                    }
                });
            } else {
                // Register wait condition
                workflowExecution.registerWaitCondition(stepName,
                                                        eventCondition,
                                                        resultPayloadReducer,
                                                        eventNameCustomizer);
                var timeoutFuture = CompletableFuture.runAsync(
                        () -> {
                            workflowExecution.removeWaitCondition(stepName);
                            workflowExecution.removeRunningStep(stepName);
                            workflowExecution.appendTask(i -> {
                                                             if (!i.state().getStep(stepName).status().isTerminal()) {
                                                                 // only timeout if we are not completed yet
                                                                 timedOut(stepName, eventNameCustomizer);
                                                             }
                                                         }
                            );
                        },
                        CompletableFuture.delayedExecutor(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
                );
                // Attach the cancellation side-effect handler to the upstream future so that
                // completeExceptionally on the registered future (e.g. from cancelAllRunningSteps)
                // fires the handler immediately instead of waiting for the delayed runnable.
                timeoutFuture.exceptionally(e -> {
                    workflowExecution.removeRunningStep(stepName);
                    if (isCancellation(e)) {
                        var terminationCause = unwrapCancellation(e);
                        workflowExecution.removeWaitCondition(stepName);
                        workflowExecution.appendTask(i -> {
                            // FIXME - This is where we should publish using an append condition
                            if (!i.state().getStep(stepName).status().isTerminal()) {
                                cancelled(stepName, terminationCause, eventNameCustomizer);
                            }
                        });
                    }
                    return null;
                });
                workflowExecution.registerRunningStep(stepName, timeoutFuture);
            }
        }

        return stateBased(stepName, workflowExecution);
    }

    /**
     * Receives an event message (because of wait condition) to trigger the wait for continuation.
     *
     * @param awaited event arrival information.
     */
    void eventReceived(@Nonnull EventWaitConditions.Awaited awaited) {
        // Cancel the timeout future since the awaited event has arrived
        workflowExecution.cancelAndRemoveRunningStep(awaited.stepName(), false);
        var payload = eventMessagePayload(awaited.eventMessage());
        final String resultMappingName;
        if (PayloadReducer.isDefault(awaited.payloadReducer())) {
            resultMappingName = PayloadReducer.name(awaited.payloadReducer());
        } else {
            resultMappingName = null;
        }
        workflowExecution.appendTask(state -> {
            try {
                completed(awaited.stepName(),
                          payload,
                          resultMappingName,
                          awaited.eventNameCustomizer()).join();
            } catch (Exception e) {
                logger.warn("Failed to publish completed event for step '{}': {}",
                            awaited.stepName(),
                            e.getMessage());
            }
        });
    }
}
