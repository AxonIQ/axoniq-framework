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
import io.axoniq.workflow.runtime.api.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.WorkflowStepResults;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;


/**
 * Delegate implementing {@link WaitForPrimitive}.
 *
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WaitForDelegate extends AbstractStepExecutor implements WaitForPrimitive {

    private static final Logger logger = LoggerFactory.getLogger(WaitForDelegate.class);

    /**
     * Constructs the delegate.
     *
     * @param workflowContext           workflow context.
     * @param workflowState             workflow state.
     * @param parentEventNameCustomizer parent event name customizer.
     * @param clock                     clock for time calculations.
     * @param unitOfWorkFactory         unit of work factory for creation of new process contexts.
     * @param eventSink                 event sink to publish events.
     * @param executor                  executor to offload threads from main thread.
     */
    public WaitForDelegate(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowState workflowState,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor
    ) {
        super(workflowContext, workflowState, parentEventNameCustomizer, clock, unitOfWorkFactory, eventSink, executor);
    }

    @Override
    @Nonnull
    public WorkflowStepResult waitFor(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        logger.trace("WaitFor {} called from thread {}", stepName, Thread.currentThread());

        acceptAllPendingTasksForStep(stepName);

        if (!workflowState.containsStep(stepName)) {
            workflowState.appendTask(i ->
                                             started(stepName,
                                                     Map.of("startTime", clock.instant()),
                                                     eventNameCustomizer)
            );
            try {
                workflowState.awaitStateChange(s -> s.containsStep(stepName)
                        && s.getStep(stepName).status() == StepStatus.STARTED);
            } catch (InterruptedException e) {
                return WorkflowStepResults.failed(stepName, e);
            }
        }

        if (workflowState.getStep(stepName).status() == StepStatus.STARTED) {
            var actualStartTime = workflowState.getStep(stepName).timestamp();
            var remainingTimeout = Duration.between(clock.instant(),
                                                    actualStartTime.plus(timeout));

            if (remainingTimeout.isNegative()) {
                workflowState.appendTask(i -> {
                    if (!i.getStep(stepName).status().isTerminal()) {
                        // FIXME - This is where we should publish using an append condition
                        timedOut(stepName, clock.instant(), eventNameCustomizer);
                    }
                });
            } else {
                // Register wait condition
                workflowState.registerWaitCondition(stepName, eventCondition, eventNameCustomizer);
                var timeoutFuture = CompletableFuture.runAsync(() -> {
                                               workflowState.removeWaitCondition(stepName);
                                               workflowState.removeRunningStep(stepName);
                                               workflowState.appendTask(i -> {
                                                                            if (!i.getStep(stepName).status().isTerminal()) {
                                                                                // only timeout if we are not completed yet
                                                                                timedOut(stepName, eventNameCustomizer);
                                                                            }
                                                                        }
                                               );
                                           }, CompletableFuture.delayedExecutor(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
                ).exceptionally(e -> {
                    workflowState.removeRunningStep(stepName);
                    if (isCancellation(e)) {
                        var terminationCause = unwrapCancellation(e);
                        workflowState.removeWaitCondition(stepName);
                        workflowState.appendTask(i -> {
                            // FIXME - This is where we should publish using an append condition
                            if (!i.getStep(stepName).status().isTerminal()) {
                                cancelled(stepName, terminationCause, eventNameCustomizer);
                            }
                        });
                    }
                    return null;
                });
                workflowState.registerRunningStep(stepName, timeoutFuture);
            }
        }

        return WorkflowStepResults.stateBased(stepName, workflowState);
    }

    /**
     * Receive event message (because of wait condition) to trigger the wait for continuation.
     *
     * @param eventMessage        message to deliver the event.
     * @param stepName            step name waiting for event.
     * @param eventNameCustomizer customizer for the step name.
     */
    void eventReceived(@Nonnull EventMessage eventMessage, @Nonnull String stepName,
                       @Nonnull EventNameCustomizer eventNameCustomizer) {
        // Cancel the timeout future since the awaited event has arrived
        workflowState.cancelAndRemoveRunningStep(stepName, false);
        // TODO event should be mapped back based on result mapping
        var payload = eventMessagePayload(eventMessage);
        workflowState.appendTask(state -> {
            try {
                completed(stepName, payload, eventNameCustomizer).join();
            } catch (Exception e) {
                logger.warn("Failed to publish completed event for step '{}': {}", stepName, e.getMessage());
            }
        });
    }
}
