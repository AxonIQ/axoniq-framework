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

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.WorkflowStepResults;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.util.ProcessingContextUtils;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.completedStep;

public class WaitForDelegate extends AbstractStepExecutor implements WaitForPrimitive {

    private static final Logger logger = LoggerFactory.getLogger(WaitForDelegate.class);

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
            @Nonnull QualifiedName qualifiedName,
            @Nonnull Predicate<EventMessage> predicate,
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
                workflowState.registerWaitCondition(stepName, qualifiedName, predicate, eventNameCustomizer);
                var timeoutFuture = CompletableFuture.runAsync(() -> {
                                               workflowState.removeWaitCondition(stepName);
                                               workflowState.removeRunningFuture(stepName);
                                               workflowState.appendTask(i -> {
                                                                            if (!i.getStep(stepName).status().isTerminal()) {
                                                                                // only timeout if we are not completed yet
                                                                                timedOut(stepName, eventNameCustomizer);
                                                                            }
                                                                        }
                                               );
                                           }, CompletableFuture.delayedExecutor(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
                ).exceptionally(e -> {
                    workflowState.removeRunningFuture(stepName);
                    if (e instanceof CancellationException) {
                        var terminationCause = e.getCause();
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
                workflowState.registerRunningFuture(stepName, timeoutFuture);
            }
        }

        return WorkflowStepResults.stateBased(stepName, workflowState);
    }

    void eventReceived(@Nonnull EventMessage eventMessage, @Nonnull String stepName,
                       @Nonnull EventNameCustomizer eventNameCustomizer) {
        // Cancel the timeout future since the awaited event has arrived
        workflowState.cancelAndRemoveRunningFuture(stepName, false);
        // TODO event should be mapped back based on result mapping
        var payload = eventMessagePayload(eventMessage);
        workflowState.appendTask(state ->
                                         ProcessingContextUtils.executeWithResult(
                                                 stepName,
                                                 unitOfWorkFactory,
                                                 executor,
                                                 state.getStep(stepName).context(),
                                                 ctx ->
                                                         eventSink.publish(
                                                                 ctx,
                                                                 completedStep(workflowContext, stepName, payload,
                                                                               merge(parentEventNameCustomizer,
                                                                                     eventNameCustomizer)
                                                                 )
                                                         )
                                         ).join()
        );
    }
}
