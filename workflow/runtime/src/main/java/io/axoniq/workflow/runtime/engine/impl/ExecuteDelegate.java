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
import io.axoniq.workflow.runtime.api.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.PayloadProcessor;
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.result.WorkflowStepResults;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Execute delegate implementing {@link ExecutePrimitive}.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 1.0.0
 */
public class ExecuteDelegate extends AbstractStepExecutor implements ExecutePrimitive {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteDelegate.class);

    /**
     * Constructs the delegate.
     *
     * @param context                   workflow context.
     * @param workflowState             workflow state.
     * @param parentEventNameCustomizer event name customizer.
     * @param clock                     clock for time calculations.
     * @param unitOfWorkFactory         unit of work factory for creation of new processing contexts.
     * @param eventSink                 event sink for event publications.
     * @param executor                  executor to offload execution tasks from workflow thread.
     */
    public ExecuteDelegate(@Nonnull WorkflowContext context,
                           @Nonnull WorkflowState workflowState,
                           @Nonnull EventNameCustomizer parentEventNameCustomizer,
                           @Nonnull Clock clock,
                           @Nonnull UnitOfWorkFactory unitOfWorkFactory,
                           @Nonnull EventSink eventSink,
                           @Nonnull Executor executor
    ) {
        super(context, workflowState, parentEventNameCustomizer, clock, unitOfWorkFactory, eventSink, executor);
    }

    @Nonnull
    @Override
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nullable Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull PayloadReducer parameterMapping,
            @Nonnull PayloadReducer resultMapping,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        logger.trace("Execute {} called from thread {}", stepName, Thread.currentThread());

        acceptAllPendingTasksForStep(stepName);

        if (!workflowState.containsStep(stepName)) {
            workflowState.appendTask(i ->
                                             started(stepName, sanitize(local), eventNameCustomizer)
            );
            try {
                workflowState.awaitStateChange(s -> s.containsStep(stepName)
                        && s.getStep(stepName).status() == StepStatus.STARTED);
            } catch (InterruptedException e) {
                return WorkflowStepResults.failed(stepName, e);
            }
        }

        // FIXME -> consider to use QOS (at least once/at most once)
        if (workflowState.getStep(stepName).status() == StepStatus.STARTED) {
            var actualStartTime = workflowState.getStep(stepName).timestamp();
            var remainingTimeout = Duration.between(Instant.now(clock),
                                                    actualStartTime.plus(timeout));
            // FIXME - This is where we capture our current consistency marker

            var result = unitOfWorkFactory
                    .create(stepName,
                            customize -> customize.workScheduler(executor)) // FIXME -> define a new thread pool for execution customer code
                    .executeWithResult(processingContext -> {
                        var procContext = ProcessingContextUtils.copyResources(workflowState.getStep(stepName)
                                                                                            .context(),
                                                                               processingContext);
                        var payload = parameterMapping.apply(workflowContext.workflowPayload(), local);
                        return CompletableFuture.completedFuture(action.apply(procContext, payload));
                    });

            workflowState.registerRunningStep(stepName, result);

            if (remainingTimeout.isNegative()) {
                workflowState.appendTask(i -> {
                    // TODO - Do one last check on the state to make sure we didn't have any concurrent state changes
                    // FIXME - This is where we should publish using an append condition
                    timedOut(stepName, clock.instant(), eventNameCustomizer);
                });
            } else {
                result
                        .orTimeout(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
                        .whenComplete((r, e) -> {
                            workflowState.removeRunningStep(stepName);
                            if (r != null) {
                                workflowState.appendTask(i -> {
                                    workflowContext.applyPayloadModification(p -> resultMapping.apply(p,
                                                                                                      r)); // write back payload
                                    // FIXME - This is where we should publish using an append condition
                                    completed(stepName, r, eventNameCustomizer);
                                });
                            } else {
                                if (e instanceof TimeoutException || e.getCause() instanceof TimeoutException) {
                                    // FIXME - This is where we should publish using an append condition
                                    workflowState.appendTask(i -> {
                                        timedOut(stepName, clock.instant(), eventNameCustomizer);
                                    });
                                } else if (isCancellation(e)) {
                                    var terminationCause = unwrapCancellation(e);
                                    // FIXME - This is where we should publish using an append condition
                                    workflowState.appendTask(i -> {
                                        cancelled(stepName, terminationCause, eventNameCustomizer);
                                    });
                                } else {
                                    // FIXME - This is where we should publish using an append condition
                                    workflowState.appendTask(i -> {
                                        failed(stepName, e, eventNameCustomizer);
                                    });
                                }
                            }
                        });
            }
        }

        return WorkflowStepResults.stateBased(stepName, workflowState);
    }
}
