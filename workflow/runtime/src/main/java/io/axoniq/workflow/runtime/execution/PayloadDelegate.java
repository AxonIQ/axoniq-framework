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
import io.axoniq.workflow.runtime.api.execution.context.PayloadPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import io.axoniq.workflow.runtime.util.FutureResolver;
import io.axoniq.workflow.runtime.util.WorkflowStateUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.time.Clock;
import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer.NAME;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.completedStep;

/**
 * Primitive implementing durable payload modifications.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class PayloadDelegate extends AbstractStepExecutor implements PayloadPrimitive {

    /**
     * Constructs the primitive implementation.
     *
     * @param workflowContext            workflow context
     * @param workflowExecution          workflow execution
     * @param runningSteps              running step registry
     * @param reachedSteps              reached steps tracker
     * @param parentEventNameCustomizer parent event name customizer
     * @param clock                     clock for time calculations
     * @param unitOfWorkFactory         unit of work factory for processing contexts
     * @param eventSink                 event sink for event publications
     * @param executor                  executor for step work
     * @param timeoutScheduler          scheduler for step timeouts
     */
    @Internal
    public PayloadDelegate(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull RunningSteps runningSteps,
            @Nonnull ReachedSteps reachedSteps,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor,
            @Nonnull WorkflowScheduler timeoutScheduler
    ) {
        super(workflowContext,
              workflowExecution,
              runningSteps,
              reachedSteps,
              parentEventNameCustomizer,
              clock,
              unitOfWorkFactory,
              eventSink,
              executor,
              timeoutScheduler);
    }

    @Override
    @Nonnull
    public WorkflowStepResult modifyPayload(@Nonnull PayloadPrimitive.ModifyPayloadCommand command) {
        var stepName = command.stepName();
        var payloadModification = command.payloadModification();
        var eventNameCustomizer = command.eventNameCustomizer();
        reachedSteps.record(stepName);
        // Drift guard + replay-skip gate: payload publishes COMPLETED directly, so gate both the guard and the
        // publish on the first live run. On a post-crash live re-run the step is already present, so skip
        // re-publishing (the replay-skip gate the other primitives have) to avoid a duplicate terminal record.
        if (!workflowExecution.state().containsStep(stepName)) {
            reachedSteps.assertNoReplayDrift(workflowExecution.workflowId(), workflowExecution.state(), stepName);
            workflowExecution.appendTask(e -> {
                                             // apply modification right away
                                             var newPayload = payloadModification.apply(workflowExecution.workflowContext().workflowPayload());
                                             var payloadEvent = completedStep(workflowContext,
                                                                              stepName,
                                                                              sanitize(newPayload),
                                                                              NAME, // replace later the entire payload
                                                                              merge(parentEventNameCustomizer, eventNameCustomizer));
                                             FutureResolver.resolve(
                                                     workflowExecution.processingContext(),
                                                     ProcessingContextUtils.executeWithResult(
                                                             workflowExecution.workflowId(),
                                                             unitOfWorkFactory,
                                                             executor,
                                                             workflowExecution.processingContext(),
                                                             ctx -> eventSink.publish(ctx, payloadEvent)
                                                     )
                                             );
                                         }
            );
        }
        try {
            workflowExecution.awaitStateChange(WorkflowStateUtils.stepStatus(stepName, StepStatus.COMPLETED));
            return WorkflowStepResults.completed(stepName,
                                                 workflowExecution.state().payload(),
                                                 workflowExecution.processingContext().component(EventConverter.class));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return WorkflowStepResults.canceled(stepName);
        }
    }
}
