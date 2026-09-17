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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.api.execution.context.PayloadPrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.time.Clock;
import java.util.concurrent.Executor;

import static io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer.NAME;

/**
 * Primitive implementing durable payload modifications.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class PayloadDelegate extends AbstractStepExecutor implements PayloadPrimitive {

    /**
     * Constructs the primitive implementation.
     *
     * @param workflowExecutionOperations runtime primitive-operation surface
     * @param workflowExecution          workflow execution
     * @param runningSteps              running step registry
     * @param reachedSteps              reached steps tracker
     * @param parentEventNameCustomizer parent event name customizer
     * @param clock                     clock for time calculations
     * @param timeoutScheduler          scheduler for step timeouts
     */
    @Internal
    public PayloadDelegate(
            WorkflowExecutionOperations workflowExecutionOperations,
            WorkflowExecution workflowExecution,
            RunningSteps runningSteps,
            ReachedSteps reachedSteps,
            EventNameCustomizer parentEventNameCustomizer,
            Clock clock,
            WorkflowScheduler timeoutScheduler
    ) {
        super(workflowExecutionOperations,
              workflowExecution,
              runningSteps,
              reachedSteps,
              parentEventNameCustomizer,
              clock,
              timeoutScheduler);
    }

    @Override
    public WorkflowStepResult modifyPayload(PayloadPrimitive.ModifyPayloadCommand command) {
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
                                             var newPayload = payloadModification.apply(workflowExecution.workflowExecutionOperations().workflowPayload());
                                             FutureResolver.resolve(
                                                     workflowExecution.processingContext(),
                                                     completed(stepName,
                                                               sanitize(newPayload),
                                                               NAME, // replace later the entire payload
                                                               eventNameCustomizer)
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
