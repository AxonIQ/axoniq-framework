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
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.api.payload.PayloadReducer.NAME_LOCAL_ONLY;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;
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
     */
    @Internal
    public PayloadDelegate(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor
    ) {
        super(workflowContext,
              workflowExecution,
              parentEventNameCustomizer,
              clock,
              unitOfWorkFactory,
              eventSink,
              executor);
    }

    @Override
    public void modifyPayload(
            @Nonnull String stepName,
            @Nonnull PayloadModification payloadModification,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        workflowExecution.appendTask(e -> {
                                         // apply modification right away
                                         var newPayload = payloadModification.apply(workflowExecution.workflowContext().workflowPayload());
                                         var payloadEvent = completedStep(workflowContext,
                                                                          stepName,
                                                                          sanitize(newPayload),
                                           NAME_LOCAL_ONLY, // replace later the entire payload
                                                                          merge(parentEventNameCustomizer, eventNameCustomizer));
                                         ProcessingContextUtils.executeWithResult(
                                                 workflowExecution.workflowId(),
                                                 unitOfWorkFactory,
                                                 executor,
                                                 workflowExecution.processingContext(),
                                                 ctx -> eventSink.publish(ctx, payloadEvent)
                                         );
                                     }
        );
        try {
            workflowExecution.awaitStateChange(s -> s.containsStep(stepName)
                    && s.getStep(stepName).status() == StepStatus.COMPLETED);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
