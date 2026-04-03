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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.PayloadModification;
import io.axoniq.workflow.runtime.api.PayloadPrimitive;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.api.PayloadReducer.NAME_LOCAL_ONLY;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.completedStep;

/**
 * Primitive implementing durable payload modifications.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class PayloadDelegate extends AbstractStepExecutor implements PayloadPrimitive {

    /**
     * Constructs the primitive implementation.
     */
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
