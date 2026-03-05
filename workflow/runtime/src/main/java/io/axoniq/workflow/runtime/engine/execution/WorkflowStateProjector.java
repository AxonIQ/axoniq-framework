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
package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import io.axoniq.workflow.runtime.engine.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.axoniq.workflow.runtime.engine.util.MetadataUtils.getStepName;

/**
 * Projector for workflow state.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowStateProjector {

    private final static Logger logger = LoggerFactory.getLogger(WorkflowStateProjector.class);

    /**
     * Applies changes carried by the event message to a given workflow state.
     *
     * @param eventMessage      event message.
     * @param processingContext processing context opf message delivery.
     * @param state             workflow state to modify.
     */
    public static void applyStateChange(
            @Nonnull EventMessage eventMessage,
            @Nonnull ProcessingContext processingContext,
            @Nonnull WorkflowState state
    ) {
        logger.trace("Applying event {}", eventMessage.type());
        Object eventPayload = eventMessage.payloadAs(Object.class);
        var metadata = eventMessage.metadata();
        // Apply step-level state changes — ignore transitions once already terminal
        MetadataUtils.getStepStatus(metadata).ifPresent(stepStatus -> {
            var stepName = getStepName(metadata);
            if (state.containsStep(stepName) && state.getStep(stepName).status().isTerminal()) {
                logger.warn("Ignoring step status {} for step '{}' — already in terminal state {}",
                            stepStatus, stepName, state.getStep(stepName).status());
                return;
            }
            switch (stepStatus) {
                case STARTED:
                    state.addStep(WorkflowStep.started(stepName,
                                                       eventPayload,
                                                       eventMessage.timestamp(),
                                                       processingContext)); // TODO copy resources of the context
                    break;
                case FAILED:
                    state.addStep(WorkflowStep.failed(stepName,
                                                      (Throwable) eventPayload,
                                                      eventMessage.timestamp(),
                                                      processingContext)); // TODO copy resources of the context
                    break;
                case TIMED_OUT:
                    state.addStep(WorkflowStep.timedOut(stepName,
                                                        eventPayload,
                                                        eventMessage.timestamp(),
                                                        processingContext)); // TODO copy resources of the context
                    break;
                case COMPLETED:
                    state.addStep(WorkflowStep.completed(stepName,
                                                         eventPayload,
                                                         eventMessage.timestamp(),
                                                         processingContext)); // TODO copy resources of the context
                    break;
                case CANCELLED:
                    state.addStep(WorkflowStep.cancelled(stepName,
                                                         eventMessage.timestamp(),
                                                         processingContext)); // TODO copy resources of the context
                    break;
                default:
                    break;
            }
        });
        // Apply workflow-level state changes — ignore transitions once already terminal
        MetadataUtils.getWorkflowStatus(metadata).ifPresent(status -> {
            if (state.workflowStatus().isTerminal()) {
                logger.warn("Ignoring workflow status {} — already in terminal state {}",
                            status, state.workflowStatus());
                return;
            }
            final Throwable terminationCause;
            if ((status == WorkflowStatus.FAILED || status == WorkflowStatus.CANCELLED)
                    && eventPayload instanceof Throwable t) {
                terminationCause = t;
            } else {
                terminationCause = null;
            }
            state.setStatus(status, terminationCause);
        });
    }
}
