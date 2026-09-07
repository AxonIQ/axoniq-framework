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
package io.axoniq.framework.workflow.simulation.scenarios;

import io.axoniq.framework.workflow.simulation.harness.LogCapture;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;

/**
 * The counts every fence scenario reads off the durable log, and the rejection warnings that prove the fence fired.
 * <p>
 * One place so a fence scenario is its own interleaving and oracle and nothing else. Every count is per instance and
 * derived from the committed log, never from live engine state, so a scenario cannot pass on a value the store does
 * not hold.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
final class FenceOracles {

    static final String REJECTION_LOGGER = "io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution";

    private FenceOracles() {
    }

    /**
     * Attaches a fresh appender to the execution logger and returns it; the caller must {@link #detach} it.
     */
    static LogCapture attachRejectionAppender() {
        return LogCapture.attach(REJECTION_LOGGER);
    }

    static void detach(LogCapture appender) {
        appender.close();
    }

    /**
     * Counts the rejection warnings logged for one instance — the fence firing, from the engine's own mouth.
     */
    static int rejections(LogCapture appender, String workflowId) {
        return (int) appender.messages().stream()
                         .filter(message -> message != null && message.contains("was rejected")
                                 && message.contains("'" + workflowId + "'"))
                         .count();
    }

    /**
     * Counts committed step records for one {@code (workflowId, stepName, status)}.
     */
    static int stepRecords(List<EventMessage> log, String workflowId,
                           String stepName, StepStatus status) {
        return (int) log.stream()
                        .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                                && stepName.equals(MetadataUtils.getStepName(event.metadata()))
                                && MetadataUtils.getStepStatus(event.metadata())
                                                .filter(s -> s == status).isPresent())
                        .count();
    }

    /**
     * Counts every committed record of one instance — the "nothing more was accepted" measure a fence needs.
     */
    static int records(List<EventMessage> log, String workflowId) {
        return (int) log.stream()
                        .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata())))
                        .count();
    }

    /**
     * Counts committed terminal workflow records for one instance.
     */
    static int terminalRecords(List<EventMessage> log, String workflowId) {
        return (int) log.stream()
                        .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                                && MetadataUtils.getWorkflowStatus(event.metadata())
                                                .filter(WorkflowStatus::isTerminal).isPresent())
                        .count();
    }

    /**
     * Counts committed {@code STARTED} workflow records for one instance.
     */
    static int startedRecords(List<EventMessage> log, String workflowId) {
        return (int) log.stream()
                        .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                                && MetadataUtils.getWorkflowStatus(event.metadata())
                                                .filter(status -> status == WorkflowStatus.STARTED).isPresent())
                        .count();
    }
}
