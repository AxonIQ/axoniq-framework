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

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Read-side helpers shared by the publish-primitive scenarios: durable-log counts per instance and the engine's own
 * reconstructed step names per instance. Package-private on purpose, like {@link FenceOracles}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
final class PublishOracles {

    private PublishOracles() {
    }

    /**
     * Distinct committed COMPLETED records of {@code stepName} for {@code workflowId} (distinct event identifiers, so a
     * duplicated commit of one event counts once).
     */
    static int publishRecords(List<EventMessage> log, String workflowId, String stepName) {
        var ids = new HashSet<String>();
        for (EventMessage event : log) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                    && MetadataUtils.getStepStatus(event.metadata()).filter(s -> s == StepStatus.COMPLETED).isPresent()
                    && stepName.equals(MetadataUtils.getStepName(event.metadata()))) {
                ids.add(event.identifier());
            }
        }
        return ids.size();
    }

    /**
     * The qualified type name of the first committed COMPLETED record of {@code stepName} for {@code workflowId}, or
     * {@code null} when there is none.
     */
    static @Nullable String recordType(List<EventMessage> log, String workflowId, String stepName) {
        return log.stream()
                  .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                          && MetadataUtils.getStepStatus(e.metadata()).filter(s -> s == StepStatus.COMPLETED).isPresent()
                          && stepName.equals(MetadataUtils.getStepName(e.metadata())))
                  .map(e -> e.type().qualifiedName().toString())
                  .findFirst()
                  .orElse(null);
    }

    /**
     * {@code true} iff the log holds any step record of {@code stepName} with {@code status} for {@code workflowId}.
     */
    static boolean hasStep(List<EventMessage> log, String workflowId, String stepName, StepStatus status) {
        return log.stream().anyMatch(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                && MetadataUtils.getStepStatus(e.metadata()).filter(s -> s == status).isPresent()
                && stepName.equals(MetadataUtils.getStepName(e.metadata())));
    }

    /**
     * {@code true} iff the log holds a terminal workflow-status record for {@code workflowId}.
     */
    static boolean isTerminal(List<EventMessage> log, String workflowId) {
        return log.stream().anyMatch(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    /**
     * Number of workflow STARTED status records for {@code workflowId} (how many times the instance was spawned).
     */
    static int starts(List<EventMessage> log, String workflowId) {
        return FenceOracles.startedRecords(log, workflowId);
    }

    /**
     * The engine's own reconstructed step names per {@code workflowId}, from the workflow-history read-model.
     */
    static Map<String, List<String>> stepNamesById(SimulationWorld world) {
        var byWorkflow = new TreeMap<String, List<String>>();
        for (var history : world.engine().historyRepository().findAll()) {
            byWorkflow.put(history.workflowId(), List.copyOf(history.state().workflowStepNames()));
        }
        return byWorkflow;
    }
}
