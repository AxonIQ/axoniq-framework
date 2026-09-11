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

import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.VersionedType;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Detached structural copy of a workflow state returned by the workflow manager.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
final class DetachedWorkflowState implements WorkflowState {

    private final String workflowId;
    private final VersionedType workflowDefinitionId;
    private final Map<String, WorkflowStep> steps;
    private final List<String> stepNames;
    private final WorkflowStatus workflowStatus;
    private final Map<String, @Nullable Object> payload;
    private final Map<String, String> versionMigrations;
    private final @Nullable RuntimeException terminalCause;

    DetachedWorkflowState(WorkflowState state) {
        workflowId = state.workflowId();
        workflowDefinitionId = state.workflowDefinitionId();
        steps = new HashMap<>();
        for (String stepName : state.workflowStepNames()) {
            steps.put(stepName, Objects.requireNonNull(state.getStep(stepName)));
        }
        stepNames = steps.values().stream()
                         .sorted(Comparator.comparing(WorkflowStep::timestamp))
                         .map(WorkflowStep::stepName)
                         .toList();
        workflowStatus = state.workflowStatus();
        payload = Collections.unmodifiableMap(new LinkedHashMap<>(state.payload()));
        versionMigrations = Map.copyOf(state.versionMigrations());
        terminalCause = terminalCause(state);
    }

    @Nullable
    private static RuntimeException terminalCause(WorkflowState state) {
        if (!state.workflowStatus().isTerminal()) {
            return null;
        }
        try {
            state.throwTerminalCause();
            return null;
        } catch (RuntimeException cause) {
            return cause;
        }
    }

    @Override
    public String workflowId() {
        return workflowId;
    }

    @Override
    public VersionedType workflowDefinitionId() {
        return workflowDefinitionId;
    }

    @Override
    public List<String> workflowStepNames() {
        return stepNames;
    }

    @Override
    @Nullable
    public WorkflowStep getStep(String stepName) {
        return steps.get(stepName);
    }

    @Override
    public boolean containsStep(String stepName) {
        return steps.containsKey(stepName);
    }

    @Override
    public WorkflowStatus workflowStatus() {
        return workflowStatus;
    }

    @Override
    public Map<String, @Nullable Object> payload() {
        return payload;
    }

    @Override
    public String versionFor(String changeId) {
        return versionMigrations.getOrDefault(changeId, workflowDefinitionId.version());
    }

    @Override
    public boolean hasVersionMigrationStep(String changeId) {
        return versionMigrations.containsKey(changeId);
    }

    @Override
    public Map<String, String> versionMigrations() {
        return versionMigrations;
    }

    @Override
    public void throwTerminalCause() {
        if (terminalCause != null) {
            throw terminalCause;
        }
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("status", workflowStatus);
        if (terminalCause != null) {
            descriptor.describeProperty("terminationCause", terminalCause.getMessage());
        }
        descriptor.describeProperty("steps", stepNames);
        descriptor.describeProperty("payload", payload);
        descriptor.describeProperty("versions", versionMigrations);
    }
}
