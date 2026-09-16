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
package io.axoniq.framework.workflow.simulation.harness;

import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.VersionedType;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A point-in-time copy of a {@link WorkflowState}, taken so a held history snapshot does not follow the projector.
 * <p>
 * The in-memory history repository stores the projector's own {@code EventSourcedWorkflowState}, which the projector
 * keeps evolving in place; a snapshot that merely keeps the reference sees every later event. This copy keeps the
 * structural content as it was: identity, status, step map in {@code workflowStepNames()} order, payload entries and
 * recorded version migrations. Payload values are kept by reference, as the manager's own detached state does.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
final class FrozenWorkflowState implements WorkflowState {

    private final String workflowId;
    private final VersionedType workflowDefinitionId;
    private final WorkflowStatus workflowStatus;
    private final Map<String, WorkflowStep> steps;
    private final List<String> stepNames;
    private final Map<String, @Nullable Object> payload;
    private final Map<String, String> versionMigrations;
    private final @Nullable RuntimeException terminalCause;

    FrozenWorkflowState(WorkflowState source) {
        this.workflowId = source.workflowId();
        this.workflowDefinitionId = source.workflowDefinitionId();
        this.workflowStatus = source.workflowStatus();
        var copiedSteps = new LinkedHashMap<String, WorkflowStep>();
        for (String stepName : source.workflowStepNames()) {
            var step = source.getStep(stepName);
            if (step != null) {
                copiedSteps.put(stepName, step);
            }
        }
        this.steps = copiedSteps;
        this.stepNames = List.copyOf(copiedSteps.keySet());
        this.payload = new LinkedHashMap<>(source.payload());
        this.versionMigrations = Map.copyOf(source.versionMigrations());
        RuntimeException cause = null;
        if (workflowStatus.isTerminal()) {
            try {
                source.throwTerminalCause();
            } catch (RuntimeException e) {
                cause = e;
            }
        }
        this.terminalCause = cause;
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
        descriptor.describeProperty("steps", stepNames);
        descriptor.describeProperty("payload", payload);
    }
}
