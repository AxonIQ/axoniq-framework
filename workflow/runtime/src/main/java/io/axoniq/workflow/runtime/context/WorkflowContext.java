package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepFailed;
import io.axoniq.workflow.runtime.event.StepStarted;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class WorkflowContext {

    private final String workflowId;
    public final Map<String, StepExecution> steps = new HashMap<>();
    private final StateManager stateManager;

    public WorkflowContext(String workflowId, StateManager stateManager) {
        this.workflowId = workflowId;
        this.stateManager = stateManager;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public <T> T run(String stepName, Class<T> returnType, Supplier<T> action) {
        StepExecution existing = steps.get(stepName);
        if (existing != null) {
            switch (existing.status()) {
                case COMPLETED -> {
                    return returnType.cast(existing.result());
                }
                case FAILED -> throw new StepFailedException(existing.error());
                case IN_PROGRESS -> {
                    // Fall through to re-run the step
                }
            }
        }

        stateManager.append(workflowId, new GenericEventMessage(MessageType.fromString(StepStarted.ID), new StepStarted(stepName)));
        steps.put(stepName, StepExecution.inProgress(stepName));

        try {
            T result = action.get();
            stateManager.append(workflowId, new GenericEventMessage(MessageType.fromString(StepCompleted.ID), new StepCompleted(stepName, result)));
            steps.put(stepName, StepExecution.completed(stepName, result));
            return result;
        } catch (Throwable e) {
            stateManager.append(workflowId, new GenericEventMessage(MessageType.fromString(StepFailed.ID), new StepFailed(stepName, e.getMessage(), e)));
            steps.put(stepName, StepExecution.failed(stepName, e));
            throw e;
        }
    }

    public void run(String stepName, Runnable action) {
        run(stepName, Void.class, () -> {
            action.run();
            return null;
        });
    }

    public void restoreStep(StepExecution step) {
        steps.put(step.stepName(), step);
    }
}
