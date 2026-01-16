package io.axoniq.workflow.runtime;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class SimpleWorkflow implements WorkflowState {

    private final String workflowId;
    private final StepState currentState;

    public SimpleWorkflow(String workflowId, StepState initialState) {
        this.workflowId = workflowId;
        this.currentState = initialState;
    }

    @Override
    public CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher) {
        return currentState.execute(eventPublisher);
    }

    @Override
    public WorkflowState apply(EventMessage eventMessage) {
        return new SimpleWorkflow(workflowId, currentState.apply(eventMessage));
    }
}
