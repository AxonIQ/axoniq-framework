package io.axoniq.workflow.runtime.step;

import io.axoniq.workflow.runtime.definition.Result;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Completed implements StepState {
    @Override
    public CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher) {
        return CompletableFuture.completedFuture(Result.completed());
    }

    @Override
    public StepState apply(EventMessage eventMessage) {
        return this;
    }

}
