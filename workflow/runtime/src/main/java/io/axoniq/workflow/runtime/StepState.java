package io.axoniq.workflow.runtime;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface StepState {

    // TODO - Add ExecutionContext as parameter so steps can propagate information to eachother
    CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher);

    // TODO - ExecutionContext should be part of StepState and modified as part of transitions
    StepState apply(EventMessage eventMessage);

}
