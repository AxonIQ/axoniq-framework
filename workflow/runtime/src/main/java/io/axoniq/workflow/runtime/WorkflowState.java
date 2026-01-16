package io.axoniq.workflow.runtime;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface WorkflowState {

    CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher);

    WorkflowState apply(EventMessage eventMessage);

}
