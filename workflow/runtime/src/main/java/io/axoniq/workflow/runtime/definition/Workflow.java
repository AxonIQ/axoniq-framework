package io.axoniq.workflow.runtime.definition;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface Workflow {

    String getWorkflowId();

    CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher);

    Workflow apply(EventMessage eventMessage);
}
