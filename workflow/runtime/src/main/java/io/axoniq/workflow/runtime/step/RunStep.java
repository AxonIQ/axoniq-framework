package io.axoniq.workflow.runtime.step;

import io.axoniq.workflow.runtime.definition.Result;
import io.axoniq.workflow.runtime.event.StepCompleted;
import io.axoniq.workflow.runtime.event.StepStarted;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class RunStep implements StepState {

    private final String stepId;
    private final Runnable action;
    private final StepState next;

    private final Instant startTime;

    public RunStep(String stepId, Runnable action, StepState next, Instant startTime) {
        this.stepId = stepId;
        this.action = action;
        this.next = next;
        this.startTime = startTime;
    }

    public RunStep(String stepId, Runnable action) {
        this(stepId, action, new Completed());
    }

    public RunStep(String stepId, Runnable action, StepState next) {
        this.stepId = stepId;
        this.action = action;
        this.next = next;
        this.startTime = null;
    }

    @Override
    public CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher) {
        if (startTime != null) {
            // we've already started, so an execute should wait for that to finish
            return CompletableFuture.completedFuture(Result.suspend());
        }
        return eventPublisher.apply(List.of(new GenericEventMessage(MessageType.fromString("io.axoniq.workflow.StepStarted#0.1"), new StepStarted(stepId))))
                             .thenRunAsync(action)
                             .thenCombineAsync(eventPublisher.apply(List.of(new GenericEventMessage(MessageType.fromString("io.axoniq.workflow.StepCompleted#0.1"), new StepCompleted(stepId, null)))), (unused, unused2) -> null)
                             .thenApply(r -> Result.completed())
                             .exceptionally(Result::error);
    }

    @Override
    public StepState apply(EventMessage eventMessage) {
        return switch (eventMessage.type().name()) {
            case "io.axoniq.workflow.StepCompleted" -> {
                // TODO - Check if completion is this step
                StepCompleted stepCompleted = eventMessage.payloadAs(StepCompleted.class);
                if (stepCompleted != null && stepId.equals(stepCompleted.stepId()))
                    yield next.apply(eventMessage);
                else
                    yield this;
            }
            case "io.axoniq.workflow.StepStarted" -> {
                StepStarted stepStarted = eventMessage.payloadAs(StepStarted.class);
                if (stepStarted != null && stepId.equals(stepStarted.stepId()))
                    yield new RunStep(stepId, action, next, eventMessage.timestamp());
                else
                    yield this;
            }
            default -> this;
        };
    }

}
