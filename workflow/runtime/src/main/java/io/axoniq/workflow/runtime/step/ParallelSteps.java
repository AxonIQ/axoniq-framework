package io.axoniq.workflow.runtime.step;

import io.axoniq.workflow.runtime.definition.Result;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ParallelSteps implements StepState {

    private final Set<StepState> steps = ConcurrentHashMap.newKeySet();
    private final String stepId;
    private final StepState whenAllComplete;

    public ParallelSteps(String stepId, Set<StepState> steps, StepState whenAllComplete) {
        this.stepId = stepId;
        this.whenAllComplete = whenAllComplete;
        this.steps.addAll(steps);
    }

    @Override
    public CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher) {
        return steps.stream().map(step -> step.execute(eventPublisher))
                    .reduce(CompletableFuture.completedFuture(Result.completed()), (one, two) -> one.thenCombine(two, (r1, r2) -> new Result() {
                        @Override
                        public boolean isCompleted() {
                            return r1.isCompleted() && r2.isCompleted();
                        }

                        @Override
                        public long timeout() {
                            return r1.isCompleted() ? r2.timeout() : r2.isCompleted() ? r1.timeout() : Long.min(r1.timeout(), r2.timeout());
                        }

                        @Override
                        public Optional<Throwable> error() {
                            return r1.error().or(r2::error);
                        }
                    })).thenCompose(finalResult -> {
                    if (finalResult.isCompleted()) {
                        return whenAllComplete.execute(eventPublisher);
                    } else {
                        return CompletableFuture.completedFuture(finalResult);
                    }
                });
    }

    @Override
    public StepState apply(EventMessage eventMessage) {
        return newStepsWithApplied(eventMessage);
    }

    @Nonnull
    private ParallelSteps newStepsWithApplied(EventMessage eventMessage) {
        Set<StepState> newStates = new HashSet<>(steps.size());
        steps.forEach(value -> newStates.add(value.apply(eventMessage)));
        return new ParallelSteps(stepId, newStates, whenAllComplete);
    }
}
