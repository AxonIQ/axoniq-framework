package io.axoniq.workflow.runtime.step;

import io.axoniq.workflow.runtime.definition.Result;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.slf4j.LoggerFactory.getLogger;

public class WaitStep implements StepState {

    private static final Logger logger = getLogger(WaitStep.class);

    private final String stepId;
    private final Duration duration;
    private final StepState next;

    private final Instant startTime;

    private WaitStep(String stepId, Duration duration, Instant startTime, StepState next) {
        this.stepId = stepId;
        this.duration = duration;
        this.startTime = startTime;
        this.next = next;
    }

    public WaitStep(String stepId, Duration duration) {
        this(stepId, duration, Instant.now(), new Completed());
    }

    public WaitStep(String stepId, Duration duration, StepState next) {
        this.stepId = stepId;
        this.duration = duration;
        this.startTime = null;
        this.next = next;
    }

    @Override
    public CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher) {
        if (startTime == null) {
            logger.info("[{}]: Unknown waiting time, there hasn't been a start trigger yet. Should generally not happen", stepId);
            return CompletableFuture.completedFuture(Result.suspend(duration.toMillis()));
        }
        Instant now = Instant.now();
        if (now.isAfter(startTime.plus(duration))) {
            logger.info("[{}]: Waiting time expired, moving on to next step", stepId);
            return next.execute(eventPublisher);
        } else {
            long millis = Duration.between(now, startTime.plus(duration)).toMillis();
            logger.info("[{}]: Wait not finished. Need {}ms more", stepId, millis);
            return CompletableFuture.completedFuture(Result.suspend(millis));
        }
    }

    @Override
    public StepState apply(EventMessage eventMessage) {
        if (startTime == null) {
            return new WaitStep(stepId, duration, eventMessage.timestamp(), next);
        }
        // we've started. The events may be for our downstream task
        var applied = next.apply(eventMessage);
        if (applied != next) {
            // it triggered a state change, so we assume we've finished waiting
            return applied;
        }
        return this;
    }
}
