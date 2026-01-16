package io.axoniq.workflow.runtime;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class OnTimeoutStep implements StepState {

    private final StepState next;
    private final Instant started;
    private final StepState onTimeout;

    private final Duration timeout;

    public OnTimeoutStep(StepState next, Duration timeout, StepState onTimeout) {
        this.next = next;
        this.timeout = timeout;
        this.onTimeout = onTimeout;
        this.started = null;
    }

    private OnTimeoutStep(StepState next, Instant started, StepState onTimeout, Duration timeout) {
        this.next = next;
        this.started = started;
        this.onTimeout = onTimeout;
        this.timeout = timeout;
    }

    @Override
    public CompletableFuture<Result> execute(Function<List<EventMessage>, CompletableFuture<Void>> eventPublisher) {
        if (started != null && Duration.between(started.plus(timeout), Instant.now()).isPositive()) {
            // timeout happened
            return next.execute(eventPublisher).thenCompose(result -> {
                if (result.isCompleted()) {
                    // the task completed, so we're good.
                    return CompletableFuture.completedFuture(result);
                } else {
                    return onTimeout.execute(eventPublisher);
                }
            });
        }
        return next.execute(eventPublisher).thenApply(result -> new Result() {
            @Override
            public boolean isCompleted() {
                return result.isCompleted();
            }

            @Override
            public long timeout() {
                // make sure we don't suspend longer than the timeout of this step
                if (started != null) {
                    return Math.min(result.timeout(), Duration.between(Instant.now(), started.plus(timeout)).toMillis());
                } else {
                    return result.timeout();
                }
            }

            @Override
            public Optional<Throwable> error() {
                return result.error();
            }
        });
    }

    @Override
    public StepState apply(EventMessage eventMessage) {
        StepState newNext = next.apply(eventMessage);
        if (!newNext.equals(next)) {
            // next changed state, so we've definitely started
            return new OnTimeoutStep(newNext, started == null ? eventMessage.timestamp() : started, onTimeout, timeout);
        } else {
            StepState newOnTimeout = onTimeout.apply(eventMessage);
            if (newOnTimeout != onTimeout) {
                if (started == null || Duration.between(started.plus(timeout), Instant.now()).isPositive()) {
                    // we're not ready to timeout yet.
                    return new OnTimeoutStep(newOnTimeout, started, newOnTimeout, timeout);
                }
                // we timed out, so the onTimeoutTask is now the active one
                return newOnTimeout;

            }
        }
        return this;
    }
}
