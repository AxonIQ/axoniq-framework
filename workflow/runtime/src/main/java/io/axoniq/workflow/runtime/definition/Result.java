package io.axoniq.workflow.runtime.definition;

import java.util.Optional;

public interface Result {

    static Result error(Throwable e) {
        return new Result() {
            @Override
            public boolean isCompleted() {
                return true;
            }

            @Override
            public long timeout() {
                return 0;
            }

            @Override
            public Optional<Throwable> error() {
                return Optional.of(e);
            }
        };
    }

    static Result suspend() {
        return suspend(Integer.MAX_VALUE);
    }

    static Result suspend(long durationMs) {
        return new Result() {
            @Override
            public boolean isCompleted() {
                return false;
            }

            @Override
            public long timeout() {
                return durationMs;
            }

            @Override
            public Optional<Throwable> error() {
                return Optional.empty();
            }
        };
    }

    static Result completed() {
        return new Result() {
            @Override
            public boolean isCompleted() {
                return true;
            }

            @Override
            public long timeout() {
                return 0;
            }

            @Override
            public Optional<Throwable> error() {
                return Optional.empty();
            }
        };
    }

    boolean isCompleted();

    long timeout();

    Optional<Throwable> error();

}
