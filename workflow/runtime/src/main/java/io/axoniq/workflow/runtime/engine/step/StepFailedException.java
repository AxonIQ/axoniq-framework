package io.axoniq.workflow.runtime.engine.step;

import jakarta.annotation.Nonnull;

public class StepFailedException extends RuntimeException {

    public StepFailedException(@Nonnull Throwable cause) {
        super(cause);
    }

    public StepFailedException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }
}
