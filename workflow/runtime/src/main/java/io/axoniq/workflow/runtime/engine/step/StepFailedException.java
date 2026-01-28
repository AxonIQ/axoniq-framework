package io.axoniq.workflow.runtime.engine.step;

public class StepFailedException extends RuntimeException {

    public StepFailedException(Throwable cause) {
        super(cause);
    }

    public StepFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
