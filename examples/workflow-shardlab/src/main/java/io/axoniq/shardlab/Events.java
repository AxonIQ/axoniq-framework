package io.axoniq.shardlab;

/**
 * Business events driving the lab workload.
 */
public final class Events {

    public record OrderPlaced(String orderId) {

    }

    public record PaymentReceived(String orderId) {

    }

    public record SlowJobRequested(String jobId, int seconds) {

    }

    public record FlakyJobRequested(String jobId, int failures) {

    }

    private Events() {
    }
}
