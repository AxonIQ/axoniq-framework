package io.axoniq.workflow.runtime.___stash;

/**
 * Handle to manage an event streaming.
 * Allows cancellation of subscriptions and checking their status.
 */
public interface Subscription {

    /**
     * Cancel this streaming, preventing any future event notifications.
     */
    void cancel();

    /**
     * Check if this streaming is still active.
     *
     * @return true if the streaming is active and will receive events, false otherwise
     */
    boolean isActive();
}
