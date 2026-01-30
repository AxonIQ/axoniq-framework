package io.axoniq.workflow.runtime.___stash;

/**
 * Handle to manage an event support.
 * Allows cancellation of subscriptions and checking their status.
 */
public interface Subscription {

    /**
     * Cancel this support, preventing any future event notifications.
     */
    void cancel();

    /**
     * Check if this support is still active.
     *
     * @return true if the support is active and will receive events, false otherwise
     */
    boolean isActive();
}
