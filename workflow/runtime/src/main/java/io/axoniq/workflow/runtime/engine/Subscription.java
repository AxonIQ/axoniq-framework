package io.axoniq.workflow.runtime.engine;

/**
 * Handle to manage an event subscription.
 * Allows cancellation of subscriptions and checking their status.
 */
public interface Subscription {

    /**
     * Cancel this subscription, preventing any future event notifications.
     */
    void cancel();

    /**
     * Check if this subscription is still active.
     *
     * @return true if the subscription is active and will receive events, false otherwise
     */
    boolean isActive();
}
