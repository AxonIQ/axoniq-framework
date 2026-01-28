package io.axoniq.workflow.runtime.___stash;

import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Callback interface for event notifications in the streaming model.
 */
@FunctionalInterface
public interface EventListener {

    /**
     * Called when a matching event is received.
     *
     * @param event the event message that matched the streaming criteria
     * @return true to unsubscribe (remove listener after this event), false to keep listening
     */
    boolean onEvent(EventMessage event);
}
