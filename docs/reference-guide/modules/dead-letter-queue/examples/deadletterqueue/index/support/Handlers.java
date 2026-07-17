package deadletterqueue.index.support;

import org.axonframework.messaging.eventhandling.EventHandlingComponent;

/**
 * Supporting stub for the dead-letter-queue documentation samples. Provides the {@code myHandlerComponent}
 * referenced by the "enable" and "custom factory" configuration snippets, so those snippets stay focused on
 * the configuration API rather than on where the handler component comes from.
 */
public final class Handlers {

    public static final EventHandlingComponent myHandlerComponent = null;

    private Handlers() {
    }
}
