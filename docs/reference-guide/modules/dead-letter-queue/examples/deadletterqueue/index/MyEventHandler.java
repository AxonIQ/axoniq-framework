/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package deadletterqueue.index;

import static deadletterqueue.index.support.Support.log;
import static deadletterqueue.index.support.Support.updateProjection;

// tag::detect-dead-letter[]
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;

class MyEventHandler {

    // SomeEvent is your application-defined event class.
    @EventHandler
    public void on(SomeEvent event, DeadLetter<EventMessage> deadLetter) {
        if (deadLetter != null) {
            // Retrying a dead letter: inspect why it was parked and how many times it was tried
            int retries = Integer.parseInt(deadLetter.diagnostics().getOrDefault("retries", "0"));
            deadLetter.cause().ifPresent(cause ->
                    log.warn("Retrying dead-lettered {} (attempt {}): {} - {}",
                             event.getClass().getSimpleName(),
                             retries + 1,
                             cause.type(),
                             cause.message()));
        }

        // The same handler logic runs for both initial processing and retries.
        // Ensure this method is idempotent.
        updateProjection(event);
    }
}
// end::detect-dead-letter[]
