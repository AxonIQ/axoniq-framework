/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.eventhandling.processing.errorhandling;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventMessageHandler;

/**
 * Interface of an error handler that is invoked when an exception is triggered as result of an {@link
 * EventMessageHandler} handling an event.
 *
 * @author Rene de Waele
 */
public interface ListenerInvocationErrorHandler {

    /**
     * Invoked after given {@code eventListener} failed to handle given {@code event}. Implementations have a
     * choice of options for how to continue:
     * <p>
     * <ul> <li>To ignore this error no special action is required. Processing will continue for this and subsequent
     * events.</li> <li>To retry processing the event, implementations can re-invoke {@link
     * EventMessageHandler#handleSync(EventMessage)} on the eventListener once or multiple times.</li> <li>To terminate event
     * handling altogether and stop propagating the event to other listeners implementations may throw an
     * exception.</li></ul>
     *
     * @param exception     The exception thrown by the given eventListener
     * @param event         The event that triggered the exception
     * @param eventHandler The listener that failed to handle given event
     * @throws Exception To stop further handling of the event
     */
    void onError(Exception exception, EventMessage event,
                 EventMessageHandler eventHandler) throws Exception;

}
