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
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;

import java.util.List;
import java.util.Objects;

/**
 * Describes the context of an error caught by an {@link EventProcessor}
 * while processing a batch of events.
 *
 * @param eventProcessor The name of the {@link EventProcessor} that failed
 *                       to process the given {@code failedEvents}.
 * @param error          The error that was raised during processing of the {@code failedEvents}.
 * @param failedEvents   The list of events that triggered the error.
 * @param context        The {@code Context} carrying the resources that applied for the given {@code failedEvents} when
 *                       they were handled.
 * @author Allard Buijze
 * @since 3.0.0
 */
public record ErrorContext(
        String eventProcessor,
        Throwable error,
        List<? extends EventMessage> failedEvents,
        Context context
) {

    /**
     * Compact constructor of the {@code ErrorContext} to validate the {@code eventProcessor}, {@code error},
     * {@code failedEvents}, and {@code context} are not null.
     */
    @SuppressWarnings("MissingJavadoc")
    public ErrorContext {
        Objects.requireNonNull(eventProcessor, "The event processor must not be null.");
        Objects.requireNonNull(error, "The error must not be null.");
        Objects.requireNonNull(failedEvents, "The failed events must not be null.");
        Objects.requireNonNull(context, "The context must not be null.");
    }
}
