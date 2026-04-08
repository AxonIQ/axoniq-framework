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

package org.axonframework.messaging.eventhandling.tracing;

import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.tracing.Span;

/**
 * Span factory that creates spans for the {@link EventBus}. You can customize the spans of the bus by creating your
 * own implementation.
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public interface EventBusSpanFactory {

    /**
     * Creates a span for the publishing of an event. This span is created when the event is published on the {@link
     * EventBus}. This span does not include the actual commit of the event, this is represented by the {@link
     * #createCommitEventsSpan()} and may occur later.
     *
     * @param eventMessage The event message to create a span for.
     * @return The created span.
     */
    Span createPublishEventSpan(EventMessage eventMessage);

    /**
     * Creates a span for the committing of events. This is usually batched and done in the commit phase of a UnitOfWork.
     * If no UnitOfWork is active, the commit is done immediately.
     * @return The created span.
     */
    Span createCommitEventsSpan();

    /**
     * Propagates the context of the current span to the given event message.
     *
     * @param eventMessage The event message to propagate the context to.
     * @return The event message with the propagated context.
     */
    EventMessage propagateContext(EventMessage eventMessage);
}
