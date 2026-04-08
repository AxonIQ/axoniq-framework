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

package org.axonframework.modelling.saga;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.tracing.Span;

/**
 * Span factory that creates spans for the {@link AbstractSagaManager}. You can customize the spans of the bus by
 * creating your own implementation.
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public interface SagaManagerSpanFactory {

    /**
     * Creates a span that represents the creation of a new saga instance.
     *
     * @param event          The event that triggered the creation of the saga.
     * @param sagaType       The type of the saga.
     * @param sagaIdentifier The identifier of the saga.
     * @return The created span.
     */
    Span createCreateSagaInstanceSpan(EventMessage event, Class<?> sagaType, String sagaIdentifier);

    /**
     * Creates a span that represents the invocation of a saga.
     *
     * @param event    The event that triggered the invocation of the saga.
     * @param sagaType The type of the saga.
     * @param saga     The saga that will be invoked.
     * @return The created span.
     */
    Span createInvokeSagaSpan(EventMessage event, Class<?> sagaType, Saga<?> saga);
}
