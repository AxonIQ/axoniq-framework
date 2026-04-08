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

import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Interface of the error handler that will be invoked if event processing fails.
 * <p>
 * The error handler is generally invoked by an {@link EventProcessor} when
 * the {@link ProcessingContext} created to coordinate the event processing was
 * rolled back.
 *
 * @author Rene de Waele
 * @since 3.0.0
 */
@FunctionalInterface
public interface ErrorHandler {

    /**
     * Handle an error raised during event processing.
     * <p>
     * Generally this means that the {@link ProcessingContext} created to
     * coordinate the event processing was rolled back. Using default configuration the {@code ErrorHandler} is only
     * invoked when there is a serious error, for instance when the database transaction connected to the
     * {@code ProcessingContext} can not be committed.
     * <p>
     * The error handler has the option to simply log or ignore the error. Depending on the type of
     * {@code EventProcessor} this will put an end to the processing of any further events (in case of a
     * {@link PooledStreamingEventProcessor}) or simply skip over the list of {@code failedEvents} in the given
     * {@code errorContext}.
     * <p>
     * Note that although the {@code ProcessingContext} and hence any related database transactions have been rolled
     * back when the error handler is invoked, the processing of one or more of the {@link ErrorContext#failedEvents()}
     * may in fact have caused other side effects which could not be reverted.
     *
     * @param errorContext Contextual information describing the error.
     * @throws Exception If this handler decides to propagate the error.
     */
    void handleError(ErrorContext errorContext) throws Exception;
}
