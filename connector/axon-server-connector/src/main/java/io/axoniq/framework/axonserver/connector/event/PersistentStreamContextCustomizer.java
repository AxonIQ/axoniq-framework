/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.axonserver.connector.event;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.function.Function;

/**
 * Places resources on the {@link ProcessingContext} that a {@link PersistentStreamConnection} spans over a batch of
 * events, returning the context the events of that batch are consumed with.
 * <p>
 * Invoked exactly once per batch, so it carries resources that are constant for every event a connection delivers,
 * rather than per-event information such as the
 * {@link org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken}.
 * <p>
 * Returning the context lets an implementation branch off the one it is given, through
 * {@link ProcessingContext#withResource(org.axonframework.messaging.core.Context.ResourceKey, Object)}.
 * <p>
 * Implementations must be thread-safe: the batches of the segments of one stream are processed concurrently, so the
 * same customizer is invoked from several threads, each with its own {@link ProcessingContext}.
 * <p>
 * Marked Internal, because it is intended as an internal extension point not a general-purpose interception point.
 * Use a {@link org.axonframework.messaging.core.MessageHandlerInterceptor} to influence event handling.
 *
 * @author Jakob Hatzl
 * @see PersistentStreamConnection
 * @since 5.3.0
 */
@Internal
@FunctionalInterface
public interface PersistentStreamContextCustomizer extends Function<ProcessingContext, ProcessingContext> {

    /**
     * A {@code PersistentStreamContextCustomizer} placing no resources on the {@link ProcessingContext}, handing back
     * the context it was given, used when a connection needs no batch-level resources of its own.
     */
    PersistentStreamContextCustomizer NO_OP = processingContext -> processingContext;

    /**
     * Places resources on the given {@code processingContext}, which spans a single batch of events, and returns the
     * context the events of that batch are consumed with.
     *
     * @param processingContext the batch-scoped processing context to place resources on
     * @return the context to consume the batch with, either the given {@code processingContext} or one branched off it
     */
    @Override
    ProcessingContext apply(ProcessingContext processingContext);
}
