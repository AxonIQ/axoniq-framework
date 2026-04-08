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

package org.axonframework.messaging.eventhandling.annotation;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.Priority;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.AbstractAnnotatedParameterResolverFactory;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * AbstractAnnotatedParameterResolverFactory that accepts parameters with type {@link Instant} that are annotated
 * with the {@link Timestamp} annotation and assigns the timestamp of the EventMessage.
 *
 * @author Allard Buijze
 * @since 2.0
 */
@Priority(Priority.HIGH)
public final class TimestampParameterResolverFactory
        extends AbstractAnnotatedParameterResolverFactory<Timestamp, Instant> {

    private final ParameterResolver<Instant> resolver;

    /**
     * Initializes a {@link ParameterResolverFactory} for {@link Timestamp}
     * annotated parameters
     */
    public TimestampParameterResolverFactory() {
        super(Timestamp.class, Instant.class);
        resolver = new TimestampParameterResolver();
    }

    @Override
    protected ParameterResolver<Instant> getResolver() {
        return resolver;
    }

    /**
     * ParameterResolver that resolved Timestamp parameters
     */
    static class TimestampParameterResolver implements ParameterResolver<Instant> {

        @Override
        public CompletableFuture<Instant> resolveParameterValue(ProcessingContext context) {
            if (Message.fromContext(context) instanceof EventMessage eventMessage) {
                return CompletableFuture.completedFuture(eventMessage.timestamp());
            }
            return FutureUtils.emptyCompletedFuture();
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return Message.fromContext(context) instanceof EventMessage;
        }
    }
}
