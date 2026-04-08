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

import java.util.concurrent.CompletableFuture;

import org.axonframework.common.Priority;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.annotation.AbstractAnnotatedParameterResolverFactory;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * An extension of the AbstractAnnotatedParameterResolverFactory that accepts parameters of a {@link Long} type
 * annotated with the {@link SequenceNumber} annotation and assigns the sequenceNumber of the DomainEventMessage.
 * <p/>
 * Primitive long parameters are also supported.
 *
 * @author Mark Ingram
 * @since 2.1.0
 */
@Priority(Priority.HIGH)
public final class SequenceNumberParameterResolverFactory extends
        AbstractAnnotatedParameterResolverFactory<SequenceNumber, Long> {

    private final ParameterResolver<Long> resolver;

    /**
     * Initializes a {@link ParameterResolverFactory} for {@link SequenceNumber} annotated parameters
     */
    public SequenceNumberParameterResolverFactory() {
        super(SequenceNumber.class, Long.class);
        resolver = new SequenceNumberParameterResolver();
    }

    @Override
    protected ParameterResolver<Long> getResolver() {
        return resolver;
    }

    /**
     * ParameterResolver that resolves SequenceNumber parameters
     */
    public static class SequenceNumberParameterResolver implements ParameterResolver<Long> {

        @Override
        public CompletableFuture<Long> resolveParameterValue(ProcessingContext context) {
            return CompletableFuture.completedFuture(context.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY));
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return context.containsResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY);
        }
    }
}
