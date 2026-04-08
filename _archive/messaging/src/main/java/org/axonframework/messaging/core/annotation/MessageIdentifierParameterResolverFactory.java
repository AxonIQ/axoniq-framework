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

package org.axonframework.messaging.core.annotation;

import org.axonframework.common.Priority;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;

/**
 * An extension of the AbstractAnnotatedParameterResolverFactory that accepts parameters of a {@link String} type that
 * are annotated with the {@link MessageIdentifier} annotation and assigns the identifier of the Message.
 *
 * @author Steven van Beelen
 * @since 3.0.0
 */
@Priority(Priority.HIGH)
public final class MessageIdentifierParameterResolverFactory
        extends AbstractAnnotatedParameterResolverFactory<MessageIdentifier, String> {

    private final ParameterResolver<String> resolver;

    /**
     * Initialize a {@link ParameterResolverFactory} for {@link MessageIdentifier} annotated parameters.
     */
    public MessageIdentifierParameterResolverFactory() {
        super(MessageIdentifier.class, String.class);
        resolver = new MessageIdentifierParameterResolver();
    }

    @Override
    protected ParameterResolver<String> getResolver() {
        return resolver;
    }

    /**
     * ParameterResolver to resolve MessageIdentifier parameters
     */
    static class MessageIdentifierParameterResolver implements ParameterResolver<String> {

        @Override
        public CompletableFuture<String> resolveParameterValue(ProcessingContext context) {
            return CompletableFuture.completedFuture(Message.fromContext(context).identifier());
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return Message.fromContext(context) != null;
        }
    }
}
