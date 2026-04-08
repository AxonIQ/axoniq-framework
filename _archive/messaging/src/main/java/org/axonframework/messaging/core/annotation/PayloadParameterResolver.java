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

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of a {@link ParameterResolver} that resolves the Message payload as parameter in a handler method.
 *
 * @author Allard Buijze
 * @since 3.0.0
 */
public class PayloadParameterResolver implements ParameterResolver<Object> {

    private final Class<?> payloadType;

    /**
     * Initializes a new {@code PayloadParameterResolver} for a method parameter of given {@code payloadType}. This
     * parameter resolver matches with a message if the payload of the message is assignable to the given
     * {@code payloadType}.
     *
     * @param payloadType the parameter type
     */
    public PayloadParameterResolver(Class<?> payloadType) {
        this.payloadType = payloadType;
    }

    @Override
    public CompletableFuture<Object> resolveParameterValue(ProcessingContext context) {
        return CompletableFuture.completedFuture(Message.fromContext(context).payload());
    }

    @Override
    public boolean matches(ProcessingContext context) {
        return Optional.ofNullable(Message.fromContext(context))
                       .map(Message::payloadType)
                       .map(payloadType::isAssignableFrom)
                       .orElse(false);
    }

    @Override
    public Class<?> supportedPayloadType() {
        return payloadType;
    }
}
