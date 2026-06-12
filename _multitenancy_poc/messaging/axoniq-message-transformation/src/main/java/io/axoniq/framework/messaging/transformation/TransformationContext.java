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

package io.axoniq.framework.messaging.transformation;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Immutable per-invocation context provided to a {@link MessageTransformer}.
 * <p>
 * Contains the services required to interpret and convert the input message during transformation.
 *
 * @param entryContext        the read-stream entry context associated with the source message, never {@code null}
 * @param processingContext   optional processing context, can be {@code null}
 * @param converter           a message converter, never {@code null}
 * @param messageTypeResolver resolves message types for transformed output validation, never {@code null}
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
public record TransformationContext(
        Context entryContext,
        @Nullable ProcessingContext processingContext,
        MessageConverter converter,
        MessageTypeResolver messageTypeResolver) {

    public TransformationContext {
        requireNonNull(entryContext, "entryContext");
        requireNonNull(converter, "converter");
        requireNonNull(messageTypeResolver, "messageTypeResolver");
    }
}
