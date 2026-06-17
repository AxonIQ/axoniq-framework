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
 * Immutable per-invocation context handed to a {@link MessageTransformation} when it transforms a
 * single message.
 * <p>
 * It groups two contexts with distinct roles. {@code entryContext} is the {@link Context} attached
 * to the source message. It is always present and, where the source provides one, carries the
 * message's position for use in diagnostics. {@code processingContext} is
 * the active unit of work for the transformation. It is present when the transformation runs inside
 * a unit of work, such as an entity load, and {@code null} when none is active, such as while an
 * event stream is being opened.
 * <p>
 * {@code converter} and {@code messageTypeResolver} are held as explicit fields rather than read
 * from {@code processingContext}, because that context is not always present.
 *
 * @param entryContext        the {@link Context} attached to the source message, carrying its position for use in diagnostics, never {@code null}
 * @param processingContext   the active unit of work for this transformation, or {@code null} when none is active
 * @param converter           the converter used to convert the input payload to the transformation's input type, never {@code null}
 * @param messageTypeResolver the resolver used to validate the transformed output's message type, never {@code null}
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
        requireNonNull(entryContext, "entryContext may not be null");
        requireNonNull(converter, "converter may not be null");
        requireNonNull(messageTypeResolver, "messageTypeResolver may not be null");
    }
}
