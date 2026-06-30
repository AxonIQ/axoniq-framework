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
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;

/**
 * Transforms messages of type {@code M}.
 * <p>
 * A transformation accepts a {@link Message} of type {@code M} and produces zero or more transformed messages of the
 * same type.
 *
 * @param <M> the message type accepted as input and emitted as output
 * @author Laura Devriendt
 * @since 5.2.0
 */
public interface MessageTransformation<M extends Message> {

    /**
     * Transforms a message into zero or more messages of the same type. If the transformation fails, the returned
     * stream completes with the error.
     *
     * @param message the input message, cannot be {@code null}
     * @param context the transformation context, cannot be {@code null}
     * @return a stream of transformed messages, or a stream completed with an error if the transformation failed,
     * never {@code null}
     * @throws NullPointerException if {@code message} or {@code context} is {@code null}
     */
    @Internal
    MessageStream<? extends M> transform(M message, TransformationContext context);
}
