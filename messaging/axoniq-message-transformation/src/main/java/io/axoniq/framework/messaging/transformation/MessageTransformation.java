/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
