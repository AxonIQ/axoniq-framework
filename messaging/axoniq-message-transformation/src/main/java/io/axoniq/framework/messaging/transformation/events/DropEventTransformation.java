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

package io.axoniq.framework.messaging.transformation.events;

import io.axoniq.framework.messaging.transformation.FromMatcher;
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;

import static java.util.Objects.requireNonNull;

/**
 * A 1:0 drop {@link EventTransformation}: it suppresses an event matched by exact identity {@code source}, declaring
 * no {@code to} and producing no output. The chain removes a matched event by short-circuiting on this variant; a
 * direct {@link #transform(EventMessage, TransformationContext)} call yields an empty stream. A drop converts no
 * payload and resolves no identity, so neither the
 * {@link org.axonframework.messaging.core.conversion.MessageConverter} nor the
 * {@link org.axonframework.messaging.core.MessageTypeResolver} is consulted.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class DropEventTransformation implements EventTransformation {

    private final MessageType source;
    private final FromMatcher matcher;

    /**
     * Constructs a drop of events matching {@code source}.
     *
     * @param source the {@code from} identity matched by exact equality
     */
    DropEventTransformation(MessageType source) {
        this.source = requireNonNull(source, "source may not be null");
        this.matcher = new FromMatcher.Exact(source);
    }

    /**
     * Produces no output: the matched event is suppressed. The empty stream satisfies a direct SPI call; within the
     * chain a drop is short-circuited and this is not invoked.
     *
     * @param message the matched input message
     * @param context the per-message {@link TransformationContext}; required by the contract but otherwise unused
     * @return an empty stream
     */
    @Override
    public MessageStream<EventMessage> transform(EventMessage message, TransformationContext context) {
        return MessageStream.empty();
    }

    @Override
    public FromMatcher matcher() {
        return matcher;
    }

    @Override
    public String toString() {
        return "DropEventTransformation{from=" + source + ", to=(dropped)}";
    }
}
