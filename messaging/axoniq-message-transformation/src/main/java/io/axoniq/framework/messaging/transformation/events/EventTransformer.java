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

import io.axoniq.framework.messaging.transformation.MessageTransformer;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

/**
 * Event-specific {@link MessageTransformer}. Use the {@code EventTransformation} factory
 * rather than implementing directly. See {@link MessageTransformer} for the transformation
 * contract; this interface narrows the message type to {@link EventMessage}.
 * <p>
 * A transformer rewrites only the {@link EventMessage} itself -- its
 * {@link org.axonframework.messaging.core.MessageType}, payload, and metadata. Properties
 * that live alongside the event in the read stream -- the tags resolved at append time
 * and any tracking-position or sequence information carried on the
 * {@link MessageStream.Entry} context -- are not part of the message and are not
 * modified by the chain. For 1:N splits, every output is delivered against the same
 * stream position as the input.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@FunctionalInterface
public interface EventTransformer extends MessageTransformer<EventMessage> {

    @Override
    MessageStream<? extends EventMessage> transform(EventMessage message,
                                                    @Nullable ProcessingContext context);
}
