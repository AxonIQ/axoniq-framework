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
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Event-specific {@link MessageTransformer}: a sealed handle for a single registered event
 * transformation, produced exclusively by the {@code EventTransformation} factory and
 * registered with an {@code EventTransformerChain}. Sealing makes the factory the only source
 * of instances, so the chain always receives a routable transformer carrying its {@code from}
 * and {@code to} metadata; raw lambdas cannot be supplied.
 * <p>
 * A transformation rewrites only the {@link EventMessage} itself: its
 * {@link org.axonframework.messaging.core.MessageType}, payload, and metadata. Tags are fixed
 * when the event is appended and are not changed by the chain. Tracking position and sequence
 * information ride on the read-stream entry context rather than on the message, and pass
 * through unchanged.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public sealed interface EventTransformer extends MessageTransformer<EventMessage>
        permits DefaultEventTransformer {

    @Override
    @Internal
    MessageStream<? extends EventMessage> transform(EventMessage message, TransformationContext context);
}
