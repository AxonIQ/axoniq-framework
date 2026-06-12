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
 * Base type for message transformations, generic over the {@link Message} subtype {@code M}
 * so events, commands, and queries share a single handle type. The element type {@code M} is
 * preserved: a transformation does not turn one message subtype into another.
 * <p>
 * This is a handle, not an entry point: users never implement it or invoke it directly. They
 * obtain instances from a transformation factory (for events, the
 * {@code io.axoniq.framework.messaging.transformation.events.EventTransformation} factory) and
 * register them with the matching chain, which calls {@link #transform(Message, TransformationContext)}
 * once it has matched a message against this transformation.
 *
 * @param <M> the {@link Message} subtype this transformation accepts and emits
 * @author Laura Devriendt
 * @since 5.2.0
 */
public interface MessageTransformer<M extends Message> {

    /**
     * Transforms a matched {@code message} into its new form, returning the result as a
     * {@link MessageStream}. The chain calls this only after the message matches this
     * transformation's {@code from}; the returned stream's element type is preserved as {@code M},
     * so a transformation never turns one message subtype into another. A 1:1 transformation emits
     * exactly one element.
     * <p>
     * Marked {@link Internal}: this is the framework's execution hook, driven by the transformation
     * chain. Application code holds and registers transformers but never invokes this method.
     *
     * @param message the matched input message
     * @param context the framework-supplied {@link TransformationContext} carrying the active
     *                processing context and the converter and resolver the transformation needs
     * @return a stream of the transformed message(s)
     */
    @Internal
    MessageStream<? extends M> transform(M message, TransformationContext context);
}
