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

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

/**
 * Base SPI for message transformations. The element type {@code M} is preserved -- a
 * transformer does not turn one message subtype into another. Per call, it MAY change the
 * {@link org.axonframework.messaging.core.MessageType} identity, the payload's Java type
 * or structure, and the cardinality (zero outputs / one output / many outputs).
 * <p>
 * Most users do not implement this directly; they use the
 * {@code io.axoniq.framework.messaging.transformation.events.EventTransformation} factory
 * which produces implementations behind the scenes.
 *
 * @param <M> the {@link Message} subtype this transformer accepts and emits
 * @author Laura Devriendt
 * @since 5.2.0
 */
@FunctionalInterface
public interface MessageTransformer<M extends Message> {

    /**
     * Transform a single matched message.
     * <p>
     * Called by the chain only when {@code message} matches this transformer's
     * {@code from}. The output stream MAY contain zero, one, or more elements.
     * Implementations MUST be deterministic and thread-safe: no external services,
     * no time or randomness, no mutable shared state.
     *
     * @param message the matched input message
     * @param context the active processing context, or {@code null} when the read path
     *                supplies none (e.g., tracking-processor reads). Implementations MUST
     *                tolerate {@code null}.
     * @return the resulting output stream
     */
    MessageStream<? extends M> transform(M message, @Nullable ProcessingContext context);
}
