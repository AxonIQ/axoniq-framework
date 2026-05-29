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

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

/**
 * Per-event framework bundle threaded through one chain invocation. Built once per stream
 * entry by {@link EventTransformerChain} and passed unchanged to every
 * {@link BuiltEventTransformer} that matches during fixed-point iteration. Bundles:
 * <ul>
 *     <li>{@code entryContext}: the per-entry {@link Context} the storage engine attached;
 *     read by diagnostic exceptions to surface the stream position.</li>
 *     <li>{@code processingContext}: the active {@link ProcessingContext}, forwarded to the
 *     user's mapper. {@code null} on tracking-processor reads when the caller supplies
 *     none.</li>
 *     <li>{@code converter}: the framework's {@link MessageConverter}, invoked when the
 *     stored payload's runtime class differs from the transformer's declared input type.</li>
 *     <li>{@code messageTypeResolver}: resolves the mapper's output class to a
 *     {@link org.axonframework.messaging.core.MessageType} for the output-identity check.</li>
 * </ul>
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
record ChainRuntime(
        Context entryContext,
        @Nullable ProcessingContext processingContext,
        MessageConverter converter,
        MessageTypeResolver messageTypeResolver) {
}
