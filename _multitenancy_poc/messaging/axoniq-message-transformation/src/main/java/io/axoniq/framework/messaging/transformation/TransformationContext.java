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
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Framework-supplied bundle of the per-invocation services a
 * {@link MessageTransformer#transform(org.axonframework.messaging.core.Message, TransformationContext)}
 * needs to do its work. The transformation chain builds one instance per source message and passes
 * it, unchanged, to every transformer applied to that message.
 * <p>
 * You receive a {@code TransformationContext}; you do not build one. It exposes:
 * <ul>
 *     <li>{@link #entryContext()}: the read-stream entry's {@link Context}, carrying the stream
 *     position when the storage engine attached a tracking token; useful for diagnostics.</li>
 *     <li>{@link #processingContext()}: the active {@link ProcessingContext}, or {@code null} on
 *     read paths (such as tracking-processor reads) where the caller supplies none. Forwarded to
 *     the transformer's payload mapper.</li>
 *     <li>{@link #converter()}: the {@link MessageConverter} used to convert a stored payload into
 *     the type a transformer declared as its input.</li>
 *     <li>{@link #messageTypeResolver()}: the {@link MessageTypeResolver} used to resolve a mapper
 *     output's class to a {@link MessageType} for the output-identity check.</li>
 * </ul>
 * <p>
 * Marked {@link Internal}: this is framework plumbing the chain assembles per source message.
 * Application code never constructs it and never invokes the {@code transform} method it is
 * passed to, so its component set may change between minor or patch releases.
 *
 * @param entryContext        the read-stream entry's {@link Context}, carrying stream position when
 *                            available
 * @param processingContext   the active {@link ProcessingContext}, or {@code null} when the read
 *                            path supplies none
 * @param converter           the {@link MessageConverter} for stored-payload conversion
 * @param messageTypeResolver the {@link MessageTypeResolver} for the output-identity check
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
public record TransformationContext(
        Context entryContext,
        @Nullable ProcessingContext processingContext,
        MessageConverter converter,
        MessageTypeResolver messageTypeResolver) {

    /**
     * Validates that the framework-supplied non-null components are present. The
     * {@link #processingContext()} is intentionally nullable: read paths such as
     * tracking-processor reads supply none.
     *
     * @param entryContext        the read-stream entry's {@link Context}, carrying stream position when
     *                            available
     * @param processingContext   the active {@link ProcessingContext}, or {@code null} when the read
     *                            path supplies none
     * @param converter           the {@link MessageConverter} for stored-payload conversion
     * @param messageTypeResolver the {@link MessageTypeResolver} for the output-identity check
     */
    public TransformationContext {
        requireNonNull(entryContext, "entryContext");
        requireNonNull(converter, "converter");
        requireNonNull(messageTypeResolver, "messageTypeResolver");
    }
}
