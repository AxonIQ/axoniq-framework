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

package io.axoniq.framework.tracing;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Contributes attributes to a {@link Span} based on the {@link Message} the span is created for. Multiple providers
 * compose; the order in which they are invoked is unspecified.
 * <p>
 * The {@code context} parameter is {@link Nullable} because some span-creation points have no
 * {@link ProcessingContext} available. Providers that do not need the context ignore it; providers that do need it
 * (for example {@link io.axoniq.framework.tracing.attributes.AggregateIdentifierSpanAttributesProvider}, which reads
 * from {@link org.axonframework.messaging.core.LegacyResources#AGGREGATE_IDENTIFIER_KEY}) MUST handle {@code null}
 * gracefully.
 * <p>
 * Implementations MUST NOT reference Axon Framework 4-era types that have been removed from Axon Framework 5.
 *
 * @author Mateusz Nowak
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
@FunctionalInterface
public interface SpanAttributesProvider {

    /**
     * Provides the attributes this provider contributes for the given {@code message} and optional {@code context}.
     *
     * @param message the message the span is created for
     * @param context the active processing context, or {@code null} when none is available
     * @return the attributes to add to the span; an empty map when this provider contributes nothing
     */
    Map<String, String> provideForMessage(Message message, @Nullable ProcessingContext context);
}
