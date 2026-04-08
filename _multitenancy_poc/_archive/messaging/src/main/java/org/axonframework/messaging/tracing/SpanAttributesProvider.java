/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.tracing;

import org.axonframework.messaging.core.Message;

import java.util.Map;

/**
 * Represents a provider of attributes to a {@link Span}, based on a {@link Message}. It's the responsibility of the
 * {@link SpanFactory} to invoke these and add the attributes to the {@link Span}.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public interface SpanAttributesProvider {

    /**
     * Provides a map of attributes to add to the {@link Span} based on the {@link Message} provided.
     *
     * @param message The message
     * @return The attributes
     */
    Map<String, String> provideForMessage(Message message);
}
