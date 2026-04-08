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

/**
 * Utilities for creating spans which are relevant for all implementations of tracing.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class SpanUtils {

    private SpanUtils() {
        // Utility class
    }

    /**
     * Creates a human-readable name for a message based on its {@link Message#type()}.
     *
     * @param message The message to determine a message name for
     * @return The message's name
     */
    public static String determineMessageName(Message message) {
        return message.type().toString();
    }
}
