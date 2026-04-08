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

package org.axonframework.messaging.tracing.attributes;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.tracing.SpanAttributesProvider;

import java.util.Map;
import static java.util.Collections.singletonMap;

/**
 * Adds the message identifier to the Span.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MessageIdSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public Map<String, String> provideForMessage(Message message) {
        return singletonMap("axon_message_id", message.identifier());
    }
}
