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

import java.util.HashMap;
import java.util.Map;

/**
 * Adds the metadata of the message to the span as attributes.
 * <p>
 * The values are not serialized to a specific format, rather the {@link Object#toString()} method is called on it.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MetadataSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public Map<String, String> provideForMessage(Message message) {
        Map<String, String> map = new HashMap<>();
        message.metadata().forEach((key, value) -> map.put("axon_metadata_" + key, value.toString()));
        return map;
    }
}
