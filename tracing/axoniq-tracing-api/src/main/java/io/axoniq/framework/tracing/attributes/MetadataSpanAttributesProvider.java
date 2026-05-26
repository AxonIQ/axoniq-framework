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

package io.axoniq.framework.tracing.attributes;

import io.axoniq.framework.tracing.SpanAttributesProvider;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adds message metadata entries to the span, each under the key {@code axoniq.metadata.<metadataKey>}.
 * <p>
 * By default (no-argument constructor) every metadata entry of the message is added. When constructed with an explicit
 * allowlist of keys, only those keys present in the message metadata are added. Entries with a {@code null} value are
 * skipped.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class MetadataSpanAttributesProvider implements SpanAttributesProvider {

    /**
     * Prefix prepended to each metadata key to form the span attribute key.
     */
    public static final String METADATA_PREFIX = "axoniq.metadata.";

    private final @Nullable List<String> allowedKeys;

    /**
     * Creates a provider that adds all metadata entries of the message.
     */
    public MetadataSpanAttributesProvider() {
        this.allowedKeys = null;
    }

    /**
     * Creates a provider that adds only the given metadata keys, when present on the message.
     *
     * @param allowedKeys the metadata keys to add
     */
    public MetadataSpanAttributesProvider(String... allowedKeys) {
        this.allowedKeys = List.of(allowedKeys);
    }

    @Override
    public Map<String, String> provideForMessage(Message message, @Nullable ProcessingContext context) {
        Map<String, String> attributes = new HashMap<>();
        message.metadata().forEach((key, value) -> {
            if (value != null && (allowedKeys == null || allowedKeys.contains(key))) {
                attributes.put(METADATA_PREFIX + key, value);
            }
        });
        return attributes;
    }
}
