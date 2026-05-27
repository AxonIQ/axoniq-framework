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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Adds message metadata entries to the span, each under the key {@code <prefix><metadataKey>}.
 * <p>
 * By default the prefix is {@link #METADATA_PREFIX} ({@code axoniq.metadata.}) and every metadata entry is added. The
 * prefix can be overridden through the constructor — for example to keep the Axon Framework 4 prefix
 * {@code axon_metadata_}. An optional allowlist restricts which metadata keys are added; an empty allowlist means all
 * keys. Entries with a {@code null} value are skipped.
 *
 * @author Mateusz Nowak
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public final class MetadataSpanAttributesProvider implements SpanAttributesProvider {

    /**
     * Default prefix prepended to each metadata key to form the span attribute key.
     */
    public static final String METADATA_PREFIX = "axoniq.metadata.";

    private final String prefix;
    private final Set<String> allowedKeys;

    /**
     * Creates a provider that adds all metadata entries under the default {@link #METADATA_PREFIX} prefix.
     */
    public MetadataSpanAttributesProvider() {
        this(METADATA_PREFIX, Set.of());
    }

    /**
     * Creates a provider that adds the given metadata keys (when present) under the default {@link #METADATA_PREFIX}
     * prefix. An empty set means all keys.
     *
     * @param allowedKeys the metadata keys to add, or an empty set for all keys
     */
    public MetadataSpanAttributesProvider(Set<String> allowedKeys) {
        this(METADATA_PREFIX, allowedKeys);
    }

    /**
     * Creates a provider that adds the given metadata keys (when present) under the given {@code prefix}. An empty
     * allowlist means all keys.
     *
     * @param prefix      the prefix prepended to each metadata key to form the span attribute key
     * @param allowedKeys the metadata keys to add, or an empty set for all keys
     */
    public MetadataSpanAttributesProvider(String prefix, Set<String> allowedKeys) {
        this.prefix = Objects.requireNonNull(prefix, "prefix may not be null");
        this.allowedKeys = Set.copyOf(Objects.requireNonNull(allowedKeys, "allowedKeys may not be null"));
    }

    @Override
    public Map<String, String> provideForMessage(Message message, @Nullable ProcessingContext context) {
        Map<String, String> attributes = new HashMap<>();
        message.metadata().forEach((key, value) -> {
            if (value != null && (allowedKeys.isEmpty() || allowedKeys.contains(key))) {
                attributes.put(prefix + key, value);
            }
        });
        return attributes;
    }
}
