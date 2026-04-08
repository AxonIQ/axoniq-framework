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

package org.axonframework.messaging.core.correlation;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@code CorrelationDataProvider} implementation that defines correlation data by the {@link Message#metadata()} key
 * names.
 * <p>
 * The metadata entries from {@link Message messages} matching these keys are returned as correlation data.
 *
 * @author Allard Buijze
 * @since 2.3.0
 */
public class SimpleCorrelationDataProvider implements CorrelationDataProvider {

    private final String[] headerNames;

    /**
     * Initializes a {@code SimpleCorrelationDataProvider} that returns the {@link Message#metadata()} entries of given
     * {@link Message messages} that match the given {@code metadataKeys} as correlation data.
     *
     * @param metadataKeys The keys of the {@link Message#metadata()} entries from {@link Message messages} to return as
     *                     correlation data.
     */
    public SimpleCorrelationDataProvider(String... metadataKeys) {
        this.headerNames = Arrays.copyOf(metadataKeys, metadataKeys.length);
    }

    @Override
    public Map<String, String> correlationDataFor(Message message) {
        if (headerNames.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> data = new HashMap<>();
        final Metadata metadata = message.metadata();
        for (String headerName : headerNames) {
            if (metadata.containsKey(headerName)) {
                data.put(headerName, metadata.get(headerName));
            }
        }
        return data;
    }
}
