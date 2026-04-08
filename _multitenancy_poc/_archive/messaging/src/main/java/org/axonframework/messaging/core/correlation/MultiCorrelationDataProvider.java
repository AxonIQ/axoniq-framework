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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code CorrelationDataProvider} that combines the data of multiple other correlation providers.
 * <p>
 * When multiple instance provide the same keys, a delegate will override the entries provided by previously resolved
 * delegates.
 *
 * @author Allard Buijze
 * @since 2.3.0
 */
public class MultiCorrelationDataProvider implements CorrelationDataProvider {

    private final List<? extends CorrelationDataProvider> delegates;

    /**
     * Initialize a {@code MultiCorrelationDataProvider}, delegating to given {@code correlationDataProviders}.
     *
     * @param correlationDataProviders The {@code CorrelationDataProviders} to delegate to.
     */
    public MultiCorrelationDataProvider(List<? extends CorrelationDataProvider> correlationDataProviders) {
        delegates = new ArrayList<>(correlationDataProviders);
    }

    @Override
    public Map<String, String> correlationDataFor(Message message) {
        Map<String, String> correlationData = new HashMap<>();
        for (CorrelationDataProvider delegate : delegates) {
            correlationData.putAll(delegate.correlationDataFor(message));
        }
        return correlationData;
    }
}
