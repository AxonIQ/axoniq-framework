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

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of the {@link CorrelationDataProviderRegistry}, maintaining a list of
 * {@link CorrelationDataProvider CorrelationDataProviders}.
 * <p>
 * This implementation ensures given correlation data providers factory methods are only invoked once.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public class DefaultCorrelationDataProviderRegistry implements CorrelationDataProviderRegistry {

    private final List<ComponentDefinition<CorrelationDataProvider>> providerDefinitions = new ArrayList<>();

    @Override
    public CorrelationDataProviderRegistry registerProvider(
            ComponentBuilder<CorrelationDataProvider> providerBuilder
    ) {
        providerDefinitions.add(ComponentDefinition.ofType(CorrelationDataProvider.class)
                                                   .withBuilder(providerBuilder));
        return this;
    }

    @Override
    public List<CorrelationDataProvider> correlationDataProviders(Configuration config) {
        List<CorrelationDataProvider> correlationDataProviders = new ArrayList<>();
        for (ComponentDefinition<CorrelationDataProvider> providerDefinition : providerDefinitions) {
            if (!(providerDefinition instanceof ComponentDefinition.ComponentCreator<CorrelationDataProvider> creator)) {
                // The compiler should avoid this from happening.
                throw new IllegalArgumentException("Unsupported component definition type: " + providerDefinition);
            }
            CorrelationDataProvider correlationDataProvider = creator.createComponent().resolve(config);
            correlationDataProviders.add(correlationDataProvider);
        }
        return correlationDataProviders;
    }
}
