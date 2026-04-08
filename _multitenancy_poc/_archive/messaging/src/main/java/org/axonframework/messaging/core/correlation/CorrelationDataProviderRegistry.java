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
import org.axonframework.common.configuration.*;
import org.axonframework.messaging.core.interception.CorrelationDataInterceptor;

import java.util.List;

/**
 * A registry of {@link CorrelationDataProvider CorrelationDataProviders}, acting as a collection of
 * {@link ComponentRegistry#registerComponent(ComponentDefinition) registered
 * CorrelationDataProvider components}.
 * <p>
 * Provides operations to register {@code CorrelationDataProviders} one by one. Registered providers can be retrieved
 * through {@link #correlationDataProviders(Configuration)}.
 * <p>
 * These operations are expected to be invoked within a {@link DecoratorDefinition}. As
 * such, <b>any</b> registered correlation data providers are <b>only</b> applied when the infrastructure component
 * requiring them is constructed. When, for example, a
 * {@link CorrelationDataInterceptor} is constructed, this registry is invoked
 * to retrieve its correlation data providers. Correlation data providers that are registered once the
 * {@code CorrelationDataInterceptor} has already been constructed are not taken into account.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public interface CorrelationDataProviderRegistry {

    /**
     * Registers the given {@code providerBuilder} constructing a {@link CorrelationDataProvider} for all handling
     * infrastructure components.
     *
     * @param providerBuilder The {@link CorrelationDataProvider} builder to register.
     * @return This {@code InterceptorRegistry}, for fluent interfacing.
     */
    CorrelationDataProviderRegistry registerProvider(
            ComponentBuilder<CorrelationDataProvider> providerBuilder
    );

    /**
     * Returns the list of {@link CorrelationDataProvider CorrelationDataProviders} registered in this registry.
     *
     * @param config The configuration to build all {@link CorrelationDataProvider CorrelationDataProviders} with.
     * @return The list of {@link CorrelationDataProvider CorrelationDataProviders}.
     */
    List<CorrelationDataProvider> correlationDataProviders(Configuration config);
}
