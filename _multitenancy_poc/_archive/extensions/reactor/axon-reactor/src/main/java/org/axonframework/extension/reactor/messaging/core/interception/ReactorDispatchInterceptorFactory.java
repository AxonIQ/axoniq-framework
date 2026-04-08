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

package org.axonframework.extension.reactor.messaging.core.interception;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.extension.reactor.messaging.core.ReactorMessageDispatchInterceptor;
import org.axonframework.messaging.core.Message;
import org.jspecify.annotations.Nullable;

/**
 * Functional interface for building a {@link ReactorMessageDispatchInterceptor} for a specific component type and
 * component name.
 * <p>
 * This interface allows {@code ReactorMessageDispatchInterceptors} to be constructed with knowledge of the component
 * they will intercept, allowing for fine-grained control on how or when to construct an interceptor.
 *
 * @param <M> the type of {@link Message} the resulting {@link ReactorMessageDispatchInterceptor} will intercept
 * @author Theo Emanuelsson
 * @since 5.1.0
 * @see ReactorMessageDispatchInterceptor
 * @see ReactorDispatchInterceptorRegistry
 */
@FunctionalInterface
public interface ReactorDispatchInterceptorFactory<M extends Message> {

    /**
     * Builds a {@link ReactorMessageDispatchInterceptor} for the specified component.
     *
     * @param config        the {@link Configuration} from which other components can be retrieved during construction
     * @param componentType the type of the component to build a dispatch interceptor for
     * @param componentName the name of the component to build a dispatch interceptor for
     * @return a {@link ReactorMessageDispatchInterceptor} instance configured for the specified component or
     * {@code null} when no interceptor is required for the given {@code componentType} and {@code componentName}
     * combination
     */
    @Nullable
    ReactorMessageDispatchInterceptor<? super M> build(
            Configuration config,
            Class<?> componentType,
            @Nullable String componentName
    );
}
