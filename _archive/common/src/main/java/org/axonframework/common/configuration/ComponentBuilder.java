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

package org.axonframework.common.configuration;

/**
 * Functional interface describing how to build a component of type {@code C} using the {@link Configuration} during
 * construction.
 *
 * @param <C> The component to be built.
 * @author Steven van Beelen
 * @since 5.0.0
 */
@FunctionalInterface
public interface ComponentBuilder<C> {

    /**
     * Builds a component of type {@code C} using the given {@code config} during construction.
     *
     * @param config The configuration from which other components can be retrieved to build the result.
     * @return A component of type {@code C} using the given {@code config} during construction.
     */
    C build(Configuration config);
}
