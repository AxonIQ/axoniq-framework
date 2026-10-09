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

package org.axonframework.messaging;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of the {@link LegacyScopeAwareProvider} of a configuration.
 * <p>
 * Register a component of this type to change the defaults:
 * <pre>{@code
 * configurer.componentRegistry(cr -> cr.registerComponent(
 *         ScopeAwareProviderSettings.class,
 *         c -> new ScopeAwareProviderSettings(Duration.ofMinutes(2))
 * ));
 * }</pre>
 * Spring Boot applications set the {@code axon.deadline.scope-aware-provider-readiness-timeout} property instead.
 *
 * @param readinessTimeout how long the provider waits for the configuration to start its event processors before a
 *                         fired deadline fails and is retried by its deadline manager
 * @author Jakob Hatzl
 * @since 5.4.0
 */
public record ScopeAwareProviderSettings(Duration readinessTimeout) {

    /**
     * The settings used when no {@code ScopeAwareProviderSettings} component is registered: a readiness timeout of 30
     * seconds.
     */
    public static final ScopeAwareProviderSettings DEFAULT = new ScopeAwareProviderSettings(Duration.ofSeconds(30));

    /**
     * Validates the settings.
     *
     * @param readinessTimeout how long the provider waits for the configuration to start its event processors
     */
    public ScopeAwareProviderSettings {
        Objects.requireNonNull(readinessTimeout, "The readiness timeout may not be null.");
        if (readinessTimeout.isNegative()) {
            throw new IllegalArgumentException("The readiness timeout may not be negative.");
        }
    }
}
