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

package org.axonframework.extension.springboot;

import org.axonframework.messaging.ScopeAwareProviderSettings;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Spring Boot properties of the Axon Framework 4 deadline managers in {@code axoniq-legacy}, under the
 * {@code axon.deadline} prefix.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@ConfigurationProperties("axon.deadline")
public class LegacyDeadlineProperties {

    /**
     * How long a fired deadline waits for Axon to start its event processors before it fails and is retried by its
     * deadline manager. Defaults to 30 seconds.
     */
    private Duration scopeAwareProviderReadinessTimeout = ScopeAwareProviderSettings.DEFAULT.readinessTimeout();

    /**
     * Returns how long a fired deadline waits for Axon to start its event processors.
     *
     * @return the readiness timeout of the {@link org.axonframework.messaging.ScopeAwareProvider}
     */
    public Duration getScopeAwareProviderReadinessTimeout() {
        return scopeAwareProviderReadinessTimeout;
    }

    /**
     * Sets how long a fired deadline waits for Axon to start its event processors.
     *
     * @param scopeAwareProviderReadinessTimeout the readiness timeout of the
     *                                           {@link org.axonframework.messaging.ScopeAwareProvider}
     */
    public void setScopeAwareProviderReadinessTimeout(Duration scopeAwareProviderReadinessTimeout) {
        this.scopeAwareProviderReadinessTimeout = scopeAwareProviderReadinessTimeout;
    }
}
