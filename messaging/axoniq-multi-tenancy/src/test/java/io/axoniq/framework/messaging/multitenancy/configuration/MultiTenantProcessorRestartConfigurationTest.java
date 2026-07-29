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

package io.axoniq.framework.messaging.multitenancy.configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the defaults, fluent copy, and validation of {@link MultiTenantProcessorRestartConfiguration}.
 *
 * @author Laura Devriendt
 */
class MultiTenantProcessorRestartConfigurationTest {

    @Test
    void defaultRestartTimeoutIsThirtySeconds() {
        assertThat(MultiTenantProcessorRestartConfiguration.DEFAULT.restartTimeout())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void restartTimeoutReturnsACopyWithTheGivenTimeout() {
        MultiTenantProcessorRestartConfiguration configuration =
                MultiTenantProcessorRestartConfiguration.DEFAULT.restartTimeout(Duration.ofMinutes(2));

        assertThat(configuration.restartTimeout()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    void rejectsANullRestartTimeout() {
        assertThatThrownBy(() -> new MultiTenantProcessorRestartConfiguration(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsANonPositiveRestartTimeout() {
        assertThatThrownBy(() -> new MultiTenantProcessorRestartConfiguration(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
