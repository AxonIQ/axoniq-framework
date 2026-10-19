/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the defaults, fluent copy, and validation of {@link MultiTenantStreamingProcessorRestartConfiguration}.
 *
 * @author Laura Devriendt
 */
class MultiTenantStreamingProcessorRestartConfigurationTest {

    @Test
    void defaultRestartTimeoutIsThirtySeconds() {
        assertThat(MultiTenantStreamingProcessorRestartConfiguration.DEFAULT.restartTimeout())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void restartTimeoutReturnsACopyWithTheGivenTimeout() {
        MultiTenantStreamingProcessorRestartConfiguration configuration =
                MultiTenantStreamingProcessorRestartConfiguration.DEFAULT.restartTimeout(Duration.ofMinutes(2));

        assertThat(configuration.restartTimeout()).isEqualTo(Duration.ofMinutes(2));
    }

    @Test
    void rejectsANullRestartTimeout() {
        assertThatThrownBy(() -> new MultiTenantStreamingProcessorRestartConfiguration(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsANonPositiveRestartTimeout() {
        assertThatThrownBy(() -> new MultiTenantStreamingProcessorRestartConfiguration(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
