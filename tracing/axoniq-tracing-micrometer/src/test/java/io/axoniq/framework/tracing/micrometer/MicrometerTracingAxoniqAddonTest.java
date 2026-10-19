/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.tracing.micrometer;

import io.axoniq.license.entitlement.AxoniqAddon;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link MicrometerTracingAxoniqAddon}, including its ServiceLoader discovery through
 * {@link AxoniqAddon#loadAddons()}.
 *
 * @author Mateusz Nowak
 */
class MicrometerTracingAxoniqAddonTest {

    private final MicrometerTracingAxoniqAddon addon = new MicrometerTracingAxoniqAddon();

    @Test
    void identifierReturnsExpectedValue() {
        assertThat(addon.identifier()).isEqualTo("framework.distributed_tracing");
    }

    @Test
    void nameReturnsExpectedValue() {
        assertThat(addon.name()).isEqualTo("Axoniq Framework - Distributed Tracing (Micrometer)");
    }

    @Test
    void addonIsDiscoveredThroughTheServiceLoader() {
        // when
        List<AxoniqAddon> addons = AxoniqAddon.loadAddons();

        // then
        assertThat(addons).anyMatch(loaded -> loaded instanceof MicrometerTracingAxoniqAddon);
    }
}
