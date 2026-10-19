/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.legacy;

import io.axoniq.license.entitlement.AxoniqAddon;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

class LegacySupportAxoniqAddonTest {

    private final LegacySupportAxoniqAddon addon = new LegacySupportAxoniqAddon();

    @Test
    void identifierReturnsExpectedValue() {
        assertThat(addon.identifier()).isEqualTo("framework.legacy_support");
    }

    @Test
    void nameReturnsExpectedValue() {
        assertThat(addon.name()).isEqualTo("Axoniq Framework - Legacy Support");
    }

    @Test
    void addonIsDiscoverableThroughServiceLoader() {
        // when
        List<Class<?>> addons = ServiceLoader.load(AxoniqAddon.class)
                                             .stream()
                                             .<Class<?>>map(ServiceLoader.Provider::type)
                                             .toList();

        // then
        assertThat(addons).contains(LegacySupportAxoniqAddon.class);
    }
}
