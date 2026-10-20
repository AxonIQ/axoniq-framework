/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.api;

import io.axoniq.license.entitlement.AxoniqAddon;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link MultiTenancyAxoniqAddon}, including its ServiceLoader discovery through
 * {@link AxoniqAddon#loadAddons()}.
 *
 * @author Mitchell Herrijgers
 */
class MultiTenancyAxoniqAddonTest {

    private final MultiTenancyAxoniqAddon addon = new MultiTenancyAxoniqAddon();

    @Test
    void identifierReturnsExpectedValue() {
        assertThat(addon.identifier()).isEqualTo("framework.multi_tenancy");
    }

    @Test
    void nameReturnsExpectedValue() {
        assertThat(addon.name()).isEqualTo("Axoniq Framework - Multi-tenancy");
    }

    @Test
    void addonIsDiscoveredThroughTheServiceLoader() {
        // when
        List<AxoniqAddon> addons = AxoniqAddon.loadAddons();

        // then
        assertThat(addons).anyMatch(loaded -> loaded instanceof MultiTenancyAxoniqAddon);
    }
}
