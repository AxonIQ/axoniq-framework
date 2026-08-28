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
