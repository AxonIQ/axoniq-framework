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

package org.axonframework.update.configuration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CommandLineUsagePropertyProviderTest {
    private final CommandLineUsagePropertyProvider provider = new CommandLineUsagePropertyProvider();

    @AfterEach
    void cleanup() {
        System.clearProperty(CommandLineUsagePropertyProvider.DISABLED_KEY);
        System.clearProperty(CommandLineUsagePropertyProvider.URL_KEY);
    }

    @Test
    void getDisabledTrue() {
        System.setProperty(CommandLineUsagePropertyProvider.DISABLED_KEY, "true");
        assertTrue(provider.getDisabled());
    }

    @Test
    void getDisabledFalse() {
        System.setProperty(CommandLineUsagePropertyProvider.DISABLED_KEY, "false");
        assertFalse(provider.getDisabled());
    }

    @Test
    void getDisabledNullWhenUnset() {
        assertNull(provider.getDisabled());
    }

    @Test
    void getUrl() {
        System.setProperty(CommandLineUsagePropertyProvider.URL_KEY, "https://custom.url");
        assertEquals("https://custom.url", provider.getUrl());
    }

    @Test
    void getUrlNullWhenUnset() {
        assertNull(provider.getUrl());
    }

    @Test
    void priority() {
        assertEquals(Integer.MAX_VALUE, provider.priority());
    }
}