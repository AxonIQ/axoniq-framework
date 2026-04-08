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

import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EnvironmentVariableUsagePropertyProviderTest {

    private final Map<String, String> env = new HashMap<>();
    private EnvironmentVariableUsagePropertyProvider provider;

    @BeforeEach
    void setup() {
        env.clear();
        provider = new EnvironmentVariableUsagePropertyProvider(env::get);
    }

    @Test
    void getDisabledTrue() {
        env.put(EnvironmentVariableUsagePropertyProvider.DISABLED_KEY, "true");
        assertTrue(provider.getDisabled());
    }

    @Test
    void getDisabledFalse() {
        env.put(EnvironmentVariableUsagePropertyProvider.DISABLED_KEY, "false");
        assertFalse(provider.getDisabled());
    }

    @Test
    void getDisabledNullWhenUnset() {
        assertNull(provider.getDisabled());
    }

    @Test
    void getUrl() {
        env.put(EnvironmentVariableUsagePropertyProvider.URL_KEY, "https://env.url");
        assertEquals("https://env.url", provider.getUrl());
    }

    @Test
    void getUrlNullWhenUnset() {
        assertNull(provider.getUrl());
    }

    @Test
    void priority() {
        assertEquals(Integer.MAX_VALUE / 2, provider.priority());
    }
}
