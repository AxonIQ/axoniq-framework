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

import java.util.List;
import org.junit.jupiter.api.Test;

class HierarchicalUsagePropertyProviderTest {
    @Test
    void getDisabled_returnsFirstNonNullByPriority() {
        UsagePropertyProvider low = new DummyProvider(null, null, 1);
        UsagePropertyProvider mid = new DummyProvider(false, null, 5);
        UsagePropertyProvider high = new DummyProvider(true, null, 10);
        HierarchicalUsagePropertyProvider provider = new HierarchicalUsagePropertyProvider(List.of(low, mid, high));
        // high has highest priority and non-null value
        assertTrue(provider.getDisabled());
    }

    @Test
    void getDisabled_skipsNullsAndReturnsNext() {
        UsagePropertyProvider low = new DummyProvider(null, null, 1);
        UsagePropertyProvider high = new DummyProvider(null, null, 10);
        UsagePropertyProvider mid = new DummyProvider(false, null, 5);
        HierarchicalUsagePropertyProvider provider = new HierarchicalUsagePropertyProvider(List.of(low, mid, high));
        // mid is the first non-null
        assertFalse(provider.getDisabled());
    }

    @Test
    void getDisabled_returnsFalseIfAllNull() {
        UsagePropertyProvider low = new DummyProvider(null, null, 1);
        UsagePropertyProvider high = new DummyProvider(null, null, 10);
        HierarchicalUsagePropertyProvider provider = new HierarchicalUsagePropertyProvider(List.of(low, high));
        assertFalse(provider.getDisabled());
    }

    @Test
    void getUrl_returnsFirstNonNullByPriority() {
        UsagePropertyProvider low = new DummyProvider(null, null, 1);
        UsagePropertyProvider mid = new DummyProvider(null, "http://mid", 5);
        UsagePropertyProvider high = new DummyProvider(null, "http://high", 10);
        HierarchicalUsagePropertyProvider provider = new HierarchicalUsagePropertyProvider(List.of(low, mid, high));
        assertEquals("http://high", provider.getUrl());
    }

    @Test
    void getUrl_skipsNullsAndReturnsNext() {
        UsagePropertyProvider low = new DummyProvider(null, null, 1);
        UsagePropertyProvider high = new DummyProvider(null, null, 10);
        UsagePropertyProvider mid = new DummyProvider(null, "http://mid", 5);
        HierarchicalUsagePropertyProvider provider = new HierarchicalUsagePropertyProvider(List.of(low, mid, high));
        assertEquals("http://mid", provider.getUrl());
    }

    @Test
    void getUrl_returnsEmptyIfAllNull() {
        UsagePropertyProvider low = new DummyProvider(null, null, 1);
        UsagePropertyProvider high = new DummyProvider(null, null, 10);
        HierarchicalUsagePropertyProvider provider = new HierarchicalUsagePropertyProvider(List.of(low, high));
        assertEquals("", provider.getUrl());
    }
}