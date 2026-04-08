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

package org.axonframework.messaging.core;

import org.axonframework.messaging.core.Context.ResourceKey;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ResourceKey}.
 *
 * @author Steven van Beelen
 */
class ResourceKeyTest {

    private static final String TEST_LABEL = "testLabel";

    @Test
    void resourceKeysShowDebugStringInOutput() {
        ResourceKey<Object> resourceKey = ResourceKey.withLabel(TEST_LABEL);

        assertTrue(resourceKey.toString().contains(TEST_LABEL));
    }

    @Test
    void resourceKeysWithEmptyDebugKeyShowsKeyIdOnly() {
        ResourceKey<Object> resourceKey = ResourceKey.withLabel("");

        assertFalse(resourceKey.toString().contains("["));
    }

    @Test
    void resourceKeysWithNullDebugKeyShowsKeyIdOnly() {
        ResourceKey<Object> resourceKey = ResourceKey.withLabel(null);

        assertFalse(resourceKey.toString().contains("["));
    }

    @Test
    void resourceKeysWithIdenticalLabelsAreNotEqual() {
        assertNotEquals(ResourceKey.withLabel(TEST_LABEL), ResourceKey.withLabel(TEST_LABEL));
    }
}
