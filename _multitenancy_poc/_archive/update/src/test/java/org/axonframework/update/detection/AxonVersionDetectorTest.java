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

package org.axonframework.update.detection;

import org.junit.jupiter.api.*;


import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the {@link AxonVersionDetector} as best as possible. However, detection from JARs has had to be tested manually,
 * as there is no jar dependency available in the test resources to test against.
 * This test might be expanded if the Update Checker is moved out to a separate module, allowing for
 * a dependency on axon-messaging to be detected.
 */
class AxonVersionDetectorTest {

    /**
     * Tests that the version detector can parse the pom.properties in the test resources under
     * META-INF/maven/org.axonframework/axon-modelling/pom.properties.
     */
    @Test
    void detectsFilePomProperties() {
        var versions = AxonVersionDetector.safeDetectAxonModules();
        assertFalse(versions.isEmpty(), "Expected at least one Axon module version to be detected");
        assertTrue(versions.stream()
                           .filter(Objects::nonNull)
                           .anyMatch(v -> v.groupId().equals("org.axonframework") &&
                           v.artifactId().equals("axon-modelling") &&
                           v.version().equals("2.1-SNAPSHOT")),
                   "Expected Axon Modelling module to be detected");
    }
}