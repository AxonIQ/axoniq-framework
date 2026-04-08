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

import static org.junit.jupiter.api.Assertions.*;

/**
 * As Axon Framework does not depend on Kotlin, we cannot test the Kotlin version detection in a meaningful way.
 * This class is a placeholder for future meaningful tests when the update checker is moved.
 */
class KotlinVersionTest {

    @Test
    void doesNotDetectKotlinVersion() {
        // This test is a placeholder. The Kotlin version detection is not tested here as Axon Framework does not depend on Kotlin.
        // When the update checker is moved to a module that depends on Kotlin, this test can be updated to check the Kotlin version.
        assertEquals("none", KotlinVersion.get());
    }

}