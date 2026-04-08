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

package org.axonframework.extension.tracing.opentelemetry;

import org.axonframework.extension.tracing.opentelemetry.MetadataContextSetter;
import org.junit.jupiter.api.*;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class MetadataContextSetterTest {

    @Test
    void shouldSetValue() {
        HashMap<String, String> subject = new HashMap<>();
        MetadataContextSetter.INSTANCE.set(subject, "myKeyOneThree", "myValueThree");
        assertTrue(subject.containsKey("myKeyOneThree"));
        assertEquals("myValueThree", subject.get("myKeyOneThree"));
    }

    @Test
    void shouldRejectNulls() {
        assertThrows(IllegalArgumentException.class, () ->
                MetadataContextSetter.INSTANCE.set(null, "", "")
        );
    }
}
