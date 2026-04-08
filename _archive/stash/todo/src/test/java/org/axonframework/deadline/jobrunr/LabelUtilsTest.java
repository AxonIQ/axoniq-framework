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

package org.axonframework.deadline.jobrunr;

import org.axonframework.deadline.TestScopeDescriptor;
import org.axonframework.messaging.core.ScopeDescriptor;
import org.axonframework.conversion.Serializer;
import org.axonframework.conversion.json.JacksonSerializer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class LabelUtilsTest {

    @Test
    void forShortStringLabelShouldBeSameAsInput() {
        String expected = "short";
        assertEquals(expected, LabelUtils.getLabel(expected));
    }

    @Test
    void forLongStringLabelShouldBeConsistentButShorter() {
        String longString = "shortsadfsdfsdfsdfsdfsdfsafdsfdgdgdgdgfgfdgdfgdfsfdsfsdfds"
                + "fdsfsdfdssdjklfisdfikusdufidsufsdfdsufsifsfsfsdfgdfdfgfgdfgdfgdfs";
        String expected = LabelUtils.getLabel(longString);
        assertEquals(expected, LabelUtils.getLabel(longString));
        assertTrue(longString.length() > expected.length());
    }

    @Test
    void combinedScopeShouldBeConsistentAndShorterThan45Characters() {
        String deadLineName = "deadlineName";
        Serializer serializer = JacksonSerializer.defaultSerializer();
        ScopeDescriptor descriptor = new TestScopeDescriptor("aggregateType", "identifier");
        String expected = LabelUtils.getCombinedLabel(serializer, deadLineName, descriptor);
        assertEquals(expected, LabelUtils.getCombinedLabel(serializer, deadLineName, descriptor));
        assertTrue(expected.length() < 45);
    }
}
