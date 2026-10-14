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

package org.axonframework.deadline.jobrunr;

import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.deadline.TestScopeDescriptor;
import org.axonframework.messaging.ScopeDescriptor;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link LabelUtils}.
 *
 * @author Gerard Klijs
 */
class LabelUtilsTest {

    @Test
    void forShortStringLabelShouldBeSameAsInput() {
        // given
        String expected = "short";

        // when / then
        assertThat(LabelUtils.getLabel(expected)).isEqualTo(expected);
    }

    @Test
    void forLongStringLabelShouldBeConsistentButShorter() {
        // given
        String longString = "shortsadfsdfsdfsdfsdfsdfsafdsfdgdgdgdgfgfdgdfgdfsfdsfsdfds"
                + "fdsfsdfdssdjklfisdfikusdufidsufsdfdsufsifsfsfsdfgdfdfgfgdfgdfgdfs";

        // when
        String label = LabelUtils.getLabel(longString);

        // then
        assertThat(LabelUtils.getLabel(longString)).isEqualTo(label);
        assertThat(label).hasSizeLessThan(longString.length());
    }

    @Test
    void combinedScopeShouldBeConsistentAndShorterThan45Characters() {
        // given
        String deadlineName = "deadlineName";
        StoredDeadlineConverter converter = new StoredDeadlineConverter(new JacksonConverter());
        ScopeDescriptor descriptor = new TestScopeDescriptor("aggregateType", "identifier");

        // when
        String label = LabelUtils.getCombinedLabel(converter, deadlineName, descriptor);

        // then
        assertThat(LabelUtils.getCombinedLabel(converter, deadlineName, descriptor)).isEqualTo(label);
        assertThat(label).hasSizeLessThan(45);
    }
}
