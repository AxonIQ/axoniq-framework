/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
