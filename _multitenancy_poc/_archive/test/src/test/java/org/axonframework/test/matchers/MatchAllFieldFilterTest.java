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

package org.axonframework.test.matchers;

import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Allard Buijze
 */
class MatchAllFieldFilterTest {

    @SuppressWarnings("unused")
    private String field;

    @Test
    void acceptWhenEmpty() throws Exception {
        assertTrue(new MatchAllFieldFilter(Collections.emptyList())
                           .accept(MatchAllFieldFilterTest.class.getDeclaredField("field")));
    }

    @Test
    void acceptWhenAllAccept() throws Exception {
        assertTrue(new MatchAllFieldFilter(Arrays.asList(AllFieldsFilter.instance(),
                                                         AllFieldsFilter.instance()))
                           .accept(MatchAllFieldFilterTest.class.getDeclaredField("field")));
    }

    @Test
    void rejectWhenOneRejects() throws Exception {
        assertFalse(new MatchAllFieldFilter(Arrays.asList(AllFieldsFilter.instance(),
                                                          field -> false))
                            .accept(MatchAllFieldFilterTest.class.getDeclaredField("field")));
    }
}
