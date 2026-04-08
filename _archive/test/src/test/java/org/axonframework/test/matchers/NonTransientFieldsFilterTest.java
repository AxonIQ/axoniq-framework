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

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Allard Buijze
 */
class NonTransientFieldsFilterTest {

    @SuppressWarnings("unused")
    private transient String transientField;
    @SuppressWarnings("unused")
    private String nonTransientField;

    @Test
    void acceptNonTransientField() throws Exception {
        assertTrue(NonTransientFieldsFilter.instance()
                                           .accept(getClass().getDeclaredField("nonTransientField")));
    }

    @Test
    void rejectTransientField() throws Exception {
        assertFalse(NonTransientFieldsFilter.instance()
                                            .accept(getClass().getDeclaredField("transientField")));
    }
}
