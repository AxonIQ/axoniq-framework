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

import org.axonframework.common.AxonConfigurationException;
import org.hamcrest.Description;
import org.hamcrest.StringDescription;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ExactClassMatcher}.
 *
 * @author Yoann CAPLAIN
 */
public class ExactClassMatcherTest {

    @Test
    void matchesReturnsFalseForNoneMatchingTypes() {
        Description description = new StringDescription();

        ExactClassMatcher<SomeBaseEvent> testSubject = new ExactClassMatcher<>(SomeBaseEvent.class);

        boolean result = testSubject.matches(new SomeEvent());

        assertFalse(result);
        testSubject.describeTo(description);
        assertTrue(description.toString().contains(" does not match with the actual type."));
    }

    @Test
    void matchesReturnsFalseForSubclass() {
        assertFalse(new ExactClassMatcher<>(SomeBaseEvent.class).matches(new SomeSubclassEvent()));
    }

    @Test
    void matchesForSameClass() {
        assertTrue(new ExactClassMatcher<>(SomeBaseEvent.class).matches(new SomeBaseEvent()));
    }

    @Test
    void matchesIsNullSafe() {
        assertFalse(new ExactClassMatcher<>(String.class).matches(null));
        assertThrows(AxonConfigurationException.class, () -> new ExactClassMatcher<>(null).matches("foo"));
    }

    private static class SomeBaseEvent {

    }

    private static class SomeSubclassEvent extends SomeBaseEvent {

    }

    private static class SomeEvent {

    }
}
