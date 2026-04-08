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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * Matcher testing for exact classes.
 *
 * @author Yoann CAPLAIN
 * @since 4.6.2
 */
public class ExactClassMatcher<T> extends TypeSafeMatcher<T> {

    private final Class<T> expectedClass;

    /**
     * Construct a {@link ExactClassMatcher} that will match an {@code actual} value with the given {@code expectedClass}.
     *
     * @param expectedClass The object class to match with during {@link #matches(Object)}.
     * @return a matcher that matches based on the class
     */
    public static <T> Matcher<T> exactClassOf(Class<T> expectedClass) {
        return new ExactClassMatcher<T>(expectedClass);
    }

    /**
     * Construct a {@link ExactClassMatcher} that will match an {@code actual} value with the given {@code expectedClass}.
     *
     * @param expectedClass The object class to match with during {@link #matches(Object)}.
     */
    public ExactClassMatcher(Class<T> expectedClass) {
        assertNonNull(expectedClass, "The expected class should be non-null.");
        this.expectedClass = expectedClass;
    }

    @Override
    protected boolean matchesSafely(T item) {
        return expectedClass.equals(item.getClass());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(expectedClass.getName());
        description.appendText(" does not match with the actual type.");
    }
}
