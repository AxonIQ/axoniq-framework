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

import java.util.Objects;

/**
 * Matcher testing for object equality as per {@link Objects#equals(Object o)}
 *
 * @author bliessens
 * @since 3.3
 */
public class EqualsMatcher<T> extends TypeSafeMatcher<T> {

    private final T expectedValue;

    public static <T> Matcher<T> equalTo(T item) {
        return new EqualsMatcher<T>(item);
    }

    public EqualsMatcher(T expected) {
        this.expectedValue = expected;
    }

    @Override
    protected boolean matchesSafely(T item) {
        return Objects.equals(expectedValue, item);
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(expectedValue);
    }
}
