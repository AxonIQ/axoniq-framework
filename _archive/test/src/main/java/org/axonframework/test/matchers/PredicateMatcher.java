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
import org.hamcrest.TypeSafeMatcher;

import java.util.function.Predicate;

/**
 * Matcher implementation that delegates the matching to a Predicate. As predicates do not provide a means to describe '
 * their expectation, this matcher defaults to '[matches a given predicate]'.
 *
 * @param <T> The type of value to match against
 */
public class PredicateMatcher<T> extends TypeSafeMatcher<T> {
    private final Predicate<T> predicate;

    /**
     * Initializes the matcher using given {@code predicate} to test values to match against.
     *
     * @param predicate The predicate defining matching values
     */
    public PredicateMatcher(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    protected boolean matchesSafely(T item) {
        return predicate.test(item);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("[matches a given predicate]");
    }
}
