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

import java.util.List;

/**
 * A matcher that will match if all the given {@code matchers} match against at least one item in a given List.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public class ListWithAllOfMatcher<T> extends ListMatcher<T> {

    /**
     * Construct a matcher that will return true if all the given {@code matchers} match against at least one
     * item in any given List.
     *
     * @param matchers The matchers that must match against at least one item in the list.
     */
    @SafeVarargs
    public ListWithAllOfMatcher(Matcher<T>... matchers) {
        super(matchers);
    }

    @Override
    public boolean matchesList(List<T> items) {
        for (Matcher<? super T> matcher : getMatchers()) {
            boolean match = false;
            for (Object item : items) {
                if (matcher.matches(item)) {
                    match = true;
                }
            }
            if (!match) {
                reportFailed(matcher);
                return false;
            }
        }
        return true;
    }


    @Override
    protected void describeCollectionType(Description description) {
        description.appendText("all");
    }
}
