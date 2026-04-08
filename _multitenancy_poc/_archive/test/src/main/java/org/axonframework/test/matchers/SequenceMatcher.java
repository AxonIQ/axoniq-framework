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

import java.util.Iterator;
import java.util.List;

/**
 * A matcher that will match if all the given {@code matchers} each match against an item that the previous
 * matcher matched against. That means the second matcher should match an item that follow the item that the first
 * matcher matched.
 * <p/>
 * If the number of items is larger than the number of matchers, the excess items are not evaluated. Use {@link
 * Matchers#exactSequenceOf(org.hamcrest.Matcher[])} to match the sequence exactly. If the last item of the list
 * has been evaluated, and Matchers still remain, they are evaluated against a {@code null} value.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public class SequenceMatcher<T> extends ListMatcher<T> {

    /**
     * Construct a matcher that will return true if all the given {@code matchers} match against an item
     * positioned after the item that the previous matcher matched against.
     *
     * @param matchers The matchers that must match against at least one item in the list.
     */
    @SafeVarargs
    public SequenceMatcher(Matcher<? super T>... matchers) {
        super(matchers);
    }

    @Override
    public boolean matchesList(List<T> items) {
        Iterator<?> itemIterator = items.iterator();
        Iterator<Matcher<? super T>> matcherIterator = getMatchers().iterator();
        Matcher<? super T> currentMatcher = null;
        if (matcherIterator.hasNext()) {
            currentMatcher = matcherIterator.next();
        }

        while (itemIterator.hasNext() && currentMatcher != null) {
            boolean hasMatch = currentMatcher.matches(itemIterator.next());
            if (hasMatch) {
                if (matcherIterator.hasNext()) {
                    currentMatcher = matcherIterator.next();
                } else {
                    currentMatcher = null;
                }
            }
        }
        if (currentMatcher != null && !currentMatcher.matches(null)) {
            reportFailed(currentMatcher);
            return false;
        }

        return matchRemainder(matcherIterator);
    }

    @Override
    protected void describeCollectionType(Description description) {
        description.appendText("sequence");
    }
}
