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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.Collection;
import java.util.List;

/**
 * Matches any empty collection.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public class EmptyCollectionMatcher<T> extends BaseMatcher<List<T>> {

    private final String contentDescription;

    /**
     * Creates a matcher of a list of empty items. The name of the item type (in plural) is passed in the given
     * {@code contentDescription} and will be part of the description of this matcher.
     *
     * @param contentDescription The description of the content type of the collection
     */
    public EmptyCollectionMatcher(String contentDescription) {
        this.contentDescription = contentDescription;
    }

    @Override
    public boolean matches(Object item) {
        return item instanceof Collection && ((Collection) item).isEmpty();
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("no ");
        description.appendText(contentDescription);
    }
}
