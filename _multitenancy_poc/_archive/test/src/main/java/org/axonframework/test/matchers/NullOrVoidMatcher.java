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

/**
 * Matcher that matches against a {@code null} or {@code void} value. Can be used to make sure no trailing
 * events remain when using an Exact Sequence Matcher.
 *
 * @param <T> The generic type of the mather
 * @author Allard Buijze
 * @since 1.1
 */
public class NullOrVoidMatcher<T> extends BaseMatcher<T> {

    @Override
    public boolean matches(Object item) {
        return item == null || Void.class.equals(item);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("<nothing>");
    }
}
