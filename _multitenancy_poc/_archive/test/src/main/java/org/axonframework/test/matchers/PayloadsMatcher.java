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

import org.axonframework.messaging.core.Message;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

/**
 * Matches a list of Messages if the list of their payloads matches the given matcher.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public class PayloadsMatcher extends BaseMatcher<List<? extends Message>> {

    private final Matcher<? extends Iterable<?>> matcher;

    /**
     * Constructs an instance that uses the given {@code matcher} to match the payloads.
     *
     * @param matcher The matcher to match the payloads with.
     */
    public PayloadsMatcher(Matcher<? extends Iterable<?>> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matches(Object item) {
        if (!List.class.isInstance(item)) {
            return false;
        }
        List<Object> payloads = new ArrayList<>();
        for (Object listItem : (List) item) {
            if (Message.class.isInstance(listItem)) {
                payloads.add(((Message) listItem).payload());
            } else {
                payloads.add(item);
            }
        }
        return matcher.matches(payloads);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("List with Messages with Payloads matching <");
        matcher.describeTo(description);
        description.appendText(">");
    }
}
