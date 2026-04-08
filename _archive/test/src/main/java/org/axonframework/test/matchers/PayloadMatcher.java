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

/**
 * Matcher that matches any message (e.g. Event, Command) who's payload matches the given matcher.
 *
 * @param <T> The type of Message the matcher can match against
 * @author Allard Buijze
 * @since 2.0
 */
public class PayloadMatcher<T extends Message> extends BaseMatcher<T> {

    private final Matcher<?> payloadMatcher;

    /**
     * Constructs an instance with the given {@code payloadMatcher}.
     *
     * @param payloadMatcher The matcher that must match the Message's payload.
     */
    public PayloadMatcher(Matcher<?> payloadMatcher) {
        this.payloadMatcher = payloadMatcher;
    }

    @Override
    public boolean matches(Object item) {
        return Message.class.isInstance(item)
                && payloadMatcher.matches(((Message) item).payload());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Message with payload <");
        payloadMatcher.describeTo(description);
        description.appendText(">");
    }
}
