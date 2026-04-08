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

package org.axonframework.messaging.eventstreaming;

import org.axonframework.messaging.core.Context;
import org.junit.jupiter.api.*;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link org.axonframework.messaging.eventstreaming.Tag}.
 *
 * @author Steven van Beelen
 */
class TagTest {

    private static final String TEST_KEY = "key";
    private static final String TEST_VALUE = "value";

    @Test
    void containsExpectedData() {
        org.axonframework.messaging.eventstreaming.Tag testSubject = new org.axonframework.messaging.eventstreaming.Tag(TEST_KEY, TEST_VALUE);

        assertEquals(TEST_KEY, testSubject.key());
        assertEquals(TEST_VALUE, testSubject.value());
    }

    @Test
    void identicalTagsAreEqual() {
        org.axonframework.messaging.eventstreaming.Tag testSubject = new org.axonframework.messaging.eventstreaming.Tag(TEST_KEY, TEST_VALUE);

        assertEquals(testSubject, testSubject);
    }

    @Test
    void assertsEventAndTagsAreNonNull() {
        //noinspection DataFlowIssue
        assertThrows(IllegalArgumentException.class, () -> new org.axonframework.messaging.eventstreaming.Tag(null, TEST_VALUE));
        //noinspection DataFlowIssue
        assertThrows(IllegalArgumentException.class, () -> new org.axonframework.messaging.eventstreaming.Tag(TEST_KEY, null));
    }

    @Test
    void addToContextAddsTheGivenTagsToTheGivenContext() {
        Context testContext = Context.empty();
        Set<org.axonframework.messaging.eventstreaming.Tag> testTags = Set.of(new org.axonframework.messaging.eventstreaming.Tag(TEST_KEY, TEST_VALUE));

        testContext = org.axonframework.messaging.eventstreaming.Tag.addToContext(testContext, testTags);

        assertTrue(testContext.containsResource(org.axonframework.messaging.eventstreaming.Tag.RESOURCE_KEY));
    }

    @Test
    void fromContextReturnsAnEmptyOptionalWhenNoTagsArePresent() {
        Context testContext = Context.empty();

        Optional<Set<org.axonframework.messaging.eventstreaming.Tag>> result = org.axonframework.messaging.eventstreaming.Tag.fromContext(testContext);

        assertTrue(result.isEmpty());
    }

    @Test
    void fromContextReturnsAnOptionalWithTheContainedTags() {
        Context testContext = Context.empty();
        Set<org.axonframework.messaging.eventstreaming.Tag> testTags = Set.of(new org.axonframework.messaging.eventstreaming.Tag(TEST_KEY, TEST_VALUE));

        testContext = org.axonframework.messaging.eventstreaming.Tag.addToContext(testContext, testTags);

        Optional<Set<org.axonframework.messaging.eventstreaming.Tag>> result = Tag.fromContext(testContext);

        assertFalse(result.isEmpty());
        assertEquals(testTags, result.get());
    }
}
