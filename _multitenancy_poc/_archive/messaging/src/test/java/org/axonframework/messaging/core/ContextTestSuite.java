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

package org.axonframework.messaging.core;

import org.axonframework.messaging.core.Context.ResourceKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite validating implementations of the {@link Context}.
 *
 * @param <C> The {@link Context} implementation under test.
 * @author Steven van Beelen
 */
public abstract class ContextTestSuite<C extends Context> {

    protected static final String EXPECTED_RESOURCE_VALUE = "testContext";
    protected static final ResourceKey<String> TEST_RESOURCE_KEY = ResourceKey.withLabel(EXPECTED_RESOURCE_VALUE);

    /**
     * Build a test subject of type {@code C} for this suite.
     *
     * @return A test subject of type {@code C} for this suite.
     */
    public abstract C testSubject();

    @Test
    void containsResourceReturnsAsExpected() {
        C testSubject = testSubject();

        assertFalse(testSubject.containsResource(TEST_RESOURCE_KEY));

        //noinspection unchecked
        C testSubjectWithResources = (C) testSubject.withResource(TEST_RESOURCE_KEY, EXPECTED_RESOURCE_VALUE);

        assertFalse(testSubject.containsResource(TEST_RESOURCE_KEY));
        assertTrue(testSubjectWithResources.containsResource(TEST_RESOURCE_KEY));
    }

    @Test
    void getResourceReturnsAsExpected() {
        C testSubject = testSubject();

        assertNull(testSubject.getResource(TEST_RESOURCE_KEY));

        //noinspection unchecked
        C testSubjectWithResources = (C) testSubject.withResource(TEST_RESOURCE_KEY, EXPECTED_RESOURCE_VALUE);

        assertNull(testSubject.getResource(TEST_RESOURCE_KEY));
        assertEquals(EXPECTED_RESOURCE_VALUE, testSubjectWithResources.getResource(TEST_RESOURCE_KEY));
    }

    @Test
    void withResourceReturnsNewContextInstanceWithTheExpectedResources() {
        String expectedResourceValueTwo = "resourceTwo";
        ResourceKey<String> testResourceKeyTwo = ResourceKey.withLabel(expectedResourceValueTwo);

        C testSubject = testSubject();

        assertNull(testSubject.getResource(TEST_RESOURCE_KEY));

        //noinspection unchecked
        C resultOne = (C) testSubject.withResource(TEST_RESOURCE_KEY, EXPECTED_RESOURCE_VALUE);

        assertNotEquals(testSubject, resultOne);
        assertEquals(EXPECTED_RESOURCE_VALUE, resultOne.getResource(TEST_RESOURCE_KEY));

        //noinspection unchecked
        C resultTwo = (C) resultOne.withResource(testResourceKeyTwo, expectedResourceValueTwo);

        assertNotEquals(testSubject, resultTwo);
        assertNotEquals(resultOne, resultTwo);
        assertEquals(EXPECTED_RESOURCE_VALUE, resultTwo.getResource(TEST_RESOURCE_KEY));
        assertEquals(expectedResourceValueTwo, resultTwo.getResource(testResourceKeyTwo));
    }
}