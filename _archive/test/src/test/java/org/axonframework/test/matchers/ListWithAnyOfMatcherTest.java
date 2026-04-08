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

import org.axonframework.messaging.eventhandling.EventMessage;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.jupiter.api.*;
import org.mockito.invocation.*;
import org.mockito.stubbing.*;

import java.util.Arrays;
import java.util.List;

import static org.axonframework.test.matchers.Matchers.listWithAnyOf;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author Allard Buijze
 */
class ListWithAnyOfMatcherTest {

    private Matcher<EventMessage> mockMatcher1;
    private Matcher<EventMessage> mockMatcher2;
    private Matcher<EventMessage> mockMatcher3;
    private Matcher<List<EventMessage>> testSubject;
    private StubEvent stubEvent1;
    private StubEvent stubEvent2;

    @SuppressWarnings({"unchecked"})
    @BeforeEach
    void setUp() {
        mockMatcher1 = mock(Matcher.class);
        mockMatcher2 = mock(Matcher.class);
        mockMatcher3 = mock(Matcher.class);
        testSubject = listWithAnyOf(mockMatcher1, mockMatcher2, mockMatcher3);
        stubEvent1 = new StubEvent();
        stubEvent2 = new StubEvent();
        when(mockMatcher1.matches(any())).thenReturn(true);
        when(mockMatcher2.matches(any())).thenReturn(true);
        when(mockMatcher3.matches(any())).thenReturn(true);
    }

    @Test
    void match_FullMatch() {
        assertTrue(testSubject.matches(Arrays.asList(stubEvent1, stubEvent2)));

        verify(mockMatcher1).matches(stubEvent1);
        verify(mockMatcher1).matches(stubEvent2);
        verify(mockMatcher2).matches(stubEvent1);
        verify(mockMatcher2).matches(stubEvent2);
        verify(mockMatcher3).matches(stubEvent1);
        verify(mockMatcher3).matches(stubEvent2);
    }

    @Test
    void match_OnlyOneEventMatches() {
        when(mockMatcher1.matches(stubEvent1)).thenReturn(false);
        when(mockMatcher2.matches(stubEvent1)).thenReturn(false);
        when(mockMatcher3.matches(stubEvent1)).thenReturn(false);

        assertTrue(testSubject.matches(Arrays.asList(stubEvent1, stubEvent2)));

        verify(mockMatcher1).matches(stubEvent1);
        verify(mockMatcher1).matches(stubEvent2);
        verify(mockMatcher2).matches(stubEvent1);
        verify(mockMatcher2).matches(stubEvent2);
        verify(mockMatcher3).matches(stubEvent1);
        verify(mockMatcher3).matches(stubEvent2);
    }

    @Test
    void match_NoMatches() {
        when(mockMatcher1.matches(any())).thenReturn(false);
        when(mockMatcher2.matches(any())).thenReturn(false);
        when(mockMatcher3.matches(any())).thenReturn(false);

        assertFalse(testSubject.matches(Arrays.asList(stubEvent1, stubEvent2)));

        verify(mockMatcher1).matches(stubEvent1);
        verify(mockMatcher1).matches(stubEvent2);
        verify(mockMatcher2).matches(stubEvent1);
        verify(mockMatcher2).matches(stubEvent2);
        verify(mockMatcher3).matches(stubEvent1);
        verify(mockMatcher3).matches(stubEvent2);
    }

    @Test
    void match_OneMatcherDoesNotMatch() {
        when(mockMatcher1.matches(any())).thenReturn(false);
        when(mockMatcher2.matches(stubEvent1)).thenReturn(false);
        when(mockMatcher3.matches(stubEvent1)).thenReturn(false);

        assertTrue(testSubject.matches(Arrays.asList(stubEvent1, stubEvent2)));

        verify(mockMatcher1).matches(stubEvent1);
        verify(mockMatcher1).matches(stubEvent2);
        verify(mockMatcher2).matches(stubEvent1);
        verify(mockMatcher2).matches(stubEvent2);
        verify(mockMatcher3).matches(stubEvent1);
        verify(mockMatcher3).matches(stubEvent2);
    }

    @Test
    void describe() {
        testSubject.matches(Arrays.asList(stubEvent1, stubEvent2));

        doAnswer(new DescribingAnswer("A")).when(mockMatcher1).describeTo(isA(Description.class));
        doAnswer(new DescribingAnswer("B")).when(mockMatcher2).describeTo(isA(Description.class));
        doAnswer(new DescribingAnswer("C")).when(mockMatcher3).describeTo(isA(Description.class));
        StringDescription description = new StringDescription();
        testSubject.describeTo(description);
        String actual = description.toString();
        assertEquals("list with any of: <A>, <B> or <C>", actual);
    }

    @Test
    void describe_OneMatcherFailed() {
        when(mockMatcher1.matches(any())).thenReturn(false);
        when(mockMatcher2.matches(any())).thenReturn(false);
        when(mockMatcher3.matches(any())).thenReturn(false);

        testSubject.matches(Arrays.asList(stubEvent1, stubEvent2));

        doAnswer(new DescribingAnswer("A")).when(mockMatcher1).describeTo(isA(Description.class));
        doAnswer(new DescribingAnswer("B")).when(mockMatcher2).describeTo(isA(Description.class));
        doAnswer(new DescribingAnswer("C")).when(mockMatcher3).describeTo(isA(Description.class));
        StringDescription description = new StringDescription();
        testSubject.describeTo(description);
        String actual = description.toString();
        assertEquals("list with any of: <A> (NO MATCH), <B> (NO MATCH) or <C> (NO MATCH)", actual);
    }

    private static class DescribingAnswer implements Answer<Object> {
        private String description;

        public DescribingAnswer(String description) {
            this.description = description;
        }

        @Override
        public Object answer(InvocationOnMock invocation) {
            Description descriptionParameter = (Description) invocation.getArguments()[0];
            descriptionParameter.appendText(this.description);
            return Void.class;
        }
    }
}
