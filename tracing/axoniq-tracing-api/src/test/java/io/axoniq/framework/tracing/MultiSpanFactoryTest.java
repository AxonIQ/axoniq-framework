/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.tracing;

import io.axoniq.framework.tracing.support.TestSpanFactory;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiSpanFactoryTest {

    private TestSpanFactory first;
    private TestSpanFactory second;
    private MultiSpanFactory testSubject;

    private final Message message = EventTestUtils.asEventMessage("payload");

    @BeforeEach
    void setUp() {
        first = new TestSpanFactory();
        second = new TestSpanFactory();
        testSubject = new MultiSpanFactory(List.of(first, second));
    }

    @Test
    void startingAndClosingFansOutToEveryDelegate() {
        // when
        SpanScope scope = testSubject.createDispatchSpan("op", message, null).start();

        // then
        first.verifySpanActive("op");
        second.verifySpanActive("op");

        // when
        scope.close();

        // then
        first.verifySpanCompleted("op");
        second.verifySpanCompleted("op");
    }

    @Test
    void addAttributeFansOutToEveryDelegate() {
        // given
        Span span = testSubject.createHandlerSpan("op", message, null);
        span.start();

        // when
        span.addAttribute("key", "value");

        // then
        first.verifySpanHasAttributeValue("op", "key", "value");
        second.verifySpanHasAttributeValue("op", "key", "value");
    }

    @Test
    void emptyDelegateListIsRejected() {
        // when / then
        assertThatThrownBy(() -> new MultiSpanFactory(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void propagateContextReturnsAMessage() {
        // given
        testSubject.createDispatchSpan("op", message, null).start();

        // when
        Message result = testSubject.propagateContext(message);

        // then
        assertThat(result).isNotNull();
        first.verifySpanPropagated("op", message);
        second.verifySpanPropagated("op", message);
    }
}
