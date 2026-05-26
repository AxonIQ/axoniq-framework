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

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class NoOpSpanFactoryTest {

    private final NoOpSpanFactory testSubject = NoOpSpanFactory.INSTANCE;
    private final Message message = EventTestUtils.asEventMessage("payload");

    @Test
    void createdSpansCanBeStartedAndClosedWithoutError() {
        // given / when
        Span span = testSubject.createDispatchSpan("op", message, null);

        // then
        assertThatCode(() -> {
            SpanScope scope = span.start();
            scope.close();
        }).doesNotThrowAnyException();
    }

    @Test
    void runExecutesTheGivenBlock() {
        // given
        AtomicBoolean executed = new AtomicBoolean(false);

        // when
        testSubject.createInternalSpan("op").run(() -> executed.set(true));

        // then
        assertThat(executed).isTrue();
    }

    @Test
    void runSupplierReturnsTheSuppliedValue() {
        // when
        String result = testSubject.createHandlerSpan("op", message, null).runSupplier(() -> "value");

        // then
        assertThat(result).isEqualTo("value");
    }

    @Test
    void propagateContextReturnsTheSameMessageInstance() {
        // when
        Message result = testSubject.propagateContext(message);

        // then
        assertThat(result).isSameAs(message);
    }

    @Test
    void addAttributeAndRecordExceptionAreNoOpsReturningTheSpan() {
        // given
        Span span = testSubject.createInternalSpan("op");

        // when / then
        assertThat(span.addAttribute("k", "v")).isSameAs(span);
        assertThat(span.recordException(new RuntimeException())).isSameAs(span);
    }
}
