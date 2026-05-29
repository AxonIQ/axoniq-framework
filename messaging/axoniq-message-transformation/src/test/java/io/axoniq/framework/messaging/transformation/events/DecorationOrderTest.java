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

package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the decoration ordering invariant: {@code TransformingEventStore} runs outer
 * (later) than {@code InterceptingEventStore} so its chain transforms before any
 * handler-side interceptor sees the event. Higher order value = outer wrap; see
 * {@code DefaultComponentRegistry} decorator-application loop.
 */
final class DecorationOrderTest {

    @Test
    void decorationOrderIsHigherThanInterceptingEventStore() {
        assertThat(TransformingEventStore.DECORATION_ORDER)
                .isGreaterThan(InterceptingEventStore.DECORATION_ORDER);
    }

    @Test
    void decorationOrderLeavesHeadroomBetweenItselfAndInterceptingEventStore() {
        // Enough room between the two orders for users or the framework to slot additional
        // decorators in between without colliding with either constant.
        int gap = TransformingEventStore.DECORATION_ORDER - InterceptingEventStore.DECORATION_ORDER;

        assertThat(gap)
                .as("decorators sit at least 500 ticks apart so intermediate decorators can be inserted")
                .isGreaterThanOrEqualTo(500);
    }
}
