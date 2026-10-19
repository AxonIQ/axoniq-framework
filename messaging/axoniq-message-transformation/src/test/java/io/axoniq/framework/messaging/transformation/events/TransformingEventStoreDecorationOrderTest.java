/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
final class TransformingEventStoreDecorationOrderTest {

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
