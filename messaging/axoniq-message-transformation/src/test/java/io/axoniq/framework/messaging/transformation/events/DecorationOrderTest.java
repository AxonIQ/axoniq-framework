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
class DecorationOrderTest {

    @Test
    void transforming_event_store_decoration_order_is_outer_than_intercepting_event_store() {
        // given -- both constants are public static finals defined at class level

        // when / then -- ascending order applied first; higher order wraps lower one
        assertThat(TransformingEventStore.DECORATION_ORDER)
                .isGreaterThan(InterceptingEventStore.DECORATION_ORDER);
    }

    @Test
    void transforming_event_store_uses_min_value_plus_1000_for_headroom() {
        // given / when / then -- the +1000 offset (vs InterceptingEventStore's +50) leaves
        // a deliberate gap so users / framework can slot other decorators between them
        assertThat(TransformingEventStore.DECORATION_ORDER).isEqualTo(Integer.MIN_VALUE + 1000);
        assertThat(InterceptingEventStore.DECORATION_ORDER).isEqualTo(Integer.MIN_VALUE + 50);
    }
}
