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

package org.axonframework.messaging.eventhandling.processing.streaming.pooled;

import org.axonframework.messaging.eventhandling.configuration.EventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SegmentChangeListener;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.common.FutureUtils.joinAndUnwrap;

class PooledStreamingEventProcessorConfigurationTest {

    @Test
    void addSegmentChangeListenerAddsListenersWithoutOverriding() {
        // given
        PooledStreamingEventProcessorConfiguration testSubject = new PooledStreamingEventProcessorConfiguration(
                new EventProcessorConfiguration("processorName", null)
        );
        AtomicInteger releaseInvocations = new AtomicInteger();
        testSubject.addSegmentChangeListener(SegmentChangeListener.runOnRelease(
                segment -> releaseInvocations.incrementAndGet()
        ));
        testSubject.addSegmentChangeListener(SegmentChangeListener.runOnRelease(
                segment -> releaseInvocations.incrementAndGet()
        ));

        // when
        joinAndUnwrap(testSubject.segmentChangeListener().onSegmentReleased(Segment.ROOT_SEGMENT));

        // then
        assertThat(releaseInvocations).hasValue(2);
    }
}
