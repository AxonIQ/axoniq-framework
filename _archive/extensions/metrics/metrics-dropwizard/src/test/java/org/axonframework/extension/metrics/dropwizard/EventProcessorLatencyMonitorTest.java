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

package org.axonframework.extension.metrics.dropwizard;

import io.dropwizard.metrics5.Gauge;
import io.dropwizard.metrics5.Metric;
import io.dropwizard.metrics5.MetricName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.monitoring.MessageMonitor;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link EventProcessorLatencyMonitor}.
 *
 * @author Marijn van Zelst
 */
class EventProcessorLatencyMonitorTest {

    private final EventProcessorLatencyMonitor testSubject = new EventProcessorLatencyMonitor();

    private final Map<MetricName, Metric> metricSet = testSubject.getMetrics();

    @Test
    void messages() {
        EventMessage firstEventMessage = mock(EventMessage.class);
        when(firstEventMessage.timestamp()).thenReturn(Instant.ofEpochMilli(0));

        EventMessage secondEventMessage = mock(EventMessage.class);
        when(secondEventMessage.timestamp()).thenReturn(Instant.now().minusMillis(1000));

        Map<? super EventMessage, MessageMonitor.MonitorCallback> callbacks = testSubject.onMessagesIngested(
                Arrays.asList(firstEventMessage, secondEventMessage)
        );

        callbacks.get(firstEventMessage).reportSuccess();

        //noinspection unchecked
        Gauge<Long> latency = (Gauge<Long>) metricSet.get(MetricName.build("latency"));

        assertTrue(latency.getValue() >= 1000);
    }

    @Test
    void failureMessage() {
        EventMessage firstEventMessage = mock(EventMessage.class);
        when(firstEventMessage.timestamp()).thenReturn(Instant.ofEpochMilli(0));

        EventMessage secondEventMessage = mock(EventMessage.class);
        when(secondEventMessage.timestamp()).thenReturn(Instant.now().minusMillis(1000));

        Map<? super EventMessage, MessageMonitor.MonitorCallback> callbacks = testSubject.onMessagesIngested(
                Arrays.asList(firstEventMessage, secondEventMessage)
        );

        callbacks.get(firstEventMessage).reportFailure(null);

        //noinspection unchecked
        Gauge<Long> latency = (Gauge<Long>) metricSet.get(MetricName.build("latency"));

        assertTrue(latency.getValue() >= 1000);
    }

    @Test
    void nullMessage() {
        testSubject.onMessageIngested(null).reportSuccess();

        //noinspection unchecked
        Gauge<Long> latency = (Gauge<Long>) metricSet.get(MetricName.build("latency"));

        assertEquals(0, latency.getValue(), 0);
    }
}
