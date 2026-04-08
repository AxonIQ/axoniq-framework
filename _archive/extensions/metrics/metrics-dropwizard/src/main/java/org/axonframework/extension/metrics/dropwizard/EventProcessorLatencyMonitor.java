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
import io.dropwizard.metrics5.MetricSet;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.monitoring.MessageMonitor;
import org.axonframework.messaging.monitoring.NoOpMessageMonitorCallback;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link MessageMonitor} implementation dedicated to {@link EventMessage EventMessages}.
 * <p>
 * This monitor defines the latency between the {@link EventMessage#timestamp()} and the {@link Clock#instant()}. Doing
 * so, it depicts the latency from when an event was published compared to when an {@link EventProcessor} processes the
 * event to clarify how far behind an {@code EventProcessor} is.
 * <p>
 * Do note that a replay (as triggered through {@link StreamingEventProcessor#resetTokens()}, for example) will cause
 * this metric to bump up due to the processor handling old events.
 *
 * @author Marijn van Zelst
 * @author Allard Buijze
 * @since 3.0
 */
public class EventProcessorLatencyMonitor implements MessageMonitor<EventMessage>, MetricSet {

    private final Clock clock;
    private final AtomicLong processTime = new AtomicLong();

    /**
     * Construct an {@link EventProcessorLatencyMonitor} using a {@link Clock#systemUTC()}.
     */
    public EventProcessorLatencyMonitor() {
        this(Clock.systemUTC());
    }

    /**
     * Construct an {@link EventProcessorLatencyMonitor} using the given {@code clock}.
     *
     * @param clock defines the {@link Clock} used by this {@link MessageMonitor} implementation
     */
    public EventProcessorLatencyMonitor(Clock clock) {
        this.clock = clock;
    }

    @Override
    public MonitorCallback onMessageIngested(EventMessage message) {
        //noinspection ConstantConditions
        if (message != null) {
            this.processTime.set(Duration.between(message.timestamp(), clock.instant()).toMillis());
        }
        return NoOpMessageMonitorCallback.INSTANCE;
    }

    @Override
    public Map<MetricName, Metric> getMetrics() {
        Map<MetricName, Metric> metrics = new HashMap<>();
        metrics.put(MetricName.build("latency"), (Gauge<Long>) processTime::get); // NOSONAR
        return metrics;
    }
}
