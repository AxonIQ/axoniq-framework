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

package org.axonframework.extension.metrics.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.monitoring.MessageMonitor;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.axonframework.extension.metrics.micrometer.TagsUtil.MESSAGE_TYPE_TAG;
import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link CapacityMonitor}.
 *
 * @author Martijn Zelst
 */
class CapacityMonitorTest {

    @Test
    void capacityWithoutTags() {
        MockClock testClock = new MockClock();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        CapacityMonitor testSubject = CapacityMonitor.buildMonitor("1", meterRegistry, 1, TimeUnit.SECONDS, testClock);

        EventMessage foo = asEventMessage(1);
        EventMessage bar = asEventMessage("bar");
        Map<? super Message, MessageMonitor.MonitorCallback> callbacks =
                testSubject.onMessagesIngested(Arrays.asList(foo, bar));

        testClock.addSeconds(1);

        callbacks.get(foo).reportSuccess();
        callbacks.get(bar).reportFailure(null);

        Gauge capacityGauge = meterRegistry.get("1.capacity").gauge();
        assertEquals(2, capacityGauge.value(), 0);
    }

    @Test
    void capacityWithPayloadTypeAsCustomTag() {
        MockClock testClock = new MockClock();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        CapacityMonitor testSubject = CapacityMonitor.buildMonitor(
                "1", meterRegistry, 1, TimeUnit.SECONDS, testClock,
                message -> Tags.of(MESSAGE_TYPE_TAG, message.payloadType().getSimpleName())
        );

        EventMessage foo = asEventMessage(1);
        EventMessage bar = asEventMessage("bar");
        Map<? super Message, MessageMonitor.MonitorCallback> callbacks =
                testSubject.onMessagesIngested(Arrays.asList(foo, bar));

        testClock.addSeconds(1);

        callbacks.get(foo).reportSuccess();
        callbacks.get(bar).reportFailure(null);

        Collection<Gauge> capacityGauges = meterRegistry.find("1.capacity").gauges();
        assertEquals(2, capacityGauges.size(), 0);
        assertTrue(capacityGauges.stream()
                                 .anyMatch(gauge -> Objects.equals(gauge.getId().getTag(MESSAGE_TYPE_TAG), "Integer")));
        assertTrue(capacityGauges.stream()
                                 .anyMatch(gauge -> Objects.equals(gauge.getId().getTag(MESSAGE_TYPE_TAG), "String")));
    }

    @Test
    void capacityWithMetadataAsCustomTag() {
        MockClock testClock = new MockClock();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        CapacityMonitor testSubject = CapacityMonitor.buildMonitor(
                "1", meterRegistry, 1, TimeUnit.SECONDS, testClock,
                message -> Tags.of(
                        "myPayloadType", message.payloadType().getSimpleName(),
                        "myMetadata", message.metadata().get("myMetadataKey")
                )
        );

        EventMessage foo = asEventMessage(1)
                .withMetadata(Collections.singletonMap("myMetadataKey", "myMetadataValue1"));
        EventMessage bar = asEventMessage("bar")
                .withMetadata(Collections.singletonMap("myMetadataKey", "myMetadataValue2"));
        Map<? super Message, MessageMonitor.MonitorCallback> callbacks =
                testSubject.onMessagesIngested(Arrays.asList(foo, bar));

        testClock.addSeconds(1);

        callbacks.get(foo).reportSuccess();
        callbacks.get(bar).reportFailure(null);

        Collection<Gauge> capacityGauges = meterRegistry.find("1.capacity").gauges();
        assertEquals(2, capacityGauges.size(), 0);
        assertTrue(capacityGauges.stream()
                                 .anyMatch(gauge -> Objects.equals(gauge.getId().getTag("myPayloadType"), "Integer")));
        assertTrue(capacityGauges.stream()
                                 .anyMatch(gauge -> Objects.equals(gauge.getId().getTag("myPayloadType"), "String")));
        assertTrue(capacityGauges.stream()
                                 .anyMatch(gauge -> Objects
                                         .equals(gauge.getId().getTag("myMetadata"), "myMetadataValue1")));
        assertTrue(capacityGauges.stream()
                                 .anyMatch(gauge -> Objects
                                         .equals(gauge.getId().getTag("myMetadata"), "myMetadataValue2")));
    }
}
