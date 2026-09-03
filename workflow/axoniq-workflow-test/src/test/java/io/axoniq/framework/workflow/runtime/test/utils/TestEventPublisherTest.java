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
package io.axoniq.framework.workflow.runtime.test.utils;

import org.jspecify.annotations.Nullable;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.same;

/**
 * Tests for {@link TestEventPublisher}.
 *
 * @author Simon Zambrovski
 */
class TestEventPublisherTest {

    private final EventSink eventSink = mock(EventSink.class);
    private final MessageTypeResolver messageTypeResolver = mock(MessageTypeResolver.class);
    private final EventConverter converter = mock(EventConverter.class);
    private final Clock clock = Clock.fixed(Instant.parse("2026-06-19T10:15:30Z"), ZoneOffset.UTC);
    private final IdGenerator idGenerator = mock(IdGenerator.class);

    @Test
    void publishWrapsPayloadAsEventMessage() {
        CompletableFuture<Void> completion = CompletableFuture.completedFuture(null);
        when(eventSink.publish(isNull(), any(EventMessage.class))).thenReturn(completion);
        Map<String, @Nullable Object> payload = Map.of("orderId", "order-1");
        MessageType messageType = new MessageType("OrderCreated");
        when(messageTypeResolver.resolveOrThrow(payload)).thenReturn(messageType);
        when(idGenerator.next()).thenReturn("event-1");
        TestEventPublisher publisher = publisher();

        CompletableFuture<Void> result = publisher.publish(payload);

        ArgumentCaptor<EventMessage> eventCaptor = ArgumentCaptor.forClass(EventMessage.class);
        verify(eventSink).publish(isNull(), eventCaptor.capture());
        EventMessage message = eventCaptor.getValue();
        assertThat(result).isSameAs(completion);
        assertThat(message.identifier()).isEqualTo("event-1");
        assertThat(message.type()).isEqualTo(messageType);
        assertThat(message.payload()).isSameAs(payload);
        assertThat(message.timestamp()).isEqualTo(clock.instant());
        assertThat(message.metadata()).isEmpty();
    }

    @Test
    void publishUsesExistingEventMessage() {
        CompletableFuture<Void> completion = CompletableFuture.completedFuture(null);
        EventMessage message = mock(EventMessage.class);
        when(message.type()).thenReturn(new MessageType("ExistingEvent"));
        when(message.payloadAs(Map.class)).thenReturn(Map.of("orderId", "order-1"));
        when(eventSink.publish(isNull(), same(message))).thenReturn(completion);
        TestEventPublisher publisher = publisher();

        CompletableFuture<Void> result = publisher.publish(message);

        assertThat(result).isSameAs(completion);
        verify(eventSink).publish(isNull(), same(message));
        verifyNoInteractions(messageTypeResolver, converter, idGenerator);
    }

    private TestEventPublisher publisher() {
        return new TestEventPublisher(eventSink, messageTypeResolver, converter, clock, idGenerator);
    }
}
