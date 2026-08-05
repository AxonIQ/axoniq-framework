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

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.event.AppendEventsTransaction;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.FluxUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AggregateBasedAxonServerEventStorageEngine} through mocking.
 * <p>
 * For an integration tests, use the {@link AggregateBasedAxonServerEventStorageEngineIT} instead.
 *
 * @author Steven van Beelen
 */
class AggregateBasedAxonServerEventStorageEngineTest {

    private static final String EVENT_NAME = "test-event";

    private EventChannel eventChannel;
    private EventStream eventStream;

    private AggregateBasedAxonServerEventStorageEngine testSubject;

    @BeforeEach
    void setUp() {
        AxonServerConnection connection = mock(AxonServerConnection.class);
        eventChannel = mock(EventChannel.class);
        eventStream = mock(EventStream.class);

        when(connection.eventChannel()).thenReturn(eventChannel);
        when(eventChannel.openStream(anyLong(), anyInt())).thenReturn(eventStream);
        when(eventStream.getError()).thenReturn(Optional.empty());
        when(eventStream.isClosed()).thenReturn(true);

        testSubject = new AggregateBasedAxonServerEventStorageEngine(
                connection,
                new DelegatingEventConverter(new JacksonConverter())
        );
    }

    @Test
    void defaultResolverSubstitutesEmptyRevisionWithMissingVersionDefault() {
        // given
        Event storedEvent = Event.newBuilder()
                                 .setMessageIdentifier(UUID.randomUUID().toString())
                                 .setPayload(SerializedObject.newBuilder()
                                                             .setType(EVENT_NAME)
                                                             // no setRevision() — proto default is empty string
                                                             .build())
                                 .setTimestamp(Instant.now().toEpochMilli())
                                 .setAggregateIdentifier(UUID.randomUUID().toString())
                                 .setAggregateType("TEST_AGGREGATE")
                                 .setAggregateSequenceNumber(0L)
                                 .build();
        EventWithToken eventWithToken = EventWithToken.newBuilder()
                                                      .setEvent(storedEvent)
                                                      .setToken(0L)
                                                      .build();
        when(eventStream.peek()).thenReturn(eventWithToken, null);
        when(eventStream.nextIfAvailable()).thenReturn(eventWithToken, null);
        StreamingCondition condition = StreamingCondition.startingFrom(new GlobalSequenceTrackingToken(0L));
        // when / then
        StepVerifier.create(FluxUtils.of(testSubject.stream(condition)))
                    .assertNext(entry -> {
                        assertThat(entry.message().type().name()).isEqualTo(EVENT_NAME);
                        assertThat(entry.message().type().version())
                                .isEqualTo(EventTypeResolver.MISSING_VERSION_DEFAULT);
                    })
                    .verifyComplete();
    }

    @Test
    void appendEventsIncludesEventMetadataOnTheAppendedEvent() {
        // given
        AppendEventsTransaction transaction = mock(AppendEventsTransaction.class);
        when(eventChannel.startAppendEventsTransaction()).thenReturn(transaction);

        Metadata metadata = Metadata.from(Map.of("correlationId", "correlation-1"));
        EventMessage event = new GenericEventMessage(new MessageType(EVENT_NAME), "payload", metadata);
        TaggedEventMessage<?> taggedEvent =
                new GenericTaggedEventMessage<>(event, Set.of(Tag.of("TEST_AGGREGATE", UUID.randomUUID().toString())));

        // when
        testSubject.appendEvents(AppendCondition.none(), null, List.of(taggedEvent));

        // then
        ArgumentCaptor<Event> appendedEvent = ArgumentCaptor.forClass(Event.class);
        verify(transaction).appendEvent(appendedEvent.capture());
        assertThat(appendedEvent.getValue().getMetaDataMap())
                .containsEntry("correlationId", MetaDataValue.newBuilder().setTextValue("correlation-1").build());
    }
}
