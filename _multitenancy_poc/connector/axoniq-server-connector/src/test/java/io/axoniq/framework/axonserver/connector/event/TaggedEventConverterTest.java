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

package io.axoniq.framework.axonserver.connector.event;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.event.dcb.Event;
import io.axoniq.axonserver.grpc.event.dcb.TaggedEvent;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link TaggedEventConverter}.
 *
 * @author Steven van Beelen
 */
class TaggedEventConverterTest {

    private static final String EVENT_ID = UUID.randomUUID().toString();
    private static final Long EVENT_TIMESTAMP = Instant.now().toEpochMilli();
    private static final String EVENT_NAME = "event-name";
    private static final String EVENT_VERSION = "event-version";
    private static final MessageType EVENT_TYPE = new MessageType(EVENT_NAME, EVENT_VERSION);
    private static final Map<String, String> EVENT_METADATA = Map.of("String", "Lorem Ipsum");

    private Converter converter;

    private TaggedEventConverter testSubject;

    private TestEvent eventPayload;
    private byte[] eventPayloadByteArray;

    @BeforeEach
    void setUp() {
        converter = spy(new JacksonConverter());

        testSubject = new TaggedEventConverter(new DelegatingEventConverter(converter));

        eventPayload = new TestEvent("Lorem Ipsum", 42, List.of(true, false));
        eventPayloadByteArray = converter.convert(eventPayload, byte[].class);

        Mockito.clearInvocations(converter);
    }

    @Test
    void throwsNullPointerExceptionForNullConverter() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new TaggedEventConverter(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void convertTaggedEventMessageThrowsNullPointerExceptionForNullTaggedEventMessage() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> testSubject.convertTaggedEventMessage(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void convertTaggedEventMessageWorksAsExpected() {
        // given...
        EventMessage eventMessage = new GenericEventMessage(
                EVENT_ID, EVENT_TYPE, eventPayload, EVENT_METADATA, Instant.ofEpochMilli(EVENT_TIMESTAMP)
        );
        Set<Tag> tags = Set.of(Tag.of("key", "value"), Tag.of("key2", "value2"), Tag.of("key3", "value3"));
        TaggedEventMessage<EventMessage> taggedEventMessage =
                new GenericTaggedEventMessage<>(eventMessage, tags);
        // when...
        TaggedEvent result = testSubject.convertTaggedEventMessage(taggedEventMessage);
        // then...
        Event resultEvent = result.getEvent();
        assertThat(resultEvent.getIdentifier()).isEqualTo(EVENT_ID);
        assertThat(resultEvent.getTimestamp()).isEqualTo(EVENT_TIMESTAMP);
        assertThat(resultEvent.getName()).isEqualTo(EVENT_NAME);
        assertThat(resultEvent.getVersion()).isEqualTo(EVENT_VERSION);
        verify(converter).convert(eventPayload, (Type) byte[].class);
        assertThat(resultEvent.getPayload().toByteArray()).containsExactly(eventPayloadByteArray);
        Map<String, String> resultMetadata = resultEvent.getMetadataMap();
        assertThat(resultMetadata)
                .hasSize(1)
                .containsEntry("String", "Lorem Ipsum");
        List<io.axoniq.axonserver.grpc.event.dcb.Tag> tagList = result.getTagList();
        assertThat(tagList)
                .hasSize(3)
                .contains(
                    io.axoniq.axonserver.grpc.event.dcb.Tag.newBuilder()
                                                       .setKey(ByteString.copyFrom("key", StandardCharsets.UTF_8))
                                                       .setValue(ByteString.copyFrom("value", StandardCharsets.UTF_8))
                                                       .build()
                )
                .contains(
                    io.axoniq.axonserver.grpc.event.dcb.Tag.newBuilder()
                                                       .setKey(ByteString.copyFrom("key2", StandardCharsets.UTF_8))
                                                       .setValue(ByteString.copyFrom("value2", StandardCharsets.UTF_8))
                                                       .build()
                )
                .contains(
                    io.axoniq.axonserver.grpc.event.dcb.Tag.newBuilder()
                                                       .setKey(ByteString.copyFrom("key3", StandardCharsets.UTF_8))
                                                       .setValue(ByteString.copyFrom("value3", StandardCharsets.UTF_8))
                                                       .build()
                );
    }

    @Test
    void convertTaggedEventMessageConvertsAnyTypeOfMetadata() {
        // given...
        Metadata metadata = Metadata.from(Map.of(
                "String", "Lorem Ipsum",
                "Double", "3.53d",
                "Float", "3.53f",
                "Long", "42L",
                "Integer", "42",
                "Short", "42",
                "Byte", "4",
                "Boolean", "false"
        ));
        EventMessage eventMessage = new GenericEventMessage(EVENT_TYPE, eventPayload, metadata);
        TaggedEventMessage<EventMessage> taggedEventMessage =
                new GenericTaggedEventMessage<>(eventMessage, Set.of(Tag.of("key", "value")));
        // when...
        Map<String, String> result = testSubject.convertTaggedEventMessage(taggedEventMessage)
                                                .getEvent()
                                                .getMetadataMap();
        // then...
        assertThat(result).hasSize(8)
                .containsEntry("String", "Lorem Ipsum")
                .containsEntry("Double", "3.53d")
                .containsEntry("Float", "3.53f")
                .containsEntry("Long", "42L")
                .containsEntry("Integer", "42")
                .containsEntry("Short", "42")
                .containsEntry("Byte", "4")
                .containsEntry("Boolean", "false");
    }

    @Test
    void convertEventThrowsNullPointerExceptionForNullEvent() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> testSubject.convertEvent(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void convertEventWorksAsExpectedAndAttachesConverterToMessage() {
        // given...
        Event testEvent = Event.newBuilder()
                               .setIdentifier(EVENT_ID)
                               .setTimestamp(EVENT_TIMESTAMP)
                               .setName(EVENT_NAME)
                               .setVersion(EVENT_VERSION)
                               .setPayload(ByteString.copyFrom(eventPayloadByteArray))
                               .putAllMetadata(EVENT_METADATA)
                               .build();
        // when...
        EventMessage result = testSubject.convertEvent(testEvent);
        // then...
        assertThat(result.identifier()).isEqualTo(EVENT_ID);
        assertThat(result.type()).isEqualTo(EVENT_TYPE);
        assertThat(result.payloadAs(byte[].class)).containsExactly(eventPayloadByteArray);
        assertThat(result.payloadAs(TestEvent.class)).isEqualTo(eventPayload);
        assertThat(result.metadata()).isEqualTo(EVENT_METADATA);
        assertThat(result.timestamp().toEpochMilli()).isEqualTo(EVENT_TIMESTAMP);

        verify(converter).convert(eventPayloadByteArray, (Type) TestEvent.class);
    }

    private record TestEvent(String stringState, Integer intState, List<Boolean> booleanState) {

    }
}