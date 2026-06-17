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

import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.eventOf;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedMessageTypeResolver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavior of {@code EventTransformation.rename(source, target)}: input validation, applying a rename when reading
 * the event stream, and widening a type-filtered read so renamed events stored under the old name are still
 * fetched. A rename passes the stored payload through untouched and lets the framework own the output identity, so
 * neither the {@link MessageConverter} nor the {@code MessageTypeResolver} is consulted. Both stand-ins fail loudly
 * if invoked.
 */
final class EventTransformerChainRenameTest {

    private static final MessageType COURSE_OPENED = new MessageType("com.example.CourseOpened", "1.0.0");
    private static final MessageType COURSE_CREATED = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType COURSE_CREATED_V2 = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType COURSE_REGISTERED = new MessageType("com.example.CourseRegistered", "1.0.0");
    private static final MessageType COURSE_V1 = new MessageType("com.example.Course", "1.0.0");
    private static final MessageType COURSE_V2 = new MessageType("com.example.Course", "2.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();

    @Nested
    final class InputValidation {

        @Test
        void renameRejectsNullSource() {
            assertThatThrownBy(() -> EventTransformation.rename(null, COURSE_CREATED))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("source");
        }

        @Test
        void renameRejectsNullTarget() {
            assertThatThrownBy(() -> EventTransformation.rename(COURSE_OPENED, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("target");
        }

        @Test
        void renameRejectsIdenticalSourceAndTarget() {
            // a no-op rename to the same identity is rejected up front rather than looping at read time
            MessageType sameAsSource = new MessageType("com.example.CourseOpened", "1.0.0");
            assertThatThrownBy(() -> EventTransformation.rename(COURSE_OPENED, sameAsSource))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("identical");
        }
    }

    @Nested
    final class ReadingRenamedEvents {

        @Test
        void renamedEventReachesTheNewNameAndNoLongerMatchesTheOld() {
            // given a rename of CourseOpened@1.0.0 to CourseCreated@1.0.0 (a qualified-name change
            // that a payload-mapping transformation would reject) and a stored CourseOpened event
            CoursePayload payload = new CoursePayload("CS-101");
            EventTransformation rename = EventTransformation.rename(COURSE_OPENED, COURSE_CREATED);
            EventTransformerChain chain = EventTransformerChain.builder().register(rename).build();
            EventMessage storedCourseOpened = eventOf(COURSE_OPENED, payload);

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedCourseOpened)),
                    null, CONVERTER, neverInvokedMessageTypeResolver()));

            // then the event surfaces under the new identity with the same payload instance, so
            // handlers for the new name receive it, and handlers still on the old name no longer match
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(COURSE_CREATED);
            assertThat(outputs.getFirst().payload()).isSameAs(payload);
        }

        @Test
        void versionBumpReachesTheNewVersionWithItsPayloadUnchanged() {
            // given a rename that only bumps the version (same qualified name, version 1.0.0 to 2.0.0)
            CoursePayload payload = new CoursePayload("CS-101");
            EventTransformation rename = EventTransformation.rename(COURSE_V1, COURSE_V2);
            EventTransformerChain chain = EventTransformerChain.builder().register(rename).build();
            EventMessage storedV1 = eventOf(COURSE_V1, payload);

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedV1)),
                    null, CONVERTER, neverInvokedMessageTypeResolver()));

            // then the event surfaces under the new version carrying the very same payload instance
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(COURSE_V2);
            assertThat(outputs.getFirst().payload()).isSameAs(payload);
        }

        @Test
        void renameChangesBothNameAndVersionInOneStep() {
            // given a rename that changes the qualified name and the version at once
            CoursePayload payload = new CoursePayload("CS-101");
            EventTransformation rename = EventTransformation.rename(COURSE_OPENED, COURSE_CREATED_V2);
            EventTransformerChain chain = EventTransformerChain.builder().register(rename).build();
            EventMessage storedCourseOpened = eventOf(COURSE_OPENED, payload);

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedCourseOpened)),
                    null, CONVERTER, neverInvokedMessageTypeResolver()));

            // then the event surfaces under the new name and version with the payload unchanged
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(COURSE_CREATED_V2);
            assertThat(outputs.getFirst().payload()).isSameAs(payload);
        }

        @Test
        void chainedRenamesReadTheOldestEventBackUnderTheNewestName() {
            // given two renames registered across successive refactorings: CourseOpened became
            // CourseCreated, which later became CourseRegistered
            CoursePayload payload = new CoursePayload("CS-101");
            EventTransformation openedToCreated = EventTransformation.rename(COURSE_OPENED, COURSE_CREATED);
            EventTransformation createdToRegistered = EventTransformation.rename(COURSE_CREATED, COURSE_REGISTERED);
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(openedToCreated)
                                                               .register(createdToRegistered)
                                                               .build();
            EventMessage storedCourseOpened = eventOf(COURSE_OPENED, payload);

            // when the chain reads an event still stored under the oldest name
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedCourseOpened)),
                    null, CONVERTER, neverInvokedMessageTypeResolver()));

            // then it hops through every rename and surfaces under the newest identity, payload untouched
            assertThat(outputs).hasSize(1);
            assertThat(outputs.getFirst().type()).isEqualTo(COURSE_REGISTERED);
            assertThat(outputs.getFirst().payload()).isSameAs(payload);
        }
    }

    @Nested
    final class WideningTypeFilteredReads {

        @Test
        void readForTheNewNameAlsoFetchesEventsStoredUnderTheOldName() {
            // given a rename of CourseOpened to CourseCreated
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.rename(COURSE_OPENED,
                                                                                                    COURSE_CREATED))
                                                               .build();
            // and a read filtered to the new name only
            EventCriteria readForNewName =
                    EventCriteria.havingAnyTag().andBeingOneOfTypes(COURSE_CREATED.qualifiedName());

            // when the chain widens the read
            EventCriteria widened = chain.widen(readForNewName);

            // then the read also fetches events still stored under the old name
            assertThat(widened.flatten())
                    .singleElement()
                    .satisfies(criterion -> assertThat(criterion.types())
                            .containsExactlyInAnyOrder(COURSE_CREATED.qualifiedName(), COURSE_OPENED.qualifiedName()));
        }

        @Test
        void readForTheNewestNameFetchesEveryOlderNameInTheChain() {
            // given a two-step rename chain: CourseOpened became CourseCreated became CourseRegistered
            EventTransformation openedToCreated = EventTransformation.rename(COURSE_OPENED, COURSE_CREATED);
            EventTransformation createdToRegistered = EventTransformation.rename(COURSE_CREATED, COURSE_REGISTERED);
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(openedToCreated)
                                                               .register(createdToRegistered)
                                                               .build();
            // and a read filtered to the newest name only
            EventCriteria readForNewestName =
                    EventCriteria.havingAnyTag().andBeingOneOfTypes(COURSE_REGISTERED.qualifiedName());

            // when the chain widens the read
            EventCriteria widened = chain.widen(readForNewestName);

            // then the read transitively fetches events stored under either older name
            assertThat(widened.flatten())
                    .singleElement()
                    .satisfies(criterion -> assertThat(criterion.types())
                            .containsExactlyInAnyOrder(COURSE_REGISTERED.qualifiedName(),
                                                       COURSE_CREATED.qualifiedName(),
                                                       COURSE_OPENED.qualifiedName()));
        }
    }

    private record CoursePayload(String courseId) {

    }
}
