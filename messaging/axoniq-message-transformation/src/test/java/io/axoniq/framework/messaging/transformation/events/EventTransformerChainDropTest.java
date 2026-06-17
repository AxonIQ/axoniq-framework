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

import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectEntries;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.eventOf;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedMessageTypeResolver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavior of the {@code EventTransformation.drop(source)} factory: a stored event
 * matching the {@code source} is removed from the read stream so no handler receives it, while
 * every other event passes through untouched. A drop converts no payload and declares no
 * {@code to} identity, so neither the {@link MessageConverter} nor the {@code MessageTypeResolver}
 * is consulted for the dropped event; the stand-ins fail loudly if a pure-drop test invokes them.
 * A dropped event never reaches a handler; surviving events keep their stream position, so once a
 * tracking processor reads an event after a drop, the dropped position is covered and not revisited.
 */
final class EventTransformerChainDropTest {

    private static final MessageType HEARTBEAT_V1 = new MessageType("com.example.SystemHeartbeat", "1.0.0");
    private static final MessageType HEARTBEAT_V2 = new MessageType("com.example.SystemHeartbeat", "2.0.0");
    private static final MessageType COURSE_OPENED = new MessageType("com.example.CourseOpened", "1.0.0");
    private static final MessageType COURSE_CREATED = new MessageType("com.example.CourseCreated", "1.0.0");

    @Nested
    final class InputValidation {

        @Test
        void dropRejectsNullSource() {
            // null is passed on purpose to assert the source is rejected
            //noinspection DataFlowIssue
            assertThatThrownBy(() -> EventTransformation.drop(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("source");
        }
    }

    @Nested
    final class DroppingEvents {

        @Test
        void droppedEventIsRemovedFromTheStream() {
            // given a drop of SystemHeartbeat and a single stored SystemHeartbeat event
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.drop(HEARTBEAT_V1))
                                                               .build();
            EventMessage storedHeartbeat = eventOf(HEARTBEAT_V1, "heartbeat-payload");

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedHeartbeat)),
                    null, neverInvokedConverter(), neverInvokedMessageTypeResolver()));

            // then no event surfaces in its place, so no handler receives it
            assertThat(outputs).isEmpty();
        }

        @Test
        void onlyMatchingEventsAreDroppedOthersPassThroughUnchanged() {
            // given a drop of SystemHeartbeat and a stream mixing heartbeats with a CourseCreated
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.drop(HEARTBEAT_V1))
                                                               .build();
            EventMessage firstHeartbeat = eventOf(HEARTBEAT_V1, "hb-1");
            EventMessage courseCreated = eventOf(COURSE_CREATED, "course-payload");
            EventMessage secondHeartbeat = eventOf(HEARTBEAT_V1, "hb-2");

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(firstHeartbeat, courseCreated, secondHeartbeat)),
                    null, neverInvokedConverter(), neverInvokedMessageTypeResolver()));

            // then every heartbeat is suppressed and only the CourseCreated reaches handlers, untouched
            assertThat(outputs).singleElement().satisfies(output -> {
                assertThat(output).isSameAs(courseCreated);
                assertThat(output.type()).isEqualTo(COURSE_CREATED);
            });
        }

        @Test
        void dropTakesEffectAfterAVersionBumpHop() {
            // given a chain that first bumps SystemHeartbeat 1.0.0 to 2.0.0, then drops 2.0.0
            EventTransformation bump = EventTransformation.from(HEARTBEAT_V1)
                                                          .to(HEARTBEAT_V2)
                                                          .transform(String.class, payload -> payload);
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(bump)
                                                               .register(EventTransformation.drop(HEARTBEAT_V2))
                                                               .build();
            EventMessage storedHeartbeat = eventOf(HEARTBEAT_V1, "heartbeat-payload");

            // when the chain reads an event stored under the old version
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedHeartbeat)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then it hops to 2.0.0 and is then dropped, so nothing reaches handlers
            assertThat(outputs).isEmpty();
        }

        @Test
        void dropTakesEffectAfterARename() {
            // given a rename of CourseOpened to CourseCreated, then a drop of CourseCreated
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.rename(COURSE_OPENED,
                                                                                                    COURSE_CREATED))
                                                               .register(EventTransformation.drop(COURSE_CREATED))
                                                               .build();
            EventMessage storedCourseOpened = eventOf(COURSE_OPENED, "course-payload");

            // when the chain reads an event stored under the old name
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedCourseOpened)),
                    null, neverInvokedConverter(), neverInvokedMessageTypeResolver()));

            // then it is renamed to CourseCreated and then dropped, so nothing reaches handlers
            assertThat(outputs).isEmpty();
        }

        @Test
        void noPayloadConversionOrIdentityResolutionHappensForDroppedEvents() {
            // given a drop of SystemHeartbeat whose stored payload is an arbitrary type
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.drop(HEARTBEAT_V1))
                                                               .build();
            EventMessage storedHeartbeat = eventOf(HEARTBEAT_V1, new Object());

            // when the chain reads the stream with stand-ins that fail if conversion or resolution is attempted
            // then the drop completes without converting the payload or resolving its identity
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedHeartbeat)),
                    null, neverInvokedConverter(), neverInvokedMessageTypeResolver()));

            assertThat(outputs).isEmpty();
        }

        @Test
        void dropTransformationEmitsAnEmptyStreamWhenInvokedDirectly() {
            // SPI contract: the chain short-circuits a drop, but a direct transform(...) call must
            // still yield no output element. The stand-ins fail loudly if a drop converts or resolves.
            EventTransformation drop = EventTransformation.drop(HEARTBEAT_V1);
            TransformationContext context = new TransformationContext(
                    Context.empty(), null, neverInvokedConverter(), neverInvokedMessageTypeResolver());

            MessageStream<? extends EventMessage> result =
                    drop.transform(eventOf(HEARTBEAT_V1, "heartbeat-payload"), context);

            assertThat(collectMessages(result)).isEmpty();
        }
    }

    @Nested
    final class LastMatchWins {

        @Test
        void aLaterDropOverridesAnEarlierTransformOnTheSameSource() {
            // given a 1:1 transform of SystemHeartbeat registered before a drop of the same source
            EventTransformation bump = EventTransformation.from(HEARTBEAT_V1)
                                                          .to(HEARTBEAT_V2)
                                                          .transform(String.class, payload -> payload);
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(bump)
                                                               .register(EventTransformation.drop(HEARTBEAT_V1))
                                                               .build();
            EventMessage storedHeartbeat = eventOf(HEARTBEAT_V1, "heartbeat-payload");

            // when the chain reads the event
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedHeartbeat)),
                    null, neverInvokedConverter(), neverInvokedMessageTypeResolver()));

            // then the later-registered drop wins and the event is suppressed
            assertThat(outputs).isEmpty();
        }

        @Test
        void anEarlierDropIsOverriddenByALaterTransformOnTheSameSource() {
            // given a drop of SystemHeartbeat registered before a 1:1 transform of the same source
            EventTransformation bump = EventTransformation.from(HEARTBEAT_V1)
                                                          .to(HEARTBEAT_V2)
                                                          .transform(String.class, payload -> payload);
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.drop(HEARTBEAT_V1))
                                                               .register(bump)
                                                               .build();
            EventMessage storedHeartbeat = eventOf(HEARTBEAT_V1, "heartbeat-payload");

            // when the chain reads the event
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(storedHeartbeat)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then the later-registered transform wins, so the event surfaces under the new version
            assertThat(outputs).singleElement().satisfies(output ->
                    assertThat(output.type()).isEqualTo(HEARTBEAT_V2));
        }
    }

    @Nested
    final class PositionProgress {

        @Test
        void survivingEventsKeepTheirStreamPositionAfterADrop() {
            // given a drop of SystemHeartbeat and a stream where each entry carries a tracking token,
            // with a heartbeat sitting between two CourseCreated events
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.drop(HEARTBEAT_V1))
                                                               .build();
            EventMessage first = eventOf(COURSE_CREATED, "course-1");
            EventMessage heartbeat = eventOf(HEARTBEAT_V1, "heartbeat");
            EventMessage last = eventOf(COURSE_CREATED, "course-2");
            MessageStream<EventMessage> stream = MessageStream.fromIterable(
                    List.of(first, heartbeat, last), EventTransformerChainDropTest::tokenForPayload);

            // when the chain reads the stream
            List<MessageStream.Entry<EventMessage>> entries = collectEntries(chain.transform(
                    stream, null, neverInvokedConverter(), neverInvokedMessageTypeResolver()));

            // then the dropped heartbeat is gone and the survivors still carry their original positions
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).message()).isSameAs(first);
            assertThat(tokenOf(entries.get(0))).isEqualTo(new GlobalSequenceTrackingToken(1));
            assertThat(entries.get(1).message()).isSameAs(last);
            assertThat(tokenOf(entries.get(1))).isEqualTo(new GlobalSequenceTrackingToken(3));
            // and the survivor's token (3) covers the dropped position (2): once a processor reads a
            // survivor after the drop, the dropped position is covered, so the heartbeat is not revisited
            assertThat(tokenOf(entries.get(1)).covers(new GlobalSequenceTrackingToken(2))).isTrue();
        }

        @Test
        void aTrailingDropEmitsNoEntrySoTheTokenStaysAtTheLastSurvivor() {
            // given a drop of SystemHeartbeat and a stream that ends with a dropped heartbeat (no survivor after it)
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.drop(HEARTBEAT_V1))
                                                               .build();
            EventMessage survivor = eventOf(COURSE_CREATED, "course-1");
            EventMessage trailingHeartbeat = eventOf(HEARTBEAT_V1, "heartbeat");
            MessageStream<EventMessage> stream = MessageStream.fromIterable(
                    List.of(survivor, trailingHeartbeat), EventTransformerChainDropTest::tokenForPayload);

            // when the chain reads the stream
            List<MessageStream.Entry<EventMessage>> entries = collectEntries(chain.transform(
                    stream, null, neverInvokedConverter(), neverInvokedMessageTypeResolver()));

            // then the trailing drop emits no entry: only the survivor surfaces, so the highest emitted token is
            // the survivor's (position 1). The dropped position (2) is not advanced past by any emitted entry, so a
            // processor resuming from the survivor re-reads then re-drops the trailing heartbeat until a real event
            // arrives. No event is lost or double-handled; a trailing drop is simply re-read-until-survivor.
            assertThat(entries).singleElement().satisfies(entry -> {
                assertThat(entry.message()).isSameAs(survivor);
                assertThat(tokenOf(entry)).isEqualTo(new GlobalSequenceTrackingToken(1));
            });
        }
    }

    @Nested
    final class WideningTypeFilteredReads {

        @Test
        void aDropDoesNotWidenReadCriteria() {
            // given a chain holding only a drop of SystemHeartbeat
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.drop(HEARTBEAT_V1))
                                                               .build();
            // and a read filtered to the dropped type
            EventCriteria readForHeartbeat =
                    EventCriteria.havingAnyTag().andBeingOneOfTypes(HEARTBEAT_V1.qualifiedName());

            // when the chain widens the read
            EventCriteria widened = chain.widen(readForHeartbeat);

            // then nothing is broadened: a drop declares no `to`, so it adds no widening edge
            assertThat(widened).isSameAs(readForHeartbeat);
        }
    }

    private static Context tokenForPayload(EventMessage event) {
        // CourseCreated payloads "course-1"/"course-2" sit at positions 1 and 3; the heartbeat at 2.
        long position = switch (String.valueOf(event.payload())) {
            case "course-1" -> 1L;
            case "heartbeat" -> 2L;
            default -> 3L;
        };
        return TrackingToken.addToContext(Context.empty(), new GlobalSequenceTrackingToken(position));
    }

    private static TrackingToken tokenOf(MessageStream.Entry<EventMessage> entry) {
        return TrackingToken.fromContext(entry).orElseThrow();
    }
}
