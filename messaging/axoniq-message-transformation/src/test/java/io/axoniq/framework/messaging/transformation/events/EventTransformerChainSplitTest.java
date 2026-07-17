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

import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectEntries;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.eventOf;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.recordingConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavior of the {@code EventTransformation.split(source, inputType)} factory: a stored event matching
 * {@code source} is replaced in the read stream by its declared outputs, each pairing a produced identity with the
 * mapper deriving its payload, emitted in declaration order. Every other event passes through untouched. All
 * replacements share the input event's stream position, and each re-enters the chain, so a later transformation
 * still applies to it.
 */
final class EventTransformerChainSplitTest {

    private static final MessageType COMBINED =
            new MessageType("com.example.StudentEnrolledAndCourseUpdated", "1.0.0");
    private static final MessageType COMBINED_V2 =
            new MessageType("com.example.StudentEnrolledAndCourseUpdated", "2.0.0");
    private static final MessageType STUDENT_ENROLLED = new MessageType("com.example.StudentEnrolled", "1.0.0");
    private static final MessageType STUDENT_ENROLLED_V2 = new MessageType("com.example.StudentEnrolled", "2.0.0");
    private static final MessageType COURSE_CAPACITY_UPDATED =
            new MessageType("com.example.CourseCapacityUpdated", "1.0.0");
    private static final MessageType STUDENT_REGISTERED = new MessageType("com.example.StudentRegistered", "1.0.0");
    private static final MessageType LEGACY_COMBINED = new MessageType("com.example.LegacyEnrollmentBundle", "1.0.0");
    private static final MessageType STUDENT_NAME_RECORDED = new MessageType("com.example.StudentNameRecorded", "1.0.0");
    private static final MessageType STUDENT_CONTACT_ADDED = new MessageType("com.example.StudentContactAdded", "1.0.0");
    private static final MessageType UNRELATED = new MessageType("com.example.SystemHeartbeat", "1.0.0");

    private record Combined(String student, int capacity) {

    }

    private record Enrollment(String student) {

    }

    private record CapacityChange(int capacity) {

    }

    /** A split of the combined event into an enrollment followed by a capacity change, in that order. */
    private static EventTransformation studentEnrolledSplit() {
        return EventTransformation.split(COMBINED, Combined.class)
                                  .producing(STUDENT_ENROLLED, combined -> new Enrollment(combined.student()))
                                  .producing(COURSE_CAPACITY_UPDATED,
                                             combined -> new CapacityChange(combined.capacity()))
                                  .build();
    }

    @Nested
    final class InputValidation {

        @Test
        void splitRejectsNullSource() {
            //noinspection DataFlowIssue
            assertThatThrownBy(() -> EventTransformation.split(null, Combined.class))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("source");
        }

        @Test
        void splitRejectsNullInputType() {
            //noinspection DataFlowIssue
            assertThatThrownBy(() -> EventTransformation.split(COMBINED, (Class<Combined>) null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("inputType");
        }

        @Test
        void splitRejectsNullTypeReferenceInputType() {
            //noinspection DataFlowIssue
            assertThatThrownBy(() -> EventTransformation.split(COMBINED, (TypeReference<Combined>) null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("inputType");
        }

        @Test
        void producingRejectsANullProducedType() {
            EventTransformation.SplitStep<Combined> step = EventTransformation.split(COMBINED, Combined.class);

            //noinspection DataFlowIssue
            assertThatThrownBy(() -> step.producing(null, combined -> new Enrollment(combined.student())))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("producedType");
        }

        @Test
        void producingRejectsANullOutputMapper() {
            EventTransformation.SplitStep<Combined> step = EventTransformation.split(COMBINED, Combined.class);

            //noinspection DataFlowIssue
            assertThatThrownBy(() -> step.producing(STUDENT_ENROLLED, (Function<Combined, ?>) null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("outputMapper");
        }

        @Test
        void producingRejectsANullContextAwareOutputMapper() {
            EventTransformation.SplitStep<Combined> step = EventTransformation.split(COMBINED, Combined.class);

            //noinspection DataFlowIssue
            assertThatThrownBy(() -> step.producing(
                    STUDENT_ENROLLED, (BiFunction<Combined, ProcessingContext, ?>) null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("outputMapper");
        }

        @Test
        void buildRejectsASplitWithNoProducedEvents() {
            // a split with no outputs is a drop, not a split
            EventTransformation.SplitStep<Combined> step = EventTransformation.split(COMBINED, Combined.class);

            assertThatThrownBy(step::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least two");
        }

        @Test
        void buildRejectsASplitWithASingleProducedEvent() {
            // a split with a single output is a plain 1:1 transformation, not a split
            EventTransformation.SplitStep<Combined> step =
                    EventTransformation.split(COMBINED, Combined.class)
                                       .producing(STUDENT_ENROLLED, combined -> new Enrollment(combined.student()));

            assertThatThrownBy(step::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least two");
        }
    }

    @Nested
    final class SplittingEvents {

        @Test
        void oneEventIsReplacedByItsReplacementsInDeclaredOrder() {
            // given a split of the combined event and a single stored combined event, whose type is a distinct
            // instance equal to the registered source, so matching is exercised through MessageType.equals
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .build();
            EventMessage stored = eventOf(new MessageType("com.example.StudentEnrolledAndCourseUpdated", "1.0.0"),
                                          new Combined("alice", 30));

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then two events appear in its place: StudentEnrolled first, then CourseCapacityUpdated
            assertThat(outputs).hasSize(2);
            assertThat(outputs.get(0).type()).isEqualTo(STUDENT_ENROLLED);
            assertThat(outputs.get(0).payload()).isEqualTo(new Enrollment("alice"));
            assertThat(outputs.get(1).type()).isEqualTo(COURSE_CAPACITY_UPDATED);
            assertThat(outputs.get(1).payload()).isEqualTo(new CapacityChange(30));
        }

        @Test
        void theReplacementsCarryTheirOwnDistinctIdentitiesSoPerTypeHandlersSeeOnlyTheirOwn() {
            // given the split and a stored combined event
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .build();
            EventMessage stored = eventOf(COMBINED, new Combined("bob", 12));

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then exactly one output carries StudentEnrolled, and exactly one carries CourseCapacityUpdated: a
            // handler subscribed to only one of them receives only that event and never sees the other
            assertThat(outputs).extracting(EventMessage::type)
                               .containsExactly(STUDENT_ENROLLED, COURSE_CAPACITY_UPDATED);
        }

        @Test
        void anEventOfADifferentTypePassesThroughUnchanged() {
            // given the split and a stream carrying an unrelated event
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .build();
            EventMessage unrelated = eventOf(UNRELATED, "heartbeat");

            // when the chain reads the stream with stand-ins that fail if conversion or resolution is attempted
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(unrelated)),
                    null, neverInvokedConverter(), neverInvokedMessageTypeResolver()));

            // then it passes through the splitting transformation unchanged
            assertThat(outputs).singleElement().satisfies(output -> {
                assertThat(output).isSameAs(unrelated);
                assertThat(output.type()).isEqualTo(UNRELATED);
            });
        }

        @Test
        void everyProducedEventCarriesTheInputsMetadata() {
            // given the split and a stored combined event carrying metadata
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .build();
            EventMessage stored = new GenericEventMessage(COMBINED, new Combined("grace", 15))
                    .andMetadata(Map.of("correlationId", "abc-123"));

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then both produced events carry the input's metadata forward
            assertThat(outputs)
                    .hasSize(2)
                    .allSatisfy(output -> assertThat(output.metadata()).containsEntry("correlationId", "abc-123"));
        }

        @Test
        void producedEventsKeepStreamOrderRelativeToSurroundingEvents() {
            // given the split and a stream with an unrelated event before and after the combined event
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .build();
            EventMessage before = eventOf(UNRELATED, "before");
            EventMessage combined = eventOf(COMBINED, new Combined("heidi", 8));
            EventMessage after = eventOf(UNRELATED, "after");

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(before, combined, after)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then the split's outputs appear in place, between the surrounding events, in order
            assertThat(outputs).extracting(EventMessage::type)
                               .containsExactly(UNRELATED, STUDENT_ENROLLED, COURSE_CAPACITY_UPDATED, UNRELATED);
        }

        @Test
        void splitEmitsItsReplacementsWhenInvokedDirectly() {
            // SPI contract: a direct transform(...) call yields the ordered replacement events
            EventTransformation split = studentEnrolledSplit();
            TransformationContext context = new TransformationContext(
                    Context.empty(), null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver());

            MessageStream<? extends EventMessage> result =
                    split.transform(eventOf(COMBINED, new Combined("dave", 7)), context);

            List<EventMessage> replacements = collectMessages(result);
            assertThat(replacements).extracting(EventMessage::type)
                                    .containsExactly(STUDENT_ENROLLED, COURSE_CAPACITY_UPDATED);
        }
    }

    @Nested
    final class ChainingAfterASplit {

        @Test
        void bothReplacementsFlowThroughTheRemainderOfTheChain() {
            // given a split of the combined event, then a later version bump of StudentEnrolled 1.0.0 -> 2.0.0
            EventTransformation bumpEnrolled = EventTransformation.from(STUDENT_ENROLLED)
                                                                  .to(STUDENT_ENROLLED_V2)
                                                                  .transform(Enrollment.class, enrollment -> enrollment);
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .register(bumpEnrolled)
                                                               .build();
            EventMessage stored = eventOf(COMBINED, new Combined("erin", 25));

            // when the chain reads the stored combined event
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then the split fires first, and the StudentEnrolled replacement is then bumped to 2.0.0, while the
            // CourseCapacityUpdated replacement passes through the remainder of the chain unchanged
            assertThat(outputs).hasSize(2);
            assertThat(outputs.get(0).type()).isEqualTo(STUDENT_ENROLLED_V2);
            assertThat(outputs.get(0).payload()).isEqualTo(new Enrollment("erin"));
            assertThat(outputs.get(1).type()).isEqualTo(COURSE_CAPACITY_UPDATED);
            assertThat(outputs.get(1).payload()).isEqualTo(new CapacityChange(25));
        }

        @Test
        void aSplitAppliesAfterAPrecedingVersionBump() {
            // given a version bump of the combined event 1.0.0 -> 2.0.0, then a split registered on 2.0.0
            EventTransformation bumpCombined = EventTransformation.from(COMBINED)
                                                                  .to(COMBINED_V2)
                                                                  .transform(Combined.class, combined -> combined);
            EventTransformation splitV2 = EventTransformation.split(COMBINED_V2, Combined.class)
                                                             .producing(STUDENT_ENROLLED,
                                                                        combined -> new Enrollment(combined.student()))
                                                             .producing(COURSE_CAPACITY_UPDATED,
                                                                        combined -> new CapacityChange(combined.capacity()))
                                                             .build();
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(bumpCombined)
                                                               .register(splitV2)
                                                               .build();
            EventMessage stored = eventOf(COMBINED, new Combined("ivan", 3));

            // when the chain reads an event stored under the old version
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then it bumps to 2.0.0, then the split fires on the bumped event
            assertThat(outputs).extracting(EventMessage::type)
                               .containsExactly(STUDENT_ENROLLED, COURSE_CAPACITY_UPDATED);
        }

        @Test
        void aProducedEventCanBeDroppedByALaterTransformation() {
            // given the split, then a drop of one of its produced types
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .register(EventTransformation.drop(STUDENT_ENROLLED))
                                                               .build();
            EventMessage stored = eventOf(COMBINED, new Combined("judy", 9));

            // when the chain reads the combined event
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then the split produces both events, and the StudentEnrolled one is then dropped
            assertThat(outputs).singleElement().satisfies(output ->
                    assertThat(output.type()).isEqualTo(COURSE_CAPACITY_UPDATED));
        }

        @Test
        void aProducedEventCanBeRenamedByALaterTransformation() {
            // given the split, then a rename of one of its produced types
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .register(EventTransformation.rename(STUDENT_ENROLLED,
                                                                                                    STUDENT_REGISTERED))
                                                               .build();
            EventMessage stored = eventOf(COMBINED, new Combined("kim", 5));

            // when the chain reads the combined event
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then the StudentEnrolled output is renamed to StudentRegistered with its payload unchanged, while the
            // CourseCapacityUpdated output passes through
            assertThat(outputs).hasSize(2);
            assertThat(outputs.get(0).type()).isEqualTo(STUDENT_REGISTERED);
            assertThat(outputs.get(0).payload()).isEqualTo(new Enrollment("kim"));
            assertThat(outputs.get(1).type()).isEqualTo(COURSE_CAPACITY_UPDATED);
        }

        @Test
        void aSplitAppliesAfterAPrecedingRename() {
            // given a rename of a legacy bundle type to the combined type, then a split registered on the combined type
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(EventTransformation.rename(LEGACY_COMBINED,
                                                                                                    COMBINED))
                                                               .register(studentEnrolledSplit())
                                                               .build();
            EventMessage stored = eventOf(LEGACY_COMBINED, new Combined("lee", 11));

            // when the chain reads an event stored under the legacy name
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then it is renamed to the combined type, then the split fires on the renamed event
            assertThat(outputs).extracting(EventMessage::type)
                               .containsExactly(STUDENT_ENROLLED, COURSE_CAPACITY_UPDATED);
        }

        @Test
        void aProducedEventCanItselfBeSplit() {
            // given the split, then a second split registered on one of its produced types
            EventTransformation splitEnrolled = EventTransformation.split(STUDENT_ENROLLED, Enrollment.class)
                                                                   .producing(STUDENT_NAME_RECORDED,
                                                                              enrollment -> enrollment)
                                                                   .producing(STUDENT_CONTACT_ADDED,
                                                                              enrollment -> "contact-" + enrollment.student())
                                                                   .build();
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .register(splitEnrolled)
                                                               .build();
            EventMessage stored = eventOf(COMBINED, new Combined("max", 4));

            // when the chain reads the combined event
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)),
                    null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then the first split's StudentEnrolled output is itself split, depth-first: its two events appear before
            // the first split's CourseCapacityUpdated output, which is emitted after
            assertThat(outputs).extracting(EventMessage::type)
                               .containsExactly(STUDENT_NAME_RECORDED, STUDENT_CONTACT_ADDED, COURSE_CAPACITY_UPDATED);
        }
    }

    @Nested
    final class StreamPosition {

        @Test
        void everyReplacementSharesTheInputsTrackingTokenAndSequenceNumber() {
            // given the split and a stored combined event carrying a tracking token at position 5
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .build();
            EventMessage stored = eventOf(COMBINED, new Combined("frank", 40));
            MessageStream<EventMessage> stream = MessageStream.fromIterable(
                    List.of(stored),
                    event -> TrackingToken.addToContext(Context.empty(), new GlobalSequenceTrackingToken(5)));

            // when the chain reads the stream
            List<MessageStream.Entry<EventMessage>> entries = collectEntries(chain.transform(
                    stream, null, neverInvokedConverter(), alwaysEmptyMessageTypeResolver()));

            // then both replacements surface at the input's position: the framework does not renumber
            assertThat(entries).hasSize(2);
            assertThat(tokenOf(entries.get(0))).isEqualTo(new GlobalSequenceTrackingToken(5));
            assertThat(tokenOf(entries.get(1))).isEqualTo(new GlobalSequenceTrackingToken(5));
            assertThat(entries.get(0).message().type()).isEqualTo(STUDENT_ENROLLED);
            assertThat(entries.get(1).message().type()).isEqualTo(COURSE_CAPACITY_UPDATED);
        }
    }

    @Nested
    final class SameSourceConflict {

        @Test
        void aSplitAndAMappingOnTheSameSourceAreRejected() {
            // given a split and a 1:1 mapping both claiming the combined event's identity exactly
            EventTransformation mapping = EventTransformation.from(COMBINED)
                                                             .to(new MessageType(COMBINED.name(), "2.0.0"))
                                                             .transform(Combined.class, combined -> combined);
            EventTransformerChain.Builder builder = EventTransformerChain.builder()
                                                                         .register(studentEnrolledSplit())
                                                                         .register(mapping);

            // when the chain is built / then the ambiguous overlap is rejected, naming the source
            assertThatThrownBy(builder::build)
                    .isInstanceOf(ChainConfigurationException.class)
                    .hasMessageContaining(COMBINED.toString());
        }
    }

    @Nested
    final class WideningTypeFilteredReads {

        @Test
        void aReadFilteredToAReplacementTypeIsWidenedToTheSource() {
            // given a split declaring its replacement types
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .build();
            // and a read filtered to one of the replacement types
            EventCriteria readForEnrolled =
                    EventCriteria.havingAnyTag().andBeingOneOfTypes(STUDENT_ENROLLED.qualifiedName());

            // when the chain widens the read
            EventCriteria widened = chain.widen(readForEnrolled);

            // then the read also fetches the stored source type, so the split can fire during sourcing
            assertThat(widened.flatten())
                    .flatExtracting(criterion -> criterion.types().stream().map(QualifiedName::name).toList())
                    .contains(STUDENT_ENROLLED.qualifiedName().name(), COMBINED.qualifiedName().name());
        }
    }

    @Nested
    final class PayloadConversion {

        @Test
        void storedPayloadIsConvertedToTheDeclaredInputTypeBeforeSplitting() {
            // given a split declaring Combined.class as input, and a stored event whose payload is a raw String
            EventTransformerChain chain = EventTransformerChain.builder()
                                                               .register(studentEnrolledSplit())
                                                               .build();
            EventMessage stored = eventOf(COMBINED, "raw-string-payload");
            EventStreamTestUtils.RecordingMessageConverter<Combined> converter =
                    recordingConverter(message -> new Combined("kate", 20));

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)), null, converter, alwaysEmptyMessageTypeResolver()));

            // then the converter is invoked once with the declared input type, and the split runs on the result
            assertThat(converter.invocationCount()).isEqualTo(1);
            assertThat(converter.lastRequestedType()).isEqualTo(Combined.class);
            assertThat(outputs).hasSize(2);
            assertThat(outputs.get(0).payload()).isEqualTo(new Enrollment("kate"));
            assertThat(outputs.get(1).payload()).isEqualTo(new CapacityChange(20));
        }

        @Test
        void genericInputTypeIsSupportedViaTypeReference() {
            // given a split declaring a parameterized input type via TypeReference, and a stored raw String payload
            TypeReference<Map<String, Object>> mapType = new TypeReference<>() {
            };
            EventTransformation split = EventTransformation.split(COMBINED, mapType)
                                                           .producing(STUDENT_ENROLLED,
                                                                      map -> new Enrollment((String) map.get("student")))
                                                           .producing(COURSE_CAPACITY_UPDATED,
                                                                      map -> new CapacityChange(((Number) map.get("capacity")).intValue()))
                                                           .build();
            EventTransformerChain chain = EventTransformerChain.builder().register(split).build();
            EventMessage stored = eventOf(COMBINED, "raw-string-payload");
            EventStreamTestUtils.RecordingMessageConverter<Map<String, Object>> converter =
                    recordingConverter(message -> Map.of("student", "liam", "capacity", 7));

            // when the chain reads the stream
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(stored)), null, converter, alwaysEmptyMessageTypeResolver()));

            // then the converter is invoked with the parameterized type, preserving its generic parameters
            assertThat(converter.invocationCount()).isEqualTo(1);
            assertThat(converter.lastRequestedType()).isEqualTo(mapType.getType());
            assertThat(outputs).extracting(EventMessage::payload)
                               .containsExactly(new Enrollment("liam"), new CapacityChange(7));
        }

        @Test
        void aNullConvertedPayloadIsRejected() {
            // given a split whose declared input type does not match the stored payload, and a converter yielding null
            EventTransformation split = studentEnrolledSplit();
            TransformationContext context = new TransformationContext(
                    Context.empty(), null, recordingConverter(message -> (Combined) null),
                    alwaysEmptyMessageTypeResolver());
            EventMessage stored = eventOf(COMBINED, "raw-string-payload");

            // when the split transforms an event whose payload the converter resolves to null,
            // then it fails fast, naming the input type
            assertThatThrownBy(() -> split.transform(stored, context))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("null");
        }
    }

    @Nested
    final class OutputMapperValidation {

        @Test
        void aNullOutputPayloadIsRejected() {
            // given a split whose second output mapper returns null: a split emits every declared output, so a
            // missing payload is a configuration error rather than a conditional omission
            EventTransformation split = EventTransformation.split(COMBINED, Combined.class)
                                                           .producing(STUDENT_ENROLLED,
                                                                      combined -> new Enrollment(combined.student()))
                                                           .producing(COURSE_CAPACITY_UPDATED, combined -> null)
                                                           .build();
            TransformationContext context = new TransformationContext(
                    Context.empty(), null, neverInvokedConverter(), neverInvokedMessageTypeResolver());
            EventMessage stored = eventOf(COMBINED, new Combined("mia", 1));

            // when the split transforms a matched event / then the null output payload is rejected
            assertThatThrownBy(() -> split.transform(stored, context))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("null");
        }

        @Test
        void theSameProducedTypeMayBeDeclaredTwice() {
            // given a split declaring the enrolled type twice with distinct mappers
            EventTransformation split = EventTransformation.split(COMBINED, Combined.class)
                                                           .producing(STUDENT_ENROLLED,
                                                                      combined -> new Enrollment(combined.student()))
                                                           .producing(STUDENT_ENROLLED,
                                                                      combined -> new Enrollment(combined.student() + "-copy"))
                                                           .build();
            TransformationContext context = new TransformationContext(
                    Context.empty(), null, neverInvokedConverter(), neverInvokedMessageTypeResolver());

            // when the split transforms a matched event
            List<EventMessage> replacements = collectMessages(
                    split.transform(eventOf(COMBINED, new Combined("noah", 2)), context));

            // then one event per declaration is emitted, in declaration order
            assertThat(replacements).extracting(EventMessage::payload)
                                    .containsExactly(new Enrollment("noah"), new Enrollment("noah-copy"));
        }
    }

    @Nested
    final class ContextAwareOutputs {

        @Test
        void aContextAwareOutputMapperReceivesTheProcessingContext() {
            // given a split whose first output mapper branches on whether a ProcessingContext was supplied
            EventTransformation split =
                    EventTransformation.split(COMBINED, Combined.class)
                                       .producing(STUDENT_ENROLLED, (combined, context) ->
                                               new Enrollment(context == null ? "no-context" : combined.student()))
                                       .producing(COURSE_CAPACITY_UPDATED,
                                                  combined -> new CapacityChange(combined.capacity()))
                                       .build();
            EventMessage stored = eventOf(COMBINED, new Combined("olivia", 3));

            // when the split transforms a matched event on the entity-load path, which supplies a context
            TransformationContext withContext = new TransformationContext(
                    Context.empty(), new StubProcessingContext(),
                    neverInvokedConverter(), neverInvokedMessageTypeResolver());
            List<EventMessage> withContextOutputs = collectMessages(split.transform(stored, withContext));

            // then the mapper observed the non-null context and used the payload
            assertThat(withContextOutputs.getFirst().payload()).isEqualTo(new Enrollment("olivia"));

            // when the split transforms on a read path that supplies no context
            TransformationContext withoutContext = new TransformationContext(
                    Context.empty(), null, neverInvokedConverter(), neverInvokedMessageTypeResolver());
            List<EventMessage> withoutContextOutputs = collectMessages(split.transform(stored, withoutContext));

            // then the mapper observed the null context
            assertThat(withoutContextOutputs.getFirst().payload()).isEqualTo(new Enrollment("no-context"));
        }
    }

    private static TrackingToken tokenOf(MessageStream.Entry<EventMessage> entry) {
        return TrackingToken.fromContext(entry).orElseThrow();
    }
}
