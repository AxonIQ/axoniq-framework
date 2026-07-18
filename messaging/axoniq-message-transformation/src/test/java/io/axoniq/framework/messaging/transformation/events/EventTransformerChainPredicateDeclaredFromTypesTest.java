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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.alwaysEmptyMessageTypeResolver;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.collectMessages;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.eventOf;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedConverter;
import static io.axoniq.framework.messaging.transformation.events.EventStreamTestUtils.neverInvokedMessageTypeResolver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A predicate-{@code from} transformation may declare the {@code from} types it consumes via
 * {@code declaringFromTypes(...)}. Those declared types act as a pre-filter: the predicate is
 * evaluated <em>only</em> against events whose qualified name is one of the declared types,
 * never against any other event. Without declared types the predicate is evaluated against
 * every read event. These tests pin both behaviors plus the builder's validation.
 */
final class EventTransformerChainPredicateDeclaredFromTypesTest {

    private static final QualifiedName COURSE = new QualifiedName("com.example.CourseCreated");
    private static final QualifiedName STUDENT = new QualifiedName("com.example.StudentRegistered");
    private static final MessageType COURSE_V1 = new MessageType(COURSE, "1.0.0");
    private static final MessageType COURSE_V2 = new MessageType(COURSE, "2.0.0");
    private static final MessageType COURSE_V3 = new MessageType(COURSE, "3.0.0");
    private static final MessageType STUDENT_V1 = new MessageType(STUDENT, "1.0.0");
    private static final MessageConverter CONVERTER = neverInvokedConverter();

    private static JsonNode json() {
        return JsonNodeFactory.instance.objectNode();
    }

    @Nested
    final class DeclaredFromTypesPreFilter {

        @Test
        void predicateIsEvaluatedOnlyAgainstEventsOfADeclaredFromType() {
            // given a predicate that records every type it is offered and matches version 1.0.0
            List<MessageType> evaluated = new ArrayList<>();
            Predicate<MessageType> recordingPredicate = type -> {
                evaluated.add(type);
                return "1.0.0".equals(type.version());
            };
            EventTransformation transformation = EventTransformation.from(recordingPredicate)
                                                                    .declaringFromTypes(COURSE)
                                                                    .to(COURSE_V2)
                                                                    .transform(JsonNode.class, (in, ctx) -> in);
            EventTransformerChain chain = EventTransformerChain.builder().register(transformation).build();

            // when a declared-type event and a non-declared-type event are read
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(eventOf(COURSE_V1, json()), eventOf(STUDENT_V1, json()))),
                    null, CONVERTER, alwaysEmptyMessageTypeResolver()));

            // then the predicate only ever saw the declared CourseCreated type, never the student type
            assertThat(evaluated).isNotEmpty()
                                 .extracting(MessageType::qualifiedName)
                                 .containsOnly(COURSE);
            // the course event is lifted to v2; the student event passes through unchanged
            assertThat(outputs).extracting(EventMessage::type).containsExactly(COURSE_V2, STUDENT_V1);
        }

        @Test
        void eventOfADeclaredTypeFailingThePredicatePassesThroughUnchanged() {
            // given a transformation restricted to CourseCreated, matching only version 1.0.0
            EventTransformation transformation = EventTransformation.from(type -> "1.0.0".equals(type.version()))
                                                                    .declaringFromTypes(COURSE)
                                                                    .to(COURSE_V2)
                                                                    .transform(JsonNode.class, (in, ctx) -> in);
            EventTransformerChain chain = EventTransformerChain.builder().register(transformation).build();

            // when a CourseCreated event of a non-matching version is read
            EventMessage courseV3 = eventOf(COURSE_V3, json());
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(courseV3)), null, CONVERTER, alwaysEmptyMessageTypeResolver()));

            // then it passes through untouched: the predicate ran but rejected it
            assertThat(outputs).singleElement().isSameAs(courseV3);
        }

        @Test
        void eventNotOfADeclaredTypeIsNeverTransformedEvenWhenThePredicateWouldMatchEverything() {
            // given a predicate that would match every type, but restricted to CourseCreated
            EventTransformation transformation = EventTransformation.from(type -> true)
                                                                    .declaringFromTypes(COURSE)
                                                                    .to(COURSE_V2)
                                                                    .transform(JsonNode.class, (in, ctx) -> in);
            EventTransformerChain chain = EventTransformerChain.builder().register(transformation).build();

            // when an event of a non-declared type is read; the resolver fails loudly if the
            // transform path is ever entered, proving the event never matched
            EventMessage studentV1 = eventOf(STUDENT_V1, json());
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(studentV1)), null, CONVERTER,
                    neverInvokedMessageTypeResolver()));

            // then the event passes through unchanged: the declared-type pre-filter excluded it
            assertThat(outputs).singleElement().isSameAs(studentV1);
        }
    }

    @Nested
    final class WithoutDeclaredFromTypes {

        @Test
        void predicateIsEvaluatedAgainstEveryReadEvent() {
            // given a predicate that records every type it is offered, with no declared from types
            List<MessageType> evaluated = new ArrayList<>();
            Predicate<MessageType> recordingPredicate = type -> {
                evaluated.add(type);
                return COURSE.equals(type.qualifiedName()) && "1.0.0".equals(type.version());
            };
            EventTransformation transformation = EventTransformation.from(recordingPredicate)
                                                                    .to(COURSE_V2)
                                                                    .transform(JsonNode.class, (in, ctx) -> in);
            EventTransformerChain chain = EventTransformerChain.builder().register(transformation).build();

            // when events of unrelated qualified names are read
            List<EventMessage> outputs = collectMessages(chain.transform(
                    MessageStream.fromIterable(List.of(eventOf(COURSE_V1, json()), eventOf(STUDENT_V1, json()))),
                    null, CONVERTER, alwaysEmptyMessageTypeResolver()));

            // then the predicate is offered the unrelated student event too (no pre-filter)
            assertThat(evaluated).extracting(MessageType::qualifiedName).contains(STUDENT);
            assertThat(outputs).extracting(EventMessage::type).containsExactly(COURSE_V2, STUDENT_V1);
        }
    }

    @Nested
    final class BuilderValidation {

        @Test
        void declaringFromTypesWithNoArgumentsIsRejected() {
            // given / when / then declaring zero from types is a configuration mistake
            assertThatThrownBy(() -> EventTransformation.from(type -> true).declaringFromTypes())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least one");
        }

        @Test
        void declaringFromTypesRejectsNullElements() {
            // given / when / then a null qualified name in the declared set is rejected
            QualifiedName missing = null;
            assertThatThrownBy(() -> EventTransformation.from(type -> true).declaringFromTypes(COURSE, missing))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
