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

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.event.dcb.ConsistencyCondition;
import io.axoniq.axonserver.grpc.event.dcb.Criterion;
import io.axoniq.axonserver.grpc.event.dcb.SnapshottedSourceRequest;
import io.axoniq.axonserver.grpc.event.dcb.SourceEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.TagsAndNamesCriterion;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPositions;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating the {@link ConditionConverter}.
 *
 * @author Steven van Beelen
 */
class ConditionConverterTest {

    private static final int START = 1337;

    @Test
    void convertAppendConditionThrowsNullPointerExceptionForNullAppendCondition() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> ConditionConverter.convertAppendCondition(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void convertAppendConditionConstructsConsistencyConditionAsExpected() {
        // given...
        AppendCondition testCondition = AppendCondition.withCriteria(
                EventCriteria.havingTags(
                                     Tag.of("key1OnCriterion1", "value1OnCriterion1"),
                                     Tag.of("key2OnCriterion1", "value2OnCriterion1")
                             )
                             .andBeingOneOfTypes(new QualifiedName("name1OnCriterion1"))
                             .or()
                             .havingTags(Tag.of("key1OnCriterion2", "value1OnCriterion2"))
                             .andBeingOneOfTypes(
                                     new QualifiedName("name1OnCriterion2"),
                                     new QualifiedName("name2OnCriterion2")
                             )
                             .or()
                             .havingTags(Tag.of("key1OnCriterion3", "value1OnCriterion3"))
        ).withMarker(new GlobalIndexConsistencyMarker(START));
        // when...
        ConsistencyCondition result = ConditionConverter.convertAppendCondition(testCondition);
        // then...
        assertThat(result.getConsistencyMarker()).isEqualTo(START);
        List<Criterion> resultCriterion = result.getCriterionList();
        assertThat(resultCriterion).hasSize(3);
        validateCriterion(resultCriterion.getFirst().getTagsAndNames());
        validateCriterion(resultCriterion.get(1).getTagsAndNames());
        validateCriterion(resultCriterion.getLast().getTagsAndNames());
    }

    @Test
    void convertSourcingConditionThrowsNullPointerExceptionForNullSourcingCondition() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> ConditionConverter.convertSourcingCondition(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void convertSourcingConditionConstructsSourceEventRequestAsExpected() {
        // given...
        SourcingCondition testCondition = SourcingCondition.conditionFor(
                GlobalIndexPositions.of(START),
                EventCriteria.havingTags(
                                     Tag.of("key1OnCriterion1", "value1OnCriterion1"),
                                     Tag.of("key2OnCriterion1", "value2OnCriterion1")
                             )
                             .andBeingOneOfTypes(new QualifiedName("name1OnCriterion1"))
                             .or()
                             .havingTags(Tag.of("key1OnCriterion2", "value1OnCriterion2"))
                             .andBeingOneOfTypes(
                                     new QualifiedName("name1OnCriterion2"),
                                     new QualifiedName("name2OnCriterion2")
                             )
                             .or()
                             .havingTags(Tag.of("key1OnCriterion3", "value1OnCriterion3"))
        );
        // when...
        SourceEventsRequest result = ConditionConverter.convertSourcingCondition(testCondition);
        // then...
        assertThat(result.getFromSequence()).isEqualTo(START);
        List<Criterion> resultCriterion = result.getCriterionList();
        assertThat(resultCriterion).hasSize(3);
        validateCriterion(resultCriterion.getFirst().getTagsAndNames());
        validateCriterion(resultCriterion.get(1).getTagsAndNames());
        validateCriterion(resultCriterion.getLast().getTagsAndNames());
    }

    @Test
    void convertSnapshottedSourcingConditionThrowsNullPointerExceptionForNullSourcingCondition() {
        Converter converter = new JacksonConverter();
        QualifiedName qualifiedName = new QualifiedName("test-entity");
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> ConditionConverter.convertSnapshottedSourcingCondition(
                null, converter, qualifiedName, "entity-id"
        )).isInstanceOf(NullPointerException.class);
    }

    @Test
    void convertSnapshottedSourcingConditionConstructsSnapshottedSourceRequestAsExpected() {
        // given...
        Converter converter = new JacksonConverter();
        QualifiedName qualifiedName = new QualifiedName("test-entity");
        String identifier = "entity-id";
        SourcingCondition testCondition = SourcingCondition.conditionFor(
                EventCriteria.havingTags(
                                     Tag.of("key1OnCriterion1", "value1OnCriterion1"),
                                     Tag.of("key2OnCriterion1", "value2OnCriterion1")
                             )
                             .andBeingOneOfTypes(new QualifiedName("name1OnCriterion1"))
                             .or()
                             .havingTags(Tag.of("key1OnCriterion2", "value1OnCriterion2"))
                             .andBeingOneOfTypes(
                                     new QualifiedName("name1OnCriterion2"),
                                     new QualifiedName("name2OnCriterion2")
                             )
                             .or()
                             .havingTags(Tag.of("key1OnCriterion3", "value1OnCriterion3"))
        );
        // when...
        SnapshottedSourceRequest result = ConditionConverter.convertSnapshottedSourcingCondition(
                testCondition, converter, qualifiedName, identifier
        );
        // then...
        ByteString expectedSnapshotKey =
                ByteString.copyFrom(converter.convert(qualifiedName.name(), byte[].class))
                          .concat(ByteString.copyFrom(new byte[]{0}))
                          .concat(ByteString.copyFrom(converter.convert(identifier, byte[].class)));
        assertThat(result.getSnapshotKey()).isEqualTo(expectedSnapshotKey);
        List<Criterion> resultCriterion = result.getCriterionList();
        assertThat(resultCriterion).hasSize(3);
        validateCriterion(resultCriterion.getFirst().getTagsAndNames());
        validateCriterion(resultCriterion.get(1).getTagsAndNames());
        validateCriterion(resultCriterion.getLast().getTagsAndNames());
    }

    @Test
    void convertStreamingConditionThrowsNullPointerExceptionForNullStreamingCondition() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> ConditionConverter.convertStreamingCondition(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void convertStreamingConditionConstructsSourceEventRequestAsExpected() {
        // given...
        StreamingCondition testCondition = StreamingCondition.conditionFor(
                new GlobalSequenceTrackingToken(START),
                EventCriteria.havingTags(
                                     Tag.of("key1OnCriterion1", "value1OnCriterion1"),
                                     Tag.of("key2OnCriterion1", "value2OnCriterion1")
                             )
                             .andBeingOneOfTypes(new QualifiedName("name1OnCriterion1"))
                             .or()
                             .havingTags(Tag.of("key1OnCriterion2", "value1OnCriterion2"))
                             .andBeingOneOfTypes(
                                     new QualifiedName("name1OnCriterion2"),
                                     new QualifiedName("name2OnCriterion2")
                             )
                             .or()
                             .havingTags(Tag.of("key1OnCriterion3", "value1OnCriterion3"))
        );
        // when...
        StreamEventsRequest result = ConditionConverter.convertStreamingCondition(testCondition);
        // then...
        assertThat(result.getFromSequence()).isEqualTo(START);
        List<Criterion> resultCriterion = result.getCriterionList();
        assertThat(resultCriterion).hasSize(3);
        validateCriterion(resultCriterion.getFirst().getTagsAndNames());
        validateCriterion(resultCriterion.get(1).getTagsAndNames());
        validateCriterion(resultCriterion.getLast().getTagsAndNames());
    }

    private static void validateCriterion(TagsAndNamesCriterion criterion) {
        if (criterion.getNameCount() == 1) {
            String name = criterion.getNameList()
                                   .getFirst();
            assertThat(name).isEqualTo("name1OnCriterion1");
            List<io.axoniq.axonserver.grpc.event.dcb.Tag> tags = criterion.getTagList();
            assertThat(tags).hasSize(2);
            assertThat(tags).contains(
                    io.axoniq.axonserver.grpc.event.dcb.Tag.newBuilder()
                                                           .setKey(ByteString.copyFrom("key1OnCriterion1",
                                                                                       StandardCharsets.UTF_8))
                                                           .setValue(ByteString.copyFrom("value1OnCriterion1",
                                                                                         StandardCharsets.UTF_8))
                                                           .build()
            );
            assertThat(tags).contains(
                    io.axoniq.axonserver.grpc.event.dcb.Tag.newBuilder()
                                                           .setKey(ByteString.copyFrom("key2OnCriterion1",
                                                                                       StandardCharsets.UTF_8))
                                                           .setValue(ByteString.copyFrom("value2OnCriterion1",
                                                                                         StandardCharsets.UTF_8))
                                                           .build()
            );
        } else if (criterion.getNameCount() == 2) {
            List<String> names = criterion.getNameList();
            assertThat(names).contains("name1OnCriterion2");
            assertThat(names).contains("name2OnCriterion2");
            assertThat(criterion.getTagCount()).isEqualTo(1);
            io.axoniq.axonserver.grpc.event.dcb.Tag firstTag = criterion.getTagList().getFirst();
            assertThat(firstTag.getKey().toString(StandardCharsets.UTF_8)).isEqualTo("key1OnCriterion2");
            assertThat(firstTag.getValue().toString(StandardCharsets.UTF_8)).isEqualTo("value1OnCriterion2");
        } else if (criterion.getNameCount() == 0) {
            assertThat(criterion.getNameList()).isEmpty();
            assertThat(criterion.getTagCount()).isEqualTo(1);
            io.axoniq.axonserver.grpc.event.dcb.Tag firstTag = criterion.getTagList().getFirst();
            assertThat(firstTag.getKey().toString(StandardCharsets.UTF_8)).isEqualTo("key1OnCriterion3");
            assertThat(firstTag.getValue().toString(StandardCharsets.UTF_8)).isEqualTo("value1OnCriterion3");
        } else {
            fail("This validation method only expects criterion with 0, 1, or 2 names!");
        }
    }
}