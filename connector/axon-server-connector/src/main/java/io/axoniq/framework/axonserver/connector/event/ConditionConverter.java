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
import io.axoniq.axonserver.grpc.event.dcb.Tag;
import io.axoniq.axonserver.grpc.event.dcb.TagsAndNamesCriterion;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.EventCriterion;
import org.axonframework.messaging.eventstreaming.StreamingCondition;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Utility class containing operations to convert Axon Framework's {@link SourcingCondition} and
 * {@link StreamingCondition} into an Axon Server {@link SourceEventsRequest} and {@link StreamEventsRequest}
 * respectively.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public final class ConditionConverter {

    private static final ByteString SNAPSHOT_KEY_SEPARATOR = ByteString.copyFrom(new byte[]{0});

    /**
     * Converts the given {@code condition} into a {@link ConsistencyCondition}.
     * <p>
     * The {@link AppendCondition#consistencyMarker()}} translates to the
     * {@link ConsistencyCondition#getConsistencyMarker() consistency marker value}. The
     * {@link AppendCondition#criteria()} are {@link EventCriteria#flatten() flattened} before being mapped to
     * {@link Criterion}.
     *
     * @param condition The {@code AppendCondition} to base the {@link ConsistencyCondition} on.
     * @return A {@code ConsistencyCondition} based on the given {@code condition}.
     */
    public static ConsistencyCondition convertAppendCondition(AppendCondition condition) {
        return ConsistencyCondition.newBuilder()
                                   .setConsistencyMarker(GlobalIndexConsistencyMarker.position(
                                           condition.consistencyMarker()
                                   ))
                                   .addAllCriterion(convertEventCriterion(condition.criteria().flatten()))
                                   .build();
    }

    /**
     * Converts the given {@code condition} into a {@link SourceEventsRequest}.
     * <p>
     * The {@link SourcingCondition#start()} translates to the
     * {@link SourceEventsRequest#getFromSequence() from sequence value}. The {@link SourcingCondition#criteria()} are
     * {@link EventCriteria#flatten() flattened} before being mapped to {@link Criterion}.
     *
     * @param condition The {@code SourcingCondition} to base the {@link SourceEventsRequest} on.
     * @return A {@code SourceEventsRequest} based on the given {@code condition}.
     */
    public static SourceEventsRequest convertSourcingCondition(SourcingCondition condition) {
        return SourceEventsRequest.newBuilder()
                                  .setFromSequence(GlobalIndexPosition.toIndex(condition.start()))
                                  .addAllCriterion(convertEventCriterion(condition.criteria().flatten()))
                                  .build();
    }

    /**
     * Converts the given {@code condition} into a {@link SnapshottedSourceRequest}.
     * <p>
     * The {@code name} and {@code identifier} (converted to bytes using the given {@code converter}) identify the
     * snapshot Axon Server should prefix the resulting stream with, if one is present, translating to the
     * {@link SnapshottedSourceRequest#getSnapshotKey() snapshot key value}. The {@link SourcingCondition#criteria()}
     * are {@link EventCriteria#flatten() flattened} before being mapped to {@link Criterion}.
     *
     * @param condition  the {@code SourcingCondition} to base the {@link SnapshottedSourceRequest} on
     * @param converter  the {@code Converter} used to convert the {@code name} and {@code identifier} into the snapshot
     *                   key bytes
     * @param name       the {@link QualifiedName} defining the snapshot type to prefix the resulting stream with, if
     *                   present
     * @param identifier the identifier of the snapshotted entity to prefix the resulting stream with, if present
     * @return a {@code SnapshottedSourceRequest} based on the given {@code condition}, {@code name}, and
     * {@code identifier}
     */
    public static SnapshottedSourceRequest convertSnapshottedSourcingCondition(SourcingCondition condition,
                                                                               Converter converter,
                                                                               QualifiedName name,
                                                                               Object identifier) {
        return SnapshottedSourceRequest.newBuilder()
                                       .setSnapshotKey(convertSnapshotKey(converter, name, identifier))
                                       .addAllCriterion(convertEventCriterion(condition.criteria().flatten()))
                                       .build();
    }

    private static ByteString convertSnapshotKey(Converter converter,
                                                 QualifiedName name,
                                                 Object identifier) {
        byte[] nameAsBytes = requireNonNull(
                converter.convert(name.name(), byte[].class),
                "Converted name must not be null."
        );
        byte[] idAsBytes = requireNonNull(
                converter.convert(identifier, byte[].class),
                "Converted identifier must not be null."
        );
        return ByteString.copyFrom(nameAsBytes)
                         .concat(SNAPSHOT_KEY_SEPARATOR)
                         .concat(ByteString.copyFrom(idAsBytes));
    }

    /**
     * Converts the given {@code condition} into a {@link StreamEventsRequest}.
     * <p>
     * The {@link StreamingCondition#position()} translates to the
     * {@link StreamEventsRequest#getFromSequence() from sequence value}. The {@link StreamingCondition#criteria()} are
     * {@link EventCriteria#flatten() flattened} before being mapped to {@link Criterion}.
     *
     * @param condition The {@code StreamingCondition} to base the {@link StreamEventsRequest} on.
     * @return A {@code StreamEventsRequest} based on the given {@code condition}.
     */
    public static StreamEventsRequest convertStreamingCondition(StreamingCondition condition) {
        return StreamEventsRequest.newBuilder()
                                  .setFromSequence(condition.position().position().orElse(-1))
                                  .addAllCriterion(convertEventCriterion(condition.criteria().flatten()))
                                  .build();
    }

    private static List<Criterion> convertEventCriterion(Set<EventCriterion> eventCriterion) {
        return eventCriterion.stream()
                             .map(ConditionConverter::convertEventCriterion)
                             .toList();
    }

    private static Criterion convertEventCriterion(EventCriterion eventCriterion) {
        return Criterion.newBuilder()
                        .setTagsAndNames(TagsAndNamesCriterion.newBuilder()
                                                              .addAllTag(convertTags(eventCriterion.tags()))
                                                              .addAllName(convertTypes(eventCriterion.types()))
                                                              .build())
                        .build();
    }

    private static List<Tag> convertTags(Set<org.axonframework.messaging.eventstreaming.Tag> tags) {
        return tags.stream()
                   .map(ConditionConverter::convertTag)
                   .toList();
    }

    private static Tag convertTag(org.axonframework.messaging.eventstreaming.Tag tag) {
        return Tag.newBuilder()
                  .setKey(ByteString.copyFrom(tag.key(), StandardCharsets.UTF_8))
                  .setValue(ByteString.copyFrom(tag.value(), StandardCharsets.UTF_8))
                  .build();
    }

    private static List<String> convertTypes(Set<QualifiedName> types) {
        return types.stream().map(QualifiedName::name).toList();
    }

    private ConditionConverter() {
        // Utility class
    }
}
