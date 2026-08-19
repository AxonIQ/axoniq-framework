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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The single home of segment routing and ownership for workflow instances: a segment owns the
 * workflow instances whose {@code workflowId.hashCode()} it matches: instance partitioning.
 * <p>
 * Two sides apply this rule. Event sequencing applies it as a {@link SequencingPolicy}:
 * {@link #sequenceIdentifierFor(EventMessage, ProcessingContext)} routes an event to the segment that owns the
 * affected instance. The engine applies it when deciding whether to handle, wake or start an instance
 * ({@link #shouldHandle(String, Segment)} and {@link NewWorkflowInstanceRouting#shouldStartNewInstance(String, Segment)}).
 * A given {@code workflowId} therefore maps to the same segment on every node and after every restart, because
 * {@code String.hashCode()} is specified by the JLS and stable across JVMs.
 * Events that do not map to a single instance are sequenced by {@link SequencingPolicy#BROADCAST} and thus delivered
 * to every segment; each segment then acts only on the instances it owns.
 * <p>
 * The segment key of a workflow id is its base part, everything before the first {@code '#'}. Cross-version
 * disambiguated identifiers ({@code base#version}, see {@link NewWorkflowInstanceRouting}) therefore share the segment of
 * their base id: every version of one logical workflow is started, woken and recovered on the same segment.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
@Internal
public final class SegmentedWorkflowRouting implements SequencingPolicy<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(SegmentedWorkflowRouting.class);

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;

    /**
     * Creates the routing for the workflow definitions held by the given registry.
     *
     * @param workflowConfigurationRegistry registry consulted to derive start-candidate workflow ids from business
     *                                      events.
     */
    public SegmentedWorkflowRouting(@Nonnull WorkflowConfigurationRegistry<?> workflowConfigurationRegistry) {
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
    }

    /**
     * Returns whether the given segment owns the given workflow instance. Ownership is decided on the id's
     * {@linkplain #segmentKey(String) segment key}, so all versions of one logical workflow resolve to the same
     * segment.
     *
     * @param segment    the segment to test against.
     * @param workflowId id of the workflow instance.
     * @return {@code true} when the segment matches the id's segment key.
     * @throws IllegalArgumentException when no workflow id is given: an id is required to name a segment, and a
     *                                  diagnosable rejection beats the {@link NullPointerException} deriving the
     *                                  segment key would raise.
     */
    public static boolean ownedBy(@Nonnull Segment segment, @Nonnull String workflowId) {
        if (workflowId == null) {
            throw new IllegalArgumentException(
                    "Cannot decide ownership by segment " + segment + " without a workflow id. The "
                            + "workflowIdProvider of the definition acting on this event derived no id; a "
                            + "misconfigured idProperty, naming a property the event does not carry, is the usual "
                            + "cause.");
        }
        return segment.matches(segmentKey(workflowId));
    }

    /**
     * Returns the segment key of the given workflow id: the base part before the first {@code '#'}. Start placement
     * decides on base ids (see {@code WorkflowEngine#checkAndCreateNewInstance}), while stored instances may carry a
     * cross-version disambiguated id ({@code base#version}); deriving the key from the base part keeps placement,
     * ownership and sequencing consistent for every form of the id.
     *
     * @param workflowId id of the workflow instance, disambiguated or not.
     * @return the segment key the id hashes with.
     */
    private static String segmentKey(@Nonnull String workflowId) {
        var separator = workflowId.indexOf('#');
        return separator == -1 ? workflowId : workflowId.substring(0, separator);
    }

    /**
     * Determines the identifier by which the given event must be sequenced so that the streaming processor delivers
     * it to the segment owning the affected workflow instance. There are three routing cases:
     * <ul>
     *   <li>Engine-emitted events (carrying {@code workflowId} metadata) are sequenced by the id's
     *   {@linkplain #segmentKey(String) segment key}.</li>
     *   <li>Business events are sequenced by the start-candidate workflow id when exactly one registered definition
     *   would start from the event, so new instances are created on the segment that owns them.</li>
     *   <li>All other events (no or multiple start candidates) may need to wake waiting instances resident in any
     *   segment; without a durable wait-association table the only correct routing is delivery to all segments, so
     *   they are sequenced by {@link SequencingPolicy#BROADCAST}. Each segment then evaluates the wait conditions of
     *   the instances it owns.</li>
     * </ul>
     *
     * @param eventMessage      the event to sequence.
     * @param processingContext the processing context of the event.
     * @return the workflow-aware sequence identifier, or {@link SequencingPolicy#BROADCAST} when the event does not
     *         map to a single workflow instance.
     */
    @Override
    @Nonnull
    public Optional<Object> sequenceIdentifierFor(@Nonnull EventMessage eventMessage,
                                                  @Nonnull ProcessingContext processingContext) {
        if (MetadataUtils.hasWorkflowId().test(eventMessage.metadata())) {
            return Optional.of(segmentKey(MetadataUtils.getWorkflowId(eventMessage.metadata())));
        }
        var candidates = newInstanceCandidateIds(eventMessage, processingContext);
        return candidates.size() == 1 ? Optional.of(segmentKey(candidates.iterator().next()))
                                      : Optional.of(SequencingPolicy.BROADCAST);
    }

    /**
     * Decides whether this segment may act on the given workflow instance (handle its engine events, wake it with a
     * business event): a segment only processes instances it owns. Engine events are sequenced by {@code workflowId}
     * (see {@link #sequenceIdentifierFor(EventMessage, ProcessingContext)}) and thus arrive at the owning segment;
     * broadcast business events (sequenced by {@link SequencingPolicy#BROADCAST}) arrive at every segment and this
     * decision degrades them to a no-op everywhere except at the owner - keeping wakes exactly-once per instance.
     *
     * @param workflowId id of the workflow instance.
     * @param segment    the segment the event is processed under, or {@code null} when processed outside a segmented
     *                   processor.
     * @return {@code true} when this segment owns the instance (or no segment is present).
     */
    public boolean shouldHandle(@Nonnull String workflowId, @Nullable Segment segment) {
        if (segment != null && !ownedBy(segment, workflowId)) {
            logger.debug("Ignoring event for workflowId '{}' - instance is owned by another segment than {}.",
                         workflowId, segment);
            return false;
        }
        return true;
    }

    /**
     * Base workflow ids of all definitions that would start a new instance from the given event: the highest
     * registered version of each definition whose start condition matches. Mirrors the matching performed by the
     * engine's start path.
     */
    private Set<String> newInstanceCandidateIds(@Nonnull EventMessage eventMessage,
                                          @Nonnull ProcessingContext processingContext) {
        try {
            return workflowConfigurationRegistry
                    .getHighestVersionConfigurations(eventMessage.type().qualifiedName())
                    .stream()
                    .filter(configuration -> configuration.predicate().test(eventMessage, processingContext))
                    .map(configuration -> configuration.configuration().workflowIdProvider().apply(eventMessage))
                    // A provider that derives no id contributes no candidate. Dropping it explicitly keeps the
                    // candidates of its healthy siblings, which the collector's own rejection of nulls would discard
                    // together with the whole stream. The start path reports and skips that definition.
                    .filter(Objects::nonNull)
                    .collect(Collectors.toUnmodifiableSet());
        } catch (RuntimeException e) {
            // Sequencing runs before the event reaches a handler, so the payload conversion a start condition or
            // workflowIdProvider needs is not always available yet. Routing must not fail the work package for that:
            // reporting no candidate degrades this event to a broadcast, which every ownership guard then narrows
            // back to exactly-once. Costs one delivery per segment for such events, never correctness.
            logger.debug("Could not derive start candidates for event {}; broadcasting it to every segment.",
                         eventMessage.type(), e);
            return Set.of();
        }
    }
}
