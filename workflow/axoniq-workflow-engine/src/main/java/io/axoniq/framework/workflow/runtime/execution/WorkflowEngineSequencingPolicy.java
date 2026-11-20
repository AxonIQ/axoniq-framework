/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Sequences events so that the streaming processor delivers each one to the segment owning the affected workflow
 * instance, implementing the {@link SequencingPolicy} the workflow event handling components run with.
 * <p>
 * Placement follows {@link WorkflowSegmentOwnership}, the ownership rule the {@link WorkflowEngine} decides on as well,
 * so sequencing and handling never disagree about which segment an instance belongs to. A business event goes to the
 * workflows waiting for it, found in the {@link WaitAssociationIndex}; when unsure, it is broadcast.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal final class WorkflowEngineSequencingPolicy implements SequencingPolicy<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineSequencingPolicy.class);

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;
    private final WaitAssociationIndex waitIndex;
    private final Predicate<Segment> isCaughtUp;

    /**
     * Creates the routing for the workflow definitions held by the given registry.
     *
     * @param workflowConfigurationRegistry registry consulted to derive start-candidate workflow ids from business
     *                                      events
     * @param waitIndex                     the open waits of the workflows on this node's claimed segments
     * @param isCaughtUp                    tells whether a claimed segment consumed its startup backlog
     */
    WorkflowEngineSequencingPolicy(WorkflowConfigurationRegistry<?> workflowConfigurationRegistry,
                                   WaitAssociationIndex waitIndex,
                                   Predicate<Segment> isCaughtUp) {
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
        this.waitIndex = waitIndex;
        this.isCaughtUp = isCaughtUp;
    }

    /**
     * Determines the identifier by which the given event must be sequenced so that the streaming processor delivers it
     * to the segment owning the affected workflow instance. There are three routing cases:
     * <ul>
     *   <li>Events published by a workflow through the publish primitive ({@code stepPrimitive=PUBLISH}) carry
     *   {@code workflowId} metadata, but are business events for every other instance: they may start or wake
     *   instances resident in any segment, and the publisher itself must observe them to complete its step. They are
     *   routed like the business events below, with the publisher counted as a waiting workflow.</li>
     *   <li>Every other engine-emitted event (carrying {@code workflowId} metadata) is sequenced by that id, the key
     *   {@link WorkflowSegmentOwnership} decides ownership on.</li>
     *   <li>Business events are sequenced by the start-candidate workflow id when exactly one registered definition
     *   would start from the event, so new instances are created on the segment that owns them.</li>
     *   <li>All other events may need to wake waiting instances resident in any segment. They are sequenced by the
     *   single workflow waiting for them in the {@link WaitAssociationIndex}, and by {@link SequencingPolicy#BROADCAST}
     *   when more workflows may need them or the index is not reliable yet. Each segment then evaluates the wait
     *   conditions of the instances it owns.</li>
     * </ul>
     *
     * @param eventMessage      the event to sequence
     * @param processingContext the processing context of the event
     * @return the workflow-aware sequence identifier, or {@link SequencingPolicy#BROADCAST} when the event does not map
     * map to a single workflow instance
     */
    @Override
    public Optional<Object> sequenceIdentifierFor(EventMessage eventMessage,
                                                  ProcessingContext processingContext) {
        trackWaitsInStreamOrder(eventMessage, processingContext);
        var metadata = eventMessage.metadata();
        if (belongsToOneWorkflow(metadata)) {
            return Optional.of(MetadataUtils.getWorkflowId(metadata));
        }
        try {
            return Optional.of(route(eventMessage, processingContext, MetadataUtils.isPublishStep(metadata)));
        } catch (RuntimeException e) {
            // Sequencing runs before the event reaches a handler, so a value the index reads from the event may not be
            // available yet. Routing must not fail the work package for that: broadcasting is always correct.
            logger.debug("Could not route event {} through the wait associations; broadcasting it to every segment.",
                         eventMessage.type(), e);
            return Optional.of(SequencingPolicy.BROADCAST);
        }
    }

    /**
     * Applies the event to the {@link WaitAssociationIndex} on the evaluation without a segment, which the processor
     * makes in stream order before any handler runs. Updating the index in a handler instead would let it lag behind
     * routing: a wait opened by one event could be missing when the next event is routed, which then misses the wait.
     * Applying an event twice changes nothing.
     */
    private void trackWaitsInStreamOrder(EventMessage eventMessage, ProcessingContext processingContext) {
        if (Segment.fromContext(processingContext).isEmpty()) {
            waitIndex.evolve(eventMessage);
        }
    }

    /**
     * Whether the event concerns only the workflow whose id it carries, so it goes to the segment owning that workflow.
     * A published event carries the publisher's id too, but it is a business event for every other workflow.
     */
    private static boolean belongsToOneWorkflow(Metadata metadata) {
        return MetadataUtils.hasWorkflowId().test(metadata) && !MetadataUtils.isPublishStep(metadata);
    }

    private Object route(EventMessage eventMessage, ProcessingContext processingContext, boolean publish) {
        var targets = new HashSet<String>();
        if (isStartEvent(eventMessage)) {
            var candidates = newInstanceCandidateIds(eventMessage, processingContext);
            if (publish || candidates.size() != 1) {
                return SequencingPolicy.BROADCAST;
            }
            targets.addAll(candidates);
        } else if (publish) {
            targets.add(MetadataUtils.getWorkflowId(eventMessage.metadata()));
        }
        if (!waitIndex.allClaimedSegmentsLoaded() || !waitIndex.allClaimedSegmentsMatch(isCaughtUp)) {
            return SequencingPolicy.BROADCAST;
        }
        targets.addAll(waitIndex.match(eventMessage, processingContext));
        return switch (targets.size()) {
            // no workflow needs the event: the event's own identifier hands it to a single segment
            case 0 -> eventMessage.identifier();
            case 1 -> targets.iterator().next();
            default -> SequencingPolicy.BROADCAST;
        };
    }

    private boolean isStartEvent(EventMessage eventMessage) {
        return !workflowConfigurationRegistry.getHighestVersionConfigurations(eventMessage.type()).isEmpty();
    }

    /**
     * Base workflow ids of all definitions that would start a new instance from the given event: the highest registered
     * version of each definition whose start condition matches. Mirrors the matching performed by the engine's start
     * path.
     */
    private Set<String> newInstanceCandidateIds(EventMessage eventMessage,
                                                ProcessingContext processingContext) {
        try {
            return workflowConfigurationRegistry
                    .getHighestVersionConfigurations(eventMessage.type())
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
