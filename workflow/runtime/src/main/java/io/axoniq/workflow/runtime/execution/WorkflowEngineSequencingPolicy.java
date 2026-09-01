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
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Sequences events so that the streaming processor delivers each one to the segment owning the affected workflow
 * instance, implementing the {@link SequencingPolicy} the workflow event handling components run with.
 * <p>
 * Placement follows {@link WorkflowSegmentOwnership}, the ownership rule the {@link WorkflowEngine} decides on as well,
 * so sequencing and handling never disagree about which segment an instance belongs to. Events that do not map to a
 * single instance are sequenced by {@link SequencingPolicy#BROADCAST} and thus delivered to every segment; each segment
 * then acts only on the instances it owns.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
@Internal final class WorkflowEngineSequencingPolicy implements SequencingPolicy<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEngineSequencingPolicy.class);

    private final WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;

    /**
     * Creates the routing for the workflow definitions held by the given registry.
     *
     * @param workflowConfigurationRegistry registry consulted to derive start-candidate workflow ids from business
     *                                      events
     */
    WorkflowEngineSequencingPolicy(WorkflowConfigurationRegistry<?> workflowConfigurationRegistry) {
        this.workflowConfigurationRegistry = workflowConfigurationRegistry;
    }

    /**
     * Determines the identifier by which the given event must be sequenced so that the streaming processor delivers it
     * to the segment owning the affected workflow instance. There are three routing cases:
     * <ul>
     *   <li>Engine-emitted events (carrying {@code workflowId} metadata) are sequenced by the id's
     *   {@linkplain WorkflowSegmentOwnership#segmentKey(String) segment key}.</li>
     *   <li>Business events are sequenced by the start-candidate workflow id when exactly one registered definition
     *   would start from the event, so new instances are created on the segment that owns them.</li>
     *   <li>All other events (no or multiple start candidates) may need to wake waiting instances resident in any
     *   segment; without a durable wait-association table the only correct routing is delivery to all segments, so
     *   they are sequenced by {@link SequencingPolicy#BROADCAST}. Each segment then evaluates the wait conditions of
     *   the instances it owns.</li>
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
        var metadata = eventMessage.metadata();
        if (MetadataUtils.hasWorkflowId().test(metadata)) {
            return Optional.of(WorkflowSegmentOwnership.segmentKey(MetadataUtils.getWorkflowId(metadata)));
        }
        var candidates = newInstanceCandidateIds(eventMessage, processingContext);
        return candidates.size() == 1
                ? Optional.of(WorkflowSegmentOwnership.segmentKey(candidates.iterator().next()))
                : Optional.of(SequencingPolicy.BROADCAST);
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
