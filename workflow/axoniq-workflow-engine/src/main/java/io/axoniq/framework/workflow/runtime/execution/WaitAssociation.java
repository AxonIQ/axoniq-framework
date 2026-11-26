/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepWaitInfo;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

/**
 * One open wait of a workflow instance: the event the wait expects, and the associations that event must satisfy.
 * <p>
 * A wait association is built from the wait-step STARTED event of a {@code waitForEvent} step, whose payload names the
 * awaited event and its associations. The {@link WorkflowEngine} uses wait associations to route an incoming event to
 * the workflows waiting for it only, instead of to every workflow. A wait association is a routing hint and never
 * decides a wake on its own: the waiting execution evaluates its full wait condition, including any predicate, when the
 * event arrives. A wait association may therefore match more events than the wait accepts, but never fewer.
 * <p>
 * A wait association whose STARTED payload cannot be read has no {@link #eventName()} and matches every routed event.
 *
 * @param workflowId   the identifier of the waiting workflow
 * @param stepName     the name of the waiting step, unique within the workflow
 * @param eventName    the qualified name of the awaited event, or {@code null} when the STARTED payload is unreadable
 * @param associations the canonical serialized associations the awaited event must all satisfy
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public record WaitAssociation(String workflowId,
                              String stepName,
                              @Nullable QualifiedName eventName,
                              Set<String> associations) {

    private static final Logger logger = LoggerFactory.getLogger(WaitAssociation.class);

    /**
     * Creates a wait association, copying the given associations.
     *
     * @param workflowId   the identifier of the waiting workflow
     * @param stepName     the name of the waiting step, unique within the workflow
     * @param eventName    the qualified name of the awaited event, or {@code null} when the STARTED payload is
     *                     unreadable
     * @param associations the canonical serialized associations the awaited event must all satisfy
     */
    public WaitAssociation {
        Objects.requireNonNull(workflowId, "The workflowId must not be null.");
        Objects.requireNonNull(stepName, "The stepName must not be null.");
        associations = Set.copyOf(Objects.requireNonNull(associations, "The associations must not be null."));
    }

    /**
     * Builds the wait association of the wait started by the given wait-step STARTED event.
     * <p>
     * Reads the payload as the {@link StepWaitInfo} the wait step publishes. Never throws on the payload: a payload
     * that cannot be read yields a wait association without an {@link #eventName()}, which matches every routed event.
     *
     * @param startedWaitEvent a wait-step STARTED event, carrying {@code workflowId} and {@code stepName} metadata
     * @return the wait association of the started wait
     */
    public static WaitAssociation fromStartedEvent(EventMessage startedWaitEvent) {
        var metadata = startedWaitEvent.metadata();
        var workflowId = MetadataUtils.getWorkflowId(metadata);
        var stepName = Objects.requireNonNull(MetadataUtils.getStepName(metadata),
                                              "A wait-step STARTED event must carry a stepName.");
        try {
            StepWaitInfo waitInfo = startedWaitEvent.payloadAs(StepWaitInfo.class);
            if (waitInfo != null && waitInfo.eventName() instanceof String name && !name.isEmpty()) {
                return new WaitAssociation(workflowId,
                                           stepName,
                                           new QualifiedName(name),
                                           Objects.requireNonNullElse(waitInfo.associations(), Set.of()));
            }
            logger.debug("Wait [{}] of workflow [{}] names no awaited event; it matches every routed event.",
                         stepName, workflowId);
        } catch (RuntimeException e) {
            // Two cases end here: routing reads the payload before the event reaches a handler, when the converter
            // cannot always convert it yet, or sourcing meets an old or broken payload. A wildcard is correct for both.
            logger.debug("Could not read the payload of wait [{}] of workflow [{}]; it matches every routed event.",
                         stepName, workflowId, e);
        }
        return new WaitAssociation(workflowId, stepName, null, Set.of());
    }

    /**
     * Returns whether this wait association matches every routed event, because its STARTED payload could not be read.
     * <p>
     * Routing reads that payload before the event reaches a handler, when it cannot always be converted yet. Matching
     * every event keeps such a wait from missing the event it waits for.
     *
     * @return {@code true} when this wait association has no {@link #eventName()}
     */
    public boolean isWildcard() {
        return eventName == null;
    }
}
