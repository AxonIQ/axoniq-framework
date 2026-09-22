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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.ArrayList;
import java.util.Set;

import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.*;

/**
 * Resolves workflow-specific event-store tags for engine-published events.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class WorkflowEventTagResolver implements TagResolver {


    @Override
    public Set<Tag> resolve(EventMessage eventMessage) {
        var tags = new ArrayList<Tag>();
        Metadata metadata = eventMessage.metadata();

        if (isEnginePublishedWorkflowEvent(metadata)) {
            tags.add(Tag.of(TAG_WORKFLOW_ID, MetadataUtils.getWorkflowId(metadata)));
        }

        MetadataUtils.getWorkflowStatus(metadata)
                     .map(WorkflowEventTagResolver::lifecycleValue)
                     .map(value -> Tag.of(TAG_WORKFLOW_EVENT_TYPE, value))
                     .ifPresent(tags::add);

        if (MetadataUtils.isWaitForEventStep(metadata)) {
            MetadataUtils.getStepStatus(metadata)
                         .filter(status -> status == StepStatus.STARTED
                                 || status == StepStatus.COMPLETED
                                 || status == StepStatus.CANCELLED
                                 || status == StepStatus.TIMED_OUT)
                         .map(WorkflowEventTagResolver::lifecycleValue)
                         .map(value -> Tag.of(TAG_WORKFLOW_EVENT_TYPE, value))
                         .ifPresent(tags::add);
        }

        return Set.copyOf(tags);
    }

    private static boolean isEnginePublishedWorkflowEvent(Metadata metadata) {
        return MetadataUtils.hasWorkflowId().test(metadata)
                && (MetadataUtils.getWorkflowStatus(metadata).isPresent()
                || MetadataUtils.getStepStatus(metadata).isPresent());
    }

    private static String lifecycleValue(WorkflowStatus status) {
        return switch (status) {
            case NONE -> throw new IllegalArgumentException("Workflow lifecycle tag is undefined for status NONE");
            case STARTED, COMPLETED, FAILED, TIMED_OUT, CANCELLED -> TAG_VALUE_EVENT_TYPE_LIFECYCLE;
        };
    }

    private static String lifecycleValue(StepStatus status) {
        return switch (status) {
            case STARTED, COMPLETED, TIMED_OUT, CANCELLED -> TAG_VALUE_EVENT_TYPE_WAIT_STEP;
            case RETRYING, RETRY_STARTED, FAILED -> throw new IllegalArgumentException(
                    "Wait for step lifecycle tag is undefined for status " + status);
        };
    }
}
