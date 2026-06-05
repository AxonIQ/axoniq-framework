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
package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Metadata;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Utility to work on metadata of workflow events.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 */
@Internal
public class MetadataUtils {

    public static final String METADATA_KEY_WORKFLOW_ID = "workflowId";
    public static final String METADATA_KEY_TYPE = "stepType";
    public static final String METADATA_KEY_MODIFY_PAYLOAD = "modifyPayload";
    public static final String METADATA_KEY_STEP_NAME = "stepName";
    public static final String METADATA_KEY_WORKFLOW_STATUS = "workflowStatus";
    public static final String METADATA_KEY_VERSION_CHANGE_ID = "versionChangeId";
    public static final String METADATA_KEY_VERSION = "version";

    private MetadataUtils() {
        // avoid
    }

    public static Metadata create(String workflowId) {
        return Metadata
                .with(METADATA_KEY_WORKFLOW_ID, workflowId);
    }

    public static Metadata create(String workflowId, String stepName, StepStatus stepStatus) {
        return create(workflowId)
                .and(METADATA_KEY_TYPE, stepStatus.name())
                .and(METADATA_KEY_STEP_NAME, stepName);
    }

    public static Metadata create(String workflowId, WorkflowStatus workflowStatus) {
        return create(workflowId)
                .and(METADATA_KEY_WORKFLOW_STATUS, workflowStatus.name());
    }

    /**
     * Metadata for a version-marker event: a COMPLETED step event with {@code stepName = changeId}
     * plus marker keys ({@code versionChangeId}, {@code version}) whose presence flags this as a
     * version marker.
     */
    public static Metadata createVersionMigrationStep(String workflowId, String changeId, String version) {
        return create(workflowId, changeId, StepStatus.COMPLETED)
                .and(METADATA_KEY_VERSION_CHANGE_ID, changeId)
                .and(METADATA_KEY_VERSION, version);
    }

    public static Optional<WorkflowStatus> getWorkflowStatus(Metadata metadata) {
        if (metadata.containsKey(METADATA_KEY_WORKFLOW_STATUS)) {
            return Optional.of(WorkflowStatus.valueOf(metadata.get(METADATA_KEY_WORKFLOW_STATUS)));
        }
        return Optional.empty();
    }

    public static Optional<StepStatus> getStepStatus(Metadata metadata) {
        if (metadata.containsKey(METADATA_KEY_TYPE)) {
            return Optional.of(StepStatus.valueOf(metadata.get(METADATA_KEY_TYPE)));
        } else {
            return Optional.empty();
        }
    }

    public static Optional<String> getVersionChangeId(Metadata metadata) {
        return Optional.ofNullable(metadata.getOrDefault(METADATA_KEY_VERSION_CHANGE_ID, null));
    }

    public static Optional<String> getVersion(Metadata metadata) {
        return Optional.ofNullable(metadata.getOrDefault(METADATA_KEY_VERSION, null));
    }

    /** {@code true} iff the metadata carries a {@code versionChangeId} key — i.e. it's a version marker. */
    public static boolean isVersionMigrationStep(Metadata metadata) {
        return metadata.containsKey(METADATA_KEY_VERSION_CHANGE_ID);
    }

    public static Optional<String> payloadReducer(Metadata metadata) {
        if (metadata.containsKey(METADATA_KEY_MODIFY_PAYLOAD)) {
            return Optional.ofNullable(metadata.getOrDefault(METADATA_KEY_MODIFY_PAYLOAD, null));
        } else {
            return Optional.empty();
        }
    }

    public static String getWorkflowId(Metadata metadata) {
        if (!hasWorkflowId().test(metadata)) {
            throw new IllegalArgumentException("Metadata contains no workflow id");
        }
        return metadata.get(METADATA_KEY_WORKFLOW_ID);
    }

    public static String getStepName(Metadata metadata) {
        return metadata.getOrDefault(METADATA_KEY_STEP_NAME, null);
    }

    public static Predicate<Metadata> workflowIdFilter(String workflowId) {
        return m -> m.containsKey(METADATA_KEY_WORKFLOW_ID) && workflowId.equals(m.get(METADATA_KEY_WORKFLOW_ID));
    }

    public static Predicate<Metadata> hasWorkflowId() {
        return m -> m.containsKey(METADATA_KEY_WORKFLOW_ID);
    }
}
