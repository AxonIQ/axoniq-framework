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
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Utility to work on metadata of workflow events.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
public class MetadataUtils {

    /**
     * Metadata key storing the workflow instance identifier.
     */
    public static final String METADATA_KEY_WORKFLOW_ID = "workflowId";

    /**
     * Metadata key storing the step status value.
     */
    public static final String METADATA_KEY_TYPE = "stepType";

    /**
     * Metadata key storing the step primitive classification.
     */
    public static final String METADATA_KEY_STEP_PRIMITIVE = "stepPrimitive";

    /**
     * Metadata key storing the payload reducer name to apply after a step result.
     */
    public static final String METADATA_KEY_MODIFY_PAYLOAD = "modifyPayload";

    /**
     * Metadata key storing the workflow step name.
     */
    public static final String METADATA_KEY_STEP_NAME = "stepName";

    /**
     * Metadata key storing the workflow lifecycle status value.
     */
    public static final String METADATA_KEY_WORKFLOW_STATUS = "workflowStatus";

    /**
     * Metadata key storing the workflow definition name.
     */
    public static final String METADATA_KEY_WORKFLOW_DEFINITION_NAME = "workflowDefinitionName";

    /**
     * Metadata key storing the workflow definition version.
     */
    public static final String METADATA_KEY_WORKFLOW_DEFINITION_VERSION = "workflowDefinitionVersion";

    /**
     * Metadata key storing the identifier of a recorded workflow version change.
     */
    public static final String METADATA_KEY_VERSION_CHANGE_ID = "versionChangeId";

    /**
     * Metadata key storing the workflow version associated with a version marker event.
     */
    public static final String METADATA_KEY_VERSION = "version";

    /**
     * Marker value identifying a step as a wait-for-event primitive.
     */
    public static final String STEP_PRIMITIVE_WAIT_FOR_EVENT = "WAIT_FOR_EVENT";

    private MetadataUtils() {
        // avoid
    }

    /**
     * Creates a new metadata instance with the given workflow id.
     *
     * @param workflowId the workflow id
     * @return metadata instance
     */
    public static Metadata create(String workflowId) {
        return Metadata
                .with(METADATA_KEY_WORKFLOW_ID, workflowId);
    }

    /**
     * Creates a new metadata instance with the given workflow id and step name.
     *
     * @param workflowId the workflow id
     * @param stepName   the step name
     * @param stepStatus the step status
     * @return metadata instance
     */
    public static Metadata create(String workflowId, String stepName, StepStatus stepStatus) {
        return create(workflowId)
                .and(METADATA_KEY_TYPE, stepStatus.name())
                .and(METADATA_KEY_STEP_NAME, stepName);
    }

    /**
     * Creates a new metadata instance with the given workflow id and workflow status.
     *
     * @param workflowId     the workflow id
     * @param workflowStatus the workflow status
     * @return metadata instance
     */
    public static Metadata create(String workflowId, WorkflowStatus workflowStatus) {
        return create(workflowId)
                .and(METADATA_KEY_WORKFLOW_STATUS, workflowStatus.name());
    }

    /**
     * Creates a new metadata instance with the given workflow id, workflow status, and workflow definition id.
     *
     * @param workflowId           the workflow id
     * @param workflowStatus       the workflow status
     * @param workflowDefinitionId the workflow definition identifier
     * @return metadata instance
     */
    public static Metadata create(String workflowId,
                                  WorkflowStatus workflowStatus,
                                  MessageType workflowDefinitionId) {
        return withWorkflowDefinitionId(create(workflowId, workflowStatus), workflowDefinitionId);
    }

    /**
     * Metadata for a version-marker event: a COMPLETED step event with {@code stepName = changeId} plus marker keys
     * ({@code versionChangeId}, {@code version}) whose presence flags this as a version marker.
     *
     * @param workflowId the workflow id
     * @param changeId   the change id
     * @param version    the version
     * @return metadata instance
     */
    public static Metadata createVersionMigrationStep(String workflowId, String changeId, String version) {
        return create(workflowId, changeId, StepStatus.COMPLETED)
                .and(METADATA_KEY_VERSION_CHANGE_ID, changeId)
                .and(METADATA_KEY_VERSION, version);
    }

    /**
     * Marks the given metadata as a step that is waiting for an event.
     *
     * @param metadata metadata to mark
     * @return enriched metadata
     */
    public static Metadata markWaitForEventStep(Metadata metadata) {
        return metadata.and(METADATA_KEY_STEP_PRIMITIVE, STEP_PRIMITIVE_WAIT_FOR_EVENT);
    }

    /**
     * Returns the workflow status if present in the metadata.
     *
     * @param metadata metadata to inspect
     * @return optional of workflow status
     */
    public static Optional<WorkflowStatus> getWorkflowStatus(Metadata metadata) {
        if (metadata.containsKey(METADATA_KEY_WORKFLOW_STATUS)) {
            return Optional.of(WorkflowStatus.valueOf(metadata.get(METADATA_KEY_WORKFLOW_STATUS)));
        }
        return Optional.empty();
    }

    /**
     * Returns the step status if present in the metadata.
     *
     * @param metadata metadata to inspect
     * @return optional of step status
     */
    public static Optional<StepStatus> getStepStatus(Metadata metadata) {
        if (metadata.containsKey(METADATA_KEY_TYPE)) {
            return Optional.of(StepStatus.valueOf(metadata.get(METADATA_KEY_TYPE)));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns the version change id if present in the metadata.
     *
     * @param metadata metadata to inspect
     * @return optional of version change id
     */
    public static Optional<String> getVersionChangeId(Metadata metadata) {
        return Optional.ofNullable(metadata.getOrDefault(METADATA_KEY_VERSION_CHANGE_ID, null));
    }

    /**
     * Returns the version if present in the metadata.
     *
     * @param metadata metadata to inspect
     * @return optional of version of the workflow
     */
    public static Optional<String> getVersion(Metadata metadata) {
        return Optional.ofNullable(metadata.getOrDefault(METADATA_KEY_VERSION, null));
    }

    /**
     * Enriches metadata with the workflow definition id.
     *
     * @param metadata             metadata to enrich
     * @param workflowDefinitionId workflow definition id to store
     * @return enriched metadata
     */
    public static Metadata withWorkflowDefinitionId(Metadata metadata, MessageType workflowDefinitionId) {
        return metadata.and(METADATA_KEY_WORKFLOW_DEFINITION_NAME, workflowDefinitionId.qualifiedName().toString())
                       .and(METADATA_KEY_WORKFLOW_DEFINITION_VERSION, workflowDefinitionId.version());
    }

    /**
     * Returns the workflow definition id if present in the metadata.
     *
     * @param metadata metadata to inspect
     * @return optional workflow definition id
     */
    public static Optional<MessageType> getWorkflowDefinitionId(Metadata metadata) {
        if (!metadata.containsKey(METADATA_KEY_WORKFLOW_DEFINITION_NAME)
                || !metadata.containsKey(METADATA_KEY_WORKFLOW_DEFINITION_VERSION)) {
            return Optional.empty();
        }
        return Optional.of(new MessageType(
                new QualifiedName(metadata.get(METADATA_KEY_WORKFLOW_DEFINITION_NAME)),
                metadata.get(METADATA_KEY_WORKFLOW_DEFINITION_VERSION)
        ));
    }

    /**
     * Returns whether the given metadata carries a version marker.
     *
     * @return {@code true} if the metadata carries a {@code versionChangeId} key — i.e. it's a version marker.
     */
    public static boolean isVersionMigrationStep(Metadata metadata) {
        return metadata.containsKey(METADATA_KEY_VERSION_CHANGE_ID);
    }

    /**
     * Returns the payload reducer if present in the metadata.
     *
     * @param metadata metadata to inspect
     * @return optional of payload reducer
     */
    public static Optional<String> payloadReducer(Metadata metadata) {
        if (metadata.containsKey(METADATA_KEY_MODIFY_PAYLOAD)) {
            return Optional.ofNullable(metadata.getOrDefault(METADATA_KEY_MODIFY_PAYLOAD, null));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns the workflow id from the metadata.
     *
     * @param metadata metadata to inspect
     * @return workflow id
     */
    public static String getWorkflowId(Metadata metadata) {
        if (!hasWorkflowId().test(metadata)) {
            throw new IllegalArgumentException("Metadata contains no workflow id");
        }
        return metadata.get(METADATA_KEY_WORKFLOW_ID);
    }

    /**
     * Returns the step name from the metadata.
     *
     * @param metadata metadata to inspect
     * @return step name
     */
    public static String getStepName(Metadata metadata) {
        return metadata.getOrDefault(METADATA_KEY_STEP_NAME, null);
    }

    /**
     * Returns whether the given metadata carries a step that is waiting for an event.
     *
     * @param metadata metadata to inspect
     * @return {@code true} iff the metadata carries a {@code stepPrimitive} key with value {@code WAIT_FOR_EVENT}.
     */
    public static boolean isWaitForEventStep(Metadata metadata) {
        return STEP_PRIMITIVE_WAIT_FOR_EVENT.equals(metadata.getOrDefault(METADATA_KEY_STEP_PRIMITIVE, null));
    }

    /**
     * Returns a predicate that matches metadata with the given workflow id.
     *
     * @param workflowId workflow id to match
     * @return predicate on metadata
     */
    public static Predicate<Metadata> workflowIdFilter(String workflowId) {
        return m -> m.containsKey(METADATA_KEY_WORKFLOW_ID) && workflowId.equals(m.get(METADATA_KEY_WORKFLOW_ID));
    }

    /**
     * Predicate that matches metadata that contain a workflow id.
     *
     * @return predicate on metadata
     */
    public static Predicate<Metadata> hasWorkflowId() {
        return m -> m.containsKey(METADATA_KEY_WORKFLOW_ID);
    }
}
