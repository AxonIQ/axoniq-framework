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
package io.axoniq.framework.workflow.query.api;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.messaging.core.VersionedType;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.axonframework.common.BuilderUtils.assertNonEmpty;

/**
 * Immutable value-based criteria for selecting workflow states.
 * <p>
 * Start with the {@code by...} factory for the first restriction, then add restrictions using the instance methods.
 * Each restriction is combined with the existing restrictions using logical AND. For example:
 * <pre>{@code
 * WorkflowStateQuery query = WorkflowStateQuery.byWorkflowId(orderId)
 *                                            .workflowStatus(WorkflowStatus.STARTED)
 *                                            .stepStatus("reserve-funds", StepStatus.COMPLETED);
 * }</pre>
 * Use {@link #all()} only for an unrestricted search. Queries contain data only; they intentionally do not accept
 * {@link Predicate predicates} or other executable filters. The same query can be supplied to
 * workflow-management finders and workflow-state repositories, which interpret the {@linkplain #criteria() criteria}
 * in their own storage model.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public final class WorkflowStateQuery {

    private final List<Criterion> criteria;

    private WorkflowStateQuery(List<Criterion> criteria) {
        this.criteria = List.copyOf(criteria);
    }

    /**
     * Creates a query without restrictions.
     *
     * @return an unrestricted workflow state query
     */
    public static WorkflowStateQuery all() {
        return new WorkflowStateQuery(List.of());
    }

    /**
     * Creates a query restricted to a workflow identifier.
     *
     * @param workflowId workflow identifier to match
     * @return a query with the workflow identifier restriction
     */
    public static WorkflowStateQuery byWorkflowId(String workflowId) {
        return all().workflowId(workflowId);
    }

    /**
     * Creates a query restricted to a workflow definition identity.
     *
     * @param workflowDefinitionId workflow definition identity to match
     * @return a query with the workflow definition identity restriction
     */
    public static WorkflowStateQuery byWorkflowDefinitionId(VersionedType workflowDefinitionId) {
        return all().workflowDefinitionId(workflowDefinitionId);
    }

    /**
     * Creates a query restricted to a workflow status.
     *
     * @param workflowStatus workflow status to match
     * @return a query with the workflow status restriction
     */
    public static WorkflowStateQuery byWorkflowStatus(WorkflowStatus workflowStatus) {
        return all().workflowStatus(workflowStatus);
    }

    /**
     * Creates a query restricted to workflows containing a step.
     *
     * @param stepName step name that must exist
     * @return a query with the step existence restriction
     */
    public static WorkflowStateQuery byStep(String stepName) {
        return all().step(stepName);
    }

    /**
     * Creates a query restricted to a status of a named step.
     *
     * @param stepName step name to inspect
     * @param stepStatus step status to match
     * @return a query with the step status restriction
     */
    public static WorkflowStateQuery byStepStatus(String stepName, StepStatus stepStatus) {
        return all().stepStatus(stepName, stepStatus);
    }

    /**
     * Creates a query restricted to an exact payload value.
     *
     * @param key payload entry name to inspect
     * @param value payload value to match, which may be {@code null}
     * @return a query with the payload value restriction
     */
    public static WorkflowStateQuery byPayloadValue(String key, @Nullable Object value) {
        return all().payloadValue(key, value);
    }

    /**
     * Creates a query restricted to an effective version recorded for a change identifier.
     *
     * @param changeId change identifier to inspect
     * @param version effective version to match
     * @return a query with the effective-version restriction
     */
    public static WorkflowStateQuery byVersion(String changeId, String version) {
        return all().version(changeId, version);
    }

    /**
     * Creates a query restricted to workflows with a recorded version migration.
     *
     * @param changeId change identifier that must have a recorded migration
     * @return a query with the version-migration restriction
     */
    public static WorkflowStateQuery byVersionMigration(String changeId) {
        return all().versionMigration(changeId);
    }

    /**
     * Restricts this query to a workflow identifier.
     *
     * @param workflowId workflow identifier to match
     * @return query with the workflow identifier restriction
     */
    public WorkflowStateQuery workflowId(String workflowId) {
        return append(new WorkflowIdCriterion(workflowId));
    }

    /**
     * Restricts this query to a workflow definition identity.
     *
     * @param workflowDefinitionId workflow definition identity to match
     * @return query with the workflow definition identity restriction
     */
    public WorkflowStateQuery workflowDefinitionId(VersionedType workflowDefinitionId) {
        return append(new WorkflowDefinitionIdCriterion(workflowDefinitionId));
    }

    /**
     * Restricts this query to a workflow status.
     *
     * @param workflowStatus workflow status to match
     * @return query with the workflow status restriction
     */
    public WorkflowStateQuery workflowStatus(WorkflowStatus workflowStatus) {
        return append(new WorkflowStatusCriterion(workflowStatus));
    }

    /**
     * Restricts this query to workflows containing a step.
     *
     * @param stepName step name that must exist
     * @return query with the step existence restriction
     */
    public WorkflowStateQuery step(String stepName) {
        return append(new StepCriterion(stepName));
    }

    /**
     * Restricts this query to a status of a named step.
     *
     * @param stepName step name to inspect
     * @param stepStatus step status to match
     * @return query with the step status restriction
     */
    public WorkflowStateQuery stepStatus(String stepName, StepStatus stepStatus) {
        return append(new StepStatusCriterion(stepName, stepStatus));
    }

    /**
     * Restricts this query to a payload value.
     *
     * @param key payload entry name to inspect
     * @param value payload value to match, which may be {@code null}
     * @return query with the payload value restriction
     */
    public WorkflowStateQuery payloadValue(String key, @Nullable Object value) {
        return append(new PayloadValueCriterion(key, value));
    }

    /**
     * Restricts this query to an effective version recorded for a change identifier.
     *
     * @param changeId change identifier to inspect
     * @param version effective version to match
     * @return query with the effective version restriction
     */
    public WorkflowStateQuery version(String changeId, String version) {
        return append(new VersionCriterion(changeId, version));
    }

    /**
     * Restricts this query to a recorded version migration.
     *
     * @param changeId change identifier that must have a recorded migration
     * @return query with the version migration restriction
     */
    public WorkflowStateQuery versionMigration(String changeId) {
        return append(new VersionMigrationCriterion(changeId));
    }

    /**
     * Returns the value-based criteria in this query.
     *
     * @return immutable criteria in their declaration order
     */
    public List<Criterion> criteria() {
        return criteria;
    }

    private WorkflowStateQuery append(Criterion criterion) {
        var appendedCriteria = new ArrayList<>(criteria);
        appendedCriteria.add(criterion);
        return new WorkflowStateQuery(appendedCriteria);
    }

    private static <T> T requireNonNull(@Nullable T value, String name) {
        return Objects.requireNonNull(value, name + " must not be null");
    }

    private static void requireNonEmpty(String value, String name) {
        assertNonEmpty(value, name + " must not be null or empty");
    }

    /**
     * A value-based condition used to select workflow instances.
     */
    public sealed interface Criterion permits WorkflowIdCriterion, WorkflowDefinitionIdCriterion,
            WorkflowStatusCriterion, StepCriterion, StepStatusCriterion, PayloadValueCriterion, VersionCriterion,
            VersionMigrationCriterion {
    }

    /**
     * Selects a workflow by its identifier.
     *
     * @param workflowId workflow identifier to match
     */
    public record WorkflowIdCriterion(String workflowId) implements Criterion {

        public WorkflowIdCriterion {
            requireNonEmpty(workflowId, "workflowId");
        }
    }

    /**
     * Selects workflows by their definition identity.
     *
     * @param workflowDefinitionId workflow definition identity to match
     */
    public record WorkflowDefinitionIdCriterion(VersionedType workflowDefinitionId) implements Criterion {

        public WorkflowDefinitionIdCriterion {
            requireNonNull(workflowDefinitionId, "workflowDefinitionId");
        }
    }

    /**
     * Selects workflows by their status.
     *
     * @param workflowStatus workflow status to match
     */
    public record WorkflowStatusCriterion(WorkflowStatus workflowStatus) implements Criterion {

        public WorkflowStatusCriterion {
            requireNonNull(workflowStatus, "workflowStatus");
        }
    }

    /**
     * Selects workflows containing a named step.
     *
     * @param stepName step name that must exist
     */
    public record StepCriterion(String stepName) implements Criterion {

        public StepCriterion {
            requireNonEmpty(stepName, "stepName");
        }
    }

    /**
     * Selects workflows by a named step's status.
     *
     * @param stepName step name to inspect
     * @param stepStatus step status to match
     */
    public record StepStatusCriterion(String stepName, StepStatus stepStatus) implements Criterion {

        public StepStatusCriterion {
            requireNonEmpty(stepName, "stepName");
            requireNonNull(stepStatus, "stepStatus");
        }
    }

    /**
     * Selects workflows by an exact payload entry value.
     *
     * @param key payload entry name to inspect
     * @param value payload value to match, which may be {@code null}
     */
    public record PayloadValueCriterion(String key, @Nullable Object value) implements Criterion {

        public PayloadValueCriterion {
            requireNonEmpty(key, "key");
        }
    }

    /**
     * Selects workflows by the effective version of a change identifier.
     *
     * @param changeId change identifier to inspect
     * @param version effective version to match
     */
    public record VersionCriterion(String changeId, String version) implements Criterion {

        public VersionCriterion {
            requireNonEmpty(changeId, "changeId");
            requireNonEmpty(version, "version");
        }
    }

    /**
     * Selects workflows with a recorded version migration for a change identifier.
     *
     * @param changeId change identifier that must have a recorded migration
     */
    public record VersionMigrationCriterion(String changeId) implements Criterion {

        public VersionMigrationCriterion {
            requireNonEmpty(changeId, "changeId");
        }
    }

}
