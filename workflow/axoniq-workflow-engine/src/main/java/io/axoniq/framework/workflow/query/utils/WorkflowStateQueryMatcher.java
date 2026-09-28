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
package io.axoniq.framework.workflow.query.utils;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStep;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.VersionedType;

import java.util.Objects;

/**
 * Evaluates {@link WorkflowStateQuery} criteria against an in-memory workflow state.
 * <p>
 * Storage-specific repositories should translate the query to their native query language instead of materializing all
 * state and using this matcher.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public final class WorkflowStateQueryMatcher {

    private WorkflowStateQueryMatcher() {
    }

    /**
     * Determines whether a workflow state satisfies every criterion in a query.
     *
     * @param query criteria to evaluate
     * @param state workflow state to inspect
     * @return {@code true} when the state satisfies all criteria
     */
    public static boolean matches(WorkflowStateQuery query, WorkflowState state) {
        for (WorkflowStateQuery.Criterion criterion : query.criteria()) {
            switch (criterion) {
                case WorkflowStateQuery.WorkflowIdCriterion(String id) when !id.equals(state.workflowId()) -> {
                    return false;
                }
                case WorkflowStateQuery.WorkflowDefinitionIdCriterion(VersionedType workflowDefinitionId)
                        when !sameType(workflowDefinitionId, state.workflowDefinitionId()) -> {
                    return false;
                }
                case WorkflowStateQuery.WorkflowStatusCriterion(var workflowStatus)
                        when workflowStatus != state.workflowStatus() -> {
                    return false;
                }
                case WorkflowStateQuery.StepCriterion(String name) when !state.containsStep(name) -> {
                    return false;
                }
                case WorkflowStateQuery.StepStatusCriterion(String stepName, var status) -> {
                    WorkflowStep step = state.getStep(stepName);
                    if (step == null || step.status() != status) {
                        return false;
                    }
                }
                case WorkflowStateQuery.PayloadValueCriterion(String key, Object value) -> {
                    var payload = state.payload();
                    if (!payload.containsKey(key) || !Objects.equals(payload.get(key), value)) {
                        return false;
                    }
                }
                case WorkflowStateQuery.VersionCriterion(String changeId, String version)
                        when !version.equals(state.versionFor(changeId)) -> {
                    return false;
                }
                case WorkflowStateQuery.VersionMigrationCriterion(String changeId)
                        when !state.hasVersionMigrationStep(changeId) -> {
                    return false;
                }
                default -> {
                }
            }
        }
        return true;
    }

    private static boolean sameType(VersionedType first, VersionedType second) {
        return first.qualifiedName().equals(second.qualifiedName()) && first.version().equals(second.version());
    }
}
