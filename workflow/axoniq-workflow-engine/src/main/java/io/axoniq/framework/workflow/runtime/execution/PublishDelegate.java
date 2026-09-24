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

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.context.PublishPrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.util.Objects;

import static io.axoniq.framework.workflow.runtime.util.MetadataUtils.METADATA_KEY_STEP_PRIMITIVE;
import static io.axoniq.framework.workflow.runtime.util.MetadataUtils.STEP_PRIMITIVE_PUBLISH;

/**
 * Implements the {@link PublishPrimitive}.
 * <p>
 * The delegate publishes the user's event enriched with workflow metadata directly through
 * {@link WorkflowExecution#appendWorkflowEvent}, bypassing the step guards of {@link AbstractStepExecutor}: there is no
 * started/completed pair, the single event is the completed step. The replay-skip gate
 * ({@link WorkflowState#containsStep}) and the drift guard ({@link ReachedSteps#assertNoReplayDrift}) mirror
 * {@link PayloadDelegate}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public class PublishDelegate implements PublishPrimitive {

    private final WorkflowExecution workflowExecution;
    private final ReachedSteps reachedSteps;

    /**
     * Constructs the delegate.
     *
     * @param workflowExecution workflow execution
     * @param reachedSteps      reached steps tracker
     */
    public PublishDelegate(WorkflowExecution workflowExecution, ReachedSteps reachedSteps) {
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow execution is mandatory");
        this.reachedSteps = Objects.requireNonNull(reachedSteps, "Reached steps tracker is mandatory");
    }

    @Override
    public WorkflowStepResult publish(PublishCommand command) {
        var stepName = command.stepName();
        reachedSteps.record(stepName);
        // Replay-skip gate + drift guard: the published event is the COMPLETED step, so on a replay or a post-crash
        // re-run the step is already present and nothing is published again.
        if (!workflowExecution.state().containsStep(stepName)) {
            reachedSteps.assertNoReplayDrift(workflowExecution.workflowId(), workflowExecution.state(), stepName);
            var processingContext = workflowExecution.processingContext();
            var userEvent = command.event();
            // Same message, workflow metadata added (workflow keys win), converter of the context attached like on
            // every other engine event so the engine can read the payload back when it handles the event itself.
            var event = new GenericEventMessage(userEvent.identifier(),
                                                userEvent.type(),
                                                userEvent.payload(),
                                                userEvent.metadata()
                                                         .mergedWith(MetadataUtils.create(workflowExecution.workflowId(),
                                                                                          stepName,
                                                                                          StepStatus.COMPLETED))
                                                         .and(METADATA_KEY_STEP_PRIMITIVE, STEP_PRIMITIVE_PUBLISH),
                                                userEvent.timestamp())
                    .withConverter(processingContext.component(EventConverter.class));
            workflowExecution.appendTask(e -> FutureResolver.resolve(
                    processingContext,
                    workflowExecution.appendWorkflowEvent(event, processingContext)
            ));
        }
        try {
            workflowExecution.awaitStateChange(WorkflowStateUtils.stepStatus(stepName, StepStatus.COMPLETED));
            return WorkflowStepResults.completed(stepName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return WorkflowStepResults.canceled(stepName);
        }
    }
}
