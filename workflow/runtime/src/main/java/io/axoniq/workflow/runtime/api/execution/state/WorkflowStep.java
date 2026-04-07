/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.api.execution.state;

import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import org.axonframework.messaging.core.Context;

import java.time.Instant;

/**
 * Basic step of workflow execution.
 *
 * @param stepName  name of the step.
 * @param status    step status.
 * @param result    result of execution, may be null.
 * @param error     error of the execution, may be null.
 * @param timestamp timestamp of step start.
 * @param context   processing context.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public record WorkflowStep(
        String stepName,
        StepStatus status,
        Object result,
        Throwable error,
        Instant timestamp,
        Context context
) {

    public static WorkflowStep started(String name, Object parameters, Instant timestamp, Context context) {
        return new WorkflowStep(name, StepStatus.STARTED, parameters, null, timestamp, context);
    }

    public static WorkflowStep completed(String name, Object result, Instant timestamp, Context context) {
        return new WorkflowStep(name, StepStatus.COMPLETED, result, null, timestamp, context);
    }

    public static WorkflowStep failed(String name, Throwable error, Instant timestamp, Context context) {
        return new WorkflowStep(name, StepStatus.FAILED, null, error, timestamp, context);
    }

    public static WorkflowStep timedOut(String name, Object payload, Instant timestamp, Context context) {
        return new WorkflowStep(name, StepStatus.TIMED_OUT, payload, null, timestamp, context);
    }

    public static WorkflowStep cancelled(String name, Instant timestamp, Context context) {
        return new WorkflowStep(name, StepStatus.CANCELLED, null, null, timestamp, context);
    }

    public static WorkflowStep retrying(String name, StepRetryInfo retryInfo, Instant timestamp, Context context) {
        return new WorkflowStep(name, StepStatus.RETRYING, retryInfo, retryInfo.error(), timestamp, context);
    }
}
