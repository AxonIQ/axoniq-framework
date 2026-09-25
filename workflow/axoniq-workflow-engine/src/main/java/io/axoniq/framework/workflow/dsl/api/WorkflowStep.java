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
package io.axoniq.framework.workflow.dsl.api;

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
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 5.4.0
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
        Throwable error = retryInfo.error() != null ? retryInfo.error().toThrowable() : null;
        return new WorkflowStep(name, StepStatus.RETRYING, retryInfo, error, timestamp, context);
    }

    /**
     * A retry attempt that the store accepted and that is now running. {@code retryInfo.attempt()} is the attempt being
     * started; the error is the one that triggered this retry.
     *
     * @param name      name of the step
     * @param retryInfo retry state of the attempt being started
     * @param timestamp timestamp of the attempt start
     * @param context   processing context
     * @return the step in {@link StepStatus#RETRY_STARTED} status
     */
    public static WorkflowStep retryStarted(String name, StepRetryInfo retryInfo, Instant timestamp, Context context) {
        Throwable error = retryInfo.error() != null ? retryInfo.error().toThrowable() : null;
        return new WorkflowStep(name, StepStatus.RETRY_STARTED, retryInfo, error, timestamp, context);
    }
}
