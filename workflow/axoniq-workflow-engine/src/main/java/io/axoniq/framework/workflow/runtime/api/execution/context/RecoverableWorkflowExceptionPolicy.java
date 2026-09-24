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
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.StepInterruptedException;
import org.axonframework.common.AxonTransientException;
import org.axonframework.common.ExceptionUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Classifies an exception that escaped a workflow body as recoverable or not.
 * <p>
 * A recoverable exception describes a condition a later run can succeed on: the engine pauses the workflow, keeps
 * the instance non-terminal and re-drives it on the next start of the processing node or claim of its segment. Any
 * other exception is treated as a defect in the body and fails the workflow durably, so the failure is visible and
 * final.
 * <p>
 * {@link #DEFAULT} walks the cause chain and treats these as recoverable:
 * <ul>
 *     <li>any {@link Error}, such as an {@link OutOfMemoryError} or {@link StackOverflowError}</li>
 *     <li>an {@link InterruptedException} or {@link StepInterruptedException}, raised when the engine stops</li>
 *     <li>a {@link RejectedExecutionException}, raised when an executor is shutting down</li>
 *     <li>an {@link AxonTransientException}, the framework's marker for a condition worth retrying</li>
 *     <li>a {@link TimeoutException} or {@link IOException}, raised when infrastructure did not answer</li>
 * </ul>
 * The policy applies to all workflows when registered as a component, and to a single workflow when set through
 * {@link io.axoniq.framework.workflow.configuration.WorkflowCustomization#recoverableExceptionPolicy(RecoverableWorkflowExceptionPolicy)}.
 * Without either, {@link #DEFAULT} applies.
 * <p>
 * Example usage:
 * <pre>{@code
 * configurer.componentRegistry(registry -> registry.registerComponent(
 *         RecoverableWorkflowExceptionPolicy.class,
 *         config -> RecoverableWorkflowExceptionPolicy.DEFAULT.or(e -> e instanceof BackendUnavailableException)
 * ));
 * }</pre>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface RecoverableWorkflowExceptionPolicy {

    /**
     * The default classification described in the interface documentation.
     */
    RecoverableWorkflowExceptionPolicy DEFAULT = failure -> ExceptionUtils.findException(
            failure,
            cause -> cause instanceof Error
                    || cause instanceof InterruptedException
                    || cause instanceof StepInterruptedException
                    || cause instanceof RejectedExecutionException
                    || cause instanceof AxonTransientException
                    || cause instanceof TimeoutException
                    || cause instanceof IOException
    ).isPresent();

    /**
     * Decides whether the given exception, which escaped the workflow body, is recoverable.
     *
     * @param failure the exception that escaped the workflow body
     * @return {@code true} to pause the workflow for a later run, {@code false} to fail it
     */
    boolean isRecoverable(Throwable failure);

    /**
     * Combines this policy with another one, treating an exception as recoverable when either policy does.
     *
     * @param other the policy to combine with
     * @return a policy that accepts what this policy or the other policy accepts
     */
    default RecoverableWorkflowExceptionPolicy or(RecoverableWorkflowExceptionPolicy other) {
        Objects.requireNonNull(other, "The other policy must not be null.");
        return failure -> isRecoverable(failure) || other.isRecoverable(failure);
    }
}
