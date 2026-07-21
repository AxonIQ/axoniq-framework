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
package io.axoniq.workflow.runtime.api.management;

import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.function.Predicate;

/**
 * External, application-facing entry point for managing running workflow instances.
 * <p>
 * Unlike the in-body {@code ctx.cancel(...)} primitive, this API cancels workflows from outside their own control
 * thread — for example from an HTTP endpoint, an administrative tool, or a compensating process. Cancellation is
 * <b>cooperative</b>: matching, non-terminal instances are driven towards a durable {@code CANCELLED} terminal state
 * on their own control thread; the call itself does not block on completion. No new persistence is introduced — the
 * durable {@code <workflow>:CANCELLED} event is the record of intent.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface WorkflowManager {

    /**
     * Cancels every workflow instance matching the given query that is not already in a terminal state.
     *
     * @param query  selector describing which workflow instances to cancel.
     * @param reason the reason for the cancellation, carried onto the cancellation cause.
     * @return the outcome describing how many instances matched and how many were requested to cancel.
     */
    @Nonnull
    CancellationResult cancel(@Nonnull WorkflowQuery query, @Nonnull CancellationReason reason);

    /**
     * Cancels every non-terminal workflow instance whose {@link WorkflowState} matches the given selector. Convenience
     * for {@code cancel(new WorkflowQuery.ByPredicate(selector), reason)}.
     *
     * @param selector predicate evaluated against each instance's current {@link WorkflowState}.
     * @param reason   the reason for the cancellation, carried onto the cancellation cause.
     * @return the outcome describing how many instances matched and how many were requested to cancel.
     */
    @Nonnull
    CancellationResult cancel(@Nonnull Predicate<WorkflowState> selector, @Nonnull CancellationReason reason);

    /**
     * Cooperatively cancels a single running step of one workflow instance while the workflow itself stays alive.
     * <p>
     * This is the external twin of the in-body {@code ctx.cancelStep(...)}: the cancellation is enqueued onto the
     * instance's own control thread (never driven from the caller thread), where — if the step is still non-terminal —
     * the {@code <step>:CANCELLED} record is published and the step's future is torn down. The workflow body can catch
     * the resulting {@link io.axoniq.workflow.runtime.api.execution.state.StepCancellationException} and compensate.
     *
     * @param workflowId the identifier of the workflow instance owning the step.
     * @param stepName   the name of the step to cancel.
     * @param reason     the reason for the cancellation, carried onto the cancellation cause.
     * @return the outcome, whose {@link StepCancellationResult#cancelled()} is {@code true} when the step existed and
     * was non-terminal at request time (a cancellation was enqueued) and {@code false} otherwise.
     */
    @Nonnull
    StepCancellationResult cancelStep(@Nonnull String workflowId, @Nonnull String stepName,
                                      @Nonnull CancellationReason reason);

    /**
     * Cooperatively cancels every currently-running step of one workflow instance while the workflow itself stays
     * alive.
     * <p>
     * The cancellation is enqueued onto the instance's own control thread, where each still-running step records
     * {@code <step>:CANCELLED}. Cancellation is <b>cooperative</b>: each cancelled step raises a
     * {@link io.axoniq.workflow.runtime.api.execution.state.StepCancellationException} into the workflow body; if the
     * body does not catch it, the exception propagates and the workflow wedges non-terminal (the documented caller
     * responsibility, consistent with the engine's uncaught-exception behaviour).
     *
     * @param workflowId the identifier of the workflow instance whose running steps to cancel.
     * @param reason     the reason for the cancellation, carried onto the cancellation cause.
     * @return the outcome describing how many currently-running steps a cancellation was enqueued for.
     */
    @Nonnull
    RunningStepsCancellationResult cancelAllRunningSteps(@Nonnull String workflowId,
                                                         @Nonnull CancellationReason reason);

    /**
     * Selector describing which workflow instances a management operation targets.
     *
     * @author Stefan Dragisic
     * @since 1.0.0
     */
    sealed interface WorkflowQuery permits WorkflowQuery.ById, WorkflowQuery.ByPredicate {

        /**
         * Selects a single workflow instance by its identifier.
         *
         * @param workflowId the workflow identifier to target.
         * @author Stefan Dragisic
         * @since 1.0.0
         */
        record ById(@Nonnull String workflowId) implements WorkflowQuery {

        }

        /**
         * Selects all workflow instances whose current {@link WorkflowState} matches the predicate.
         *
         * @param predicate predicate evaluated against each instance's current state.
         * @author Stefan Dragisic
         * @since 1.0.0
         */
        record ByPredicate(@Nonnull Predicate<WorkflowState> predicate) implements WorkflowQuery {

        }
    }

    /**
     * Reason for a cancellation. Either the human-readable {@code reason} or an explicit {@code cause} may be provided;
     * both are optional.
     *
     * @param reason optional human-readable reason, or {@code null} if none.
     * @param cause  optional explicit cause, or {@code null} if none.
     * @author Stefan Dragisic
     * @since 1.0.0
     */
    record CancellationReason(@Nullable String reason, @Nullable Throwable cause) {

        /**
         * Creates a reason with neither a message nor a cause.
         *
         * @return an empty cancellation reason.
         */
        @Nonnull
        public static CancellationReason none() {
            return new CancellationReason(null, null);
        }

        /**
         * Creates a reason carrying the given human-readable message.
         *
         * @param reason the human-readable reason.
         * @return a cancellation reason carrying {@code reason}.
         */
        @Nonnull
        public static CancellationReason of(@Nonnull String reason) {
            return new CancellationReason(reason, null);
        }

        /**
         * Creates a reason carrying the given explicit cause.
         *
         * @param cause the cause of the cancellation.
         * @return a cancellation reason carrying {@code cause}.
         */
        @Nonnull
        public static CancellationReason of(@Nonnull Throwable cause) {
            return new CancellationReason(cause.getMessage(), cause);
        }
    }

    /**
     * Outcome of a cancellation request.
     *
     * @param matched     number of workflow instances that matched the query.
     * @param cancelled   number of matched instances that were non-terminal and therefore requested to cancel.
     * @param workflowIds identifiers of the instances that were requested to cancel.
     * @author Stefan Dragisic
     * @since 1.0.0
     */
    record CancellationResult(int matched, int cancelled, @Nonnull List<String> workflowIds) {

    }

    /**
     * Outcome of a single-step cancellation request.
     *
     * @param cancelled  {@code true} if the step was non-terminal at request time and a {@code <step>:CANCELLED}
     *                   record was enqueued; {@code false} if the step was unknown or already terminal.
     * @param workflowId identifier of the targeted workflow instance.
     * @param stepName   name of the targeted step.
     * @author Stefan Dragisic
     * @since 1.0.0
     */
    record StepCancellationResult(boolean cancelled, @Nonnull String workflowId, @Nonnull String stepName) {

    }

    /**
     * Outcome of a cancel-all-running-steps request.
     *
     * @param cancelled  number of currently-running steps for which a cancellation was enqueued.
     * @param workflowId identifier of the targeted workflow instance.
     * @author Stefan Dragisic
     * @since 1.0.0
     */
    record RunningStepsCancellationResult(int cancelled, @Nonnull String workflowId) {

    }
}
