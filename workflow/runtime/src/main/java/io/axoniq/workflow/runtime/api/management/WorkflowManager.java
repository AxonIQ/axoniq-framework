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
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * External, application-facing entry point for managing running workflow instances.
 * <p>
 * Unlike the in-body {@code ctx.cancel(...)} primitive, this API manages workflows from outside their own control
 * thread — for example from an HTTP endpoint, an administrative tool, or a compensating process. It follows a fluent
 * <b>selection-then-action</b> shape: first select what to act on, then invoke a command on the selection.
 * <ul>
 *     <li>{@link #workflow(String)} selects a single instance by id and returns a {@link WorkflowHandle}.</li>
 *     <li>{@link #workflows(Predicate)} selects a point-in-time snapshot of matching, non-terminal instances and
 *     returns a {@link WorkflowSelection} that is {@link Iterable} over their handles.</li>
 * </ul>
 * Cancellation is <b>cooperative</b> and <b>asynchronous</b>: matching, non-terminal instances are driven towards a
 * durable {@code CANCELLED} terminal state on their own control thread; the call itself does not block on completion.
 * No new persistence is introduced — the durable {@code <workflow>:CANCELLED} / {@code <step>:CANCELLED} events are the
 * record of intent.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
public interface WorkflowManager {

    /**
     * Selects the workflow instance with the given id, resolving it against the repository now, and returns a handle
     * for acting on it. A handle is returned even when the id is unknown; in that case its command methods report no
     * effect ({@code false} / {@code 0}) and {@link WorkflowHandle#state()} throws.
     *
     * @param workflowId the identifier of the workflow instance to select.
     * @return a handle for the selected instance, never {@code null}.
     */
    @Nonnull
    WorkflowHandle workflow(@Nonnull String workflowId);

    /**
     * Selects a point-in-time snapshot of every non-terminal workflow instance whose current {@link WorkflowState}
     * matches the given selector, and returns it as an iterable selection of handles.
     * <p>
     * The snapshot is materialized when this method is called: handles added or removed afterwards are not reflected.
     * Because commands are cooperative, an instance that terminates between selection and action is tolerated (its
     * command simply reports no effect).
     *
     * @param selector predicate evaluated against each instance's current {@link WorkflowState}.
     * @return a selection of handles for the matching instances, never {@code null}.
     */
    @Nonnull
    WorkflowSelection workflows(@Nonnull Predicate<WorkflowState> selector);

    /**
     * Safe facade for acting on a single workflow instance from outside its control thread.
     * <p>
     * A handle exposes only the instance id, a read-only {@link #state()} snapshot, and the cooperative command
     * methods. It never exposes the live execution, its task queue, or any way to mutate engine internals: every
     * command is enqueued onto the workflow's own control thread and returns without blocking on the terminal state.
     *
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    interface WorkflowHandle {

        /**
         * Returns the identifier of the workflow instance this handle targets.
         *
         * @return the workflow identifier.
         */
        @Nonnull
        String id();

        /**
         * Returns a read-only snapshot of the instance's current {@link WorkflowState}, or empty if no workflow
         * instance exists for this handle's id (it is unknown, or has terminated and been evicted from the engine).
         *
         * @return the current workflow state, or empty if the instance does not exist.
         */
        @Nonnull
        Optional<WorkflowState> state();

        /**
         * Cooperatively cancels this workflow instance, driving it towards a durable {@code CANCELLED} terminal state on
         * its own control thread. Does not block on completion.
         *
         * @param reason the reason for the cancellation, carried onto the cancellation cause.
         * @return {@code true} if the instance existed and was non-terminal (a cancellation was enqueued); {@code false}
         * if it is unknown or already terminal.
         */
        boolean cancel(@Nonnull CancellationReason reason);

        /**
         * Cooperatively cancels a single running step of this workflow instance while the workflow itself stays alive.
         * The cancellation is enqueued onto the instance's control thread where, if the step is still non-terminal, the
         * {@code <step>:CANCELLED} record is published; the workflow body can catch the resulting
         * {@link io.axoniq.workflow.runtime.api.execution.state.StepCancellationException} and compensate.
         *
         * @param stepName the name of the step to cancel.
         * @param reason   the reason for the cancellation, carried onto the cancellation cause.
         * @return {@code true} if the step existed and was non-terminal at request time (a cancellation was enqueued);
         * {@code false} if the instance is unknown or the step is unknown or already terminal.
         */
        boolean cancelStep(@Nonnull String stepName, @Nonnull CancellationReason reason);

        /**
         * Cooperatively cancels every currently-running step of this workflow instance while the workflow itself stays
         * alive. Each still-running step records {@code <step>:CANCELLED}; each cancelled step raises a
         * {@link io.axoniq.workflow.runtime.api.execution.state.StepCancellationException} into the workflow body, which
         * the body is responsible for catching (an uncaught exception wedges the instance non-terminally).
         *
         * @param reason the reason for the cancellation, carried onto the cancellation cause.
         * @return the number of currently-running steps for which a cancellation was enqueued ({@code 0} if the instance
         * is unknown or has no running steps).
         */
        int cancelAllRunningSteps(@Nonnull CancellationReason reason);
    }

    /**
     * An iterable, point-in-time selection of {@link WorkflowHandle handles} produced by {@link #workflows(Predicate)}.
     * <p>
     * Bulk commands apply the corresponding {@link WorkflowHandle} action to every handle in the selection and
     * aggregate the outcome into a {@link CancellationResult}. Iterating (via {@link Iterable} or {@link #stream()})
     * yields the handles so callers can inspect state and act per instance.
     *
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    interface WorkflowSelection extends Iterable<WorkflowHandle> {

        /**
         * Returns the handles in this selection as a stream.
         *
         * @return a stream over the selected handles.
         */
        @Nonnull
        Stream<WorkflowHandle> stream();

        /**
         * Cooperatively cancels every workflow instance in this selection.
         *
         * @param reason the reason for the cancellation, carried onto the cancellation cause.
         * @return the aggregate outcome (matched handles, instances a cancellation was enqueued for, and their ids).
         */
        @Nonnull
        CancellationResult cancel(@Nonnull CancellationReason reason);

        /**
         * Cooperatively cancels the step with the given name in every workflow instance in this selection.
         *
         * @param stepName the name of the step to cancel in each matched instance.
         * @param reason   the reason for the cancellation, carried onto the cancellation cause.
         * @return the aggregate outcome (matched handles, instances where the step was non-terminal, and their ids).
         */
        @Nonnull
        CancellationResult cancelStep(@Nonnull String stepName, @Nonnull CancellationReason reason);

        /**
         * Cooperatively cancels every currently-running step of every workflow instance in this selection.
         *
         * @param reason the reason for the cancellation, carried onto the cancellation cause.
         * @return the aggregate outcome (matched handles, instances that had at least one running step cancelled, and
         * their ids).
         */
        @Nonnull
        CancellationResult cancelAllRunningSteps(@Nonnull CancellationReason reason);
    }

    /**
     * Reason for a cancellation. Either the human-readable {@code reason} or an explicit {@code cause} may be provided;
     * both are optional.
     *
     * @param reason optional human-readable reason, or {@code null} if none.
     * @param cause  optional explicit cause, or {@code null} if none.
     * @author Stefan Dragisic
     * @since 0.3.0
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
     * Aggregate outcome of a bulk {@link WorkflowSelection} command.
     *
     * @param matched     number of handles the command was applied to (the size of the selection).
     * @param affected    number of matched instances for which a cancellation was actually enqueued (they were
     *                    non-terminal, or had at least one running step).
     * @param workflowIds identifiers of the affected instances.
     * @author Stefan Dragisic
     * @since 0.3.0
     */
    record CancellationResult(int matched, int affected, @Nonnull List<String> workflowIds) {

    }
}
