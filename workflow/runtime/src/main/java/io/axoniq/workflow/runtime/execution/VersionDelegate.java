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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.Version;
import io.axoniq.workflow.runtime.api.execution.context.VersionPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import io.axoniq.workflow.runtime.util.FutureResolver;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;

/**
 * Implements the {@link VersionPrimitive}.
 * <p>
 * The delegate is intentionally tiny: there is no user-supplied action to run, no async lifecycle to manage, and at
 * most a single event is published per invocation. Correctness hinges on the "downstream-steps guard" — see the
 * algorithm in {@link #version(VersionPrimitive.VersionCommand)} for details.
 *
 * @author Stefan Dragisic
 * @since 0.2.0
 */
@Internal
public class VersionDelegate implements VersionPrimitive {

    private static final Logger logger = LoggerFactory.getLogger(VersionDelegate.class);

    private final WorkflowContext workflowContext;
    private final WorkflowExecution workflowExecution;
    private final ReachedSteps reachedSteps;
    private final EventNameCustomizer parentEventNameCustomizer;
    private final Clock clock;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final EventSink eventSink;
    private final Executor executor;

    /**
     * Constructs the delegate.
     *
     * @param workflowContext            workflow context
     * @param workflowExecution          workflow execution
     * @param reachedSteps              reached steps tracker
     * @param parentEventNameCustomizer parent event name customizer
     * @param clock                     clock for time calculations
     * @param unitOfWorkFactory         unit of work factory
     * @param eventSink                 event sink
     * @param executor                  executor for event publication
     */
    public VersionDelegate(@Nonnull WorkflowContext workflowContext,
                           @Nonnull WorkflowExecution workflowExecution,
                           @Nonnull ReachedSteps reachedSteps,
                           @Nonnull EventNameCustomizer parentEventNameCustomizer,
                           @Nonnull Clock clock,
                           @Nonnull UnitOfWorkFactory unitOfWorkFactory,
                           @Nonnull EventSink eventSink,
                           @Nonnull Executor executor) {
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow execution is mandatory");
        this.reachedSteps = Objects.requireNonNull(reachedSteps, "Reached steps tracker is mandatory");
        this.parentEventNameCustomizer = Objects.requireNonNull(parentEventNameCustomizer,
                                                                "Parent event name customizer is mandatory");
        this.clock = Objects.requireNonNull(clock, "Clock is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UnitOfWorkFactory is mandatory");
        this.eventSink = Objects.requireNonNull(eventSink, "EventSink is mandatory");
        this.executor = Objects.requireNonNull(executor, "Executor is mandatory");
    }

    @Override
    @Nonnull
    public WorkflowStepResult version(@Nonnull VersionCommand command) {
        var stepName = command.stepName();
        var requestedRaw = command.newVersion();
        var state = workflowExecution.state();
        reachedSteps.record(stepName);

        // 1. Step already recorded for this stepName — return its value deterministically.
        if (state.hasVersionMigrationStep(stepName)) {
            return WorkflowStepResults.completed(stepName);
        }

        var currentRaw = state.workflowDefinitionId().version();
        var requested = Version.of(requestedRaw);
        var current = Version.of(currentRaw);

        // 2. Same as current — never emit, just return current. (Idempotent no-op.)
        if (requested.equals(current)) {
            return WorkflowStepResults.completed(stepName);
        }

        // 3. Downgrade attempt — reject. The workflow has already committed to a higher version.
        if (!requested.isGreaterThan(current)) {
            throw new IllegalArgumentException(
                    "ctx.migrateVersion(\"" + stepName + "\", \"" + requestedRaw + "\") rejected: requested "
                            + "version is not strictly greater than the workflow's current version \""
                            + currentRaw + "\". Versions must only move forward (semver-ordered).");
        }

        // 4. Downstream-steps guard: if state contains any terminal step that the current invocation has
        //    not yet referenced, the workflow has already executed past this point under old code, so we
        //    must stay on the legacy branch and emit nothing. See ADR 005 for the full rationale.
        if (reachedSteps.hasUnreferencedTerminalStep(workflowExecution.state())) {
            logger.debug("ctx.migrateVersion(\"{}\", \"{}\") staying on legacy branch (workflow stays at \"{}\", "
                                 + "no migration step emitted) — workflow has already executed past this point "
                                 + "under old code (untouched terminal steps in state).",
                         stepName, requestedRaw, currentRaw);
            return WorkflowStepResults.completed(stepName);
        }

        // 5. Live emit. Publish the migration step, then wait for the projection to apply it before
        //    returning so a subsequent ctx.migrateVersion() call later in the same invocation sees the
        //    recorded value.
        var eventNameCustomizer = merge(parentEventNameCustomizer, command.eventNameCustomizer());
        var event = EventMessageUtils.versionMigrationStep(workflowContext,
                                                           stepName,
                                                           requestedRaw,
                                                           eventNameCustomizer);

        workflowExecution.appendTask(e -> FutureResolver.resolve(
                workflowExecution.processingContext(),
                ProcessingContextUtils.executeWithResult(
                        workflowExecution.workflowId(),
                        unitOfWorkFactory,
                        executor,
                        workflowExecution.processingContext(),
                        ctx -> eventSink.publish(ctx, event)
                )
        ));

        try {
            workflowExecution.awaitStateChange(s -> s.hasVersionMigrationStep(stepName));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Return the current version. The migration task may still publish on the queue; the next
            // replay will read the recorded value from state and be consistent. The interrupted run's
            // branching is discarded — handleWorkflowException treats InterruptedException as a
            // non-terminal pause, so no terminal event is published either.
            return WorkflowStepResults.canceled(stepName);
        }
        return WorkflowStepResults.completed(stepName);
    }
}
