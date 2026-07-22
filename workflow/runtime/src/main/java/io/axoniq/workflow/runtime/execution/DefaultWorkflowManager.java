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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.management.WorkflowManager;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Default {@link WorkflowManager} backed by a {@link WorkflowExecutionRepository}.
 * <p>
 * Cancellation is resolved against the live executions held by the repository. Already-terminal instances are skipped;
 * each remaining match is asked to cancel via {@link WorkflowExecution#requestWorkflowCancellation(Throwable)}, which
 * enqueues the work onto the instance's own control thread. The call is therefore asynchronous and does not block on
 * the instances reaching their terminal state.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
@Internal
public class DefaultWorkflowManager implements WorkflowManager {

    private final WorkflowExecutionRepository workflowExecutionRepository;

    /**
     * Creates a manager backed by the given repository.
     *
     * @param workflowExecutionRepository repository holding the live workflow executions.
     */
    public DefaultWorkflowManager(@Nonnull WorkflowExecutionRepository workflowExecutionRepository) {
        this.workflowExecutionRepository = Objects.requireNonNull(workflowExecutionRepository,
                                                                  "Workflow execution repository must not be null");
    }

    @Nonnull
    @Override
    public CancellationResult cancel(@Nonnull WorkflowQuery query, @Nonnull CancellationReason reason) {
        var matches = resolve(query);
        var cause = effectiveCause(reason);
        var cancelledIds = new ArrayList<String>(matches.size());
        for (var execution : matches) {
            if (execution.state().workflowStatus().isTerminal()) {
                continue;
            }
            execution.requestWorkflowCancellation(cause);
            cancelledIds.add(execution.workflowId());
        }
        return new CancellationResult(matches.size(), cancelledIds.size(), List.copyOf(cancelledIds));
    }

    @Nonnull
    @Override
    public CancellationResult cancel(@Nonnull Predicate<WorkflowState> selector, @Nonnull CancellationReason reason) {
        return cancel(new WorkflowQuery.ByPredicate(selector), reason);
    }

    @Nonnull
    @Override
    public StepCancellationResult cancelStep(@Nonnull String workflowId, @Nonnull String stepName,
                                             @Nonnull CancellationReason reason) {
        var execution = workflowExecutionRepository.findById(workflowId).orElse(null);
        if (execution == null) {
            return new StepCancellationResult(false, workflowId, stepName);
        }
        boolean cancelled = execution.requestStepCancellation(stepName, effectiveStepCause(reason));
        return new StepCancellationResult(cancelled, workflowId, stepName);
    }

    @Nonnull
    @Override
    public RunningStepsCancellationResult cancelAllRunningSteps(@Nonnull String workflowId,
                                                                @Nonnull CancellationReason reason) {
        var execution = workflowExecutionRepository.findById(workflowId).orElse(null);
        if (execution == null) {
            return new RunningStepsCancellationResult(0, workflowId);
        }
        int cancelled = execution.requestAllRunningStepsCancellation(effectiveStepCause(reason));
        return new RunningStepsCancellationResult(cancelled, workflowId);
    }

    @Nonnull
    private List<WorkflowExecution> resolve(@Nonnull WorkflowQuery query) {
        return switch (query) {
            case WorkflowQuery.ById byId -> workflowExecutionRepository.findById(byId.workflowId())
                                                                       .map(List::of)
                                                                       .orElseGet(List::of);
            case WorkflowQuery.ByPredicate byPredicate -> workflowExecutionRepository
                    .findAll()
                    .stream()
                    .filter(execution -> byPredicate.predicate().test(execution.state()))
                    .toList();
        };
    }

    @Nullable
    private static Throwable effectiveCause(@Nonnull CancellationReason reason) {
        if (reason.cause() != null) {
            return reason.cause();
        }
        return reason.reason() != null ? new WorkflowCancelledException(reason.reason()) : null;
    }

    @Nullable
    private static Throwable effectiveStepCause(@Nonnull CancellationReason reason) {
        if (reason.cause() != null) {
            return reason.cause();
        }
        return reason.reason() != null ? new StepCancellationException(reason.reason()) : null;
    }
}
