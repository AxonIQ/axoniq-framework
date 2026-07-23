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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Default {@link WorkflowManager} backed by a {@link WorkflowExecutionRepository}.
 * <p>
 * Selections are resolved against the live executions held by the repository: {@link #workflow(String)} re-resolves the
 * targeted instance on each access (so a handle reflects a present, unknown, or since-terminated instance), and
 * {@link #workflows(Predicate)} materializes a point-in-time snapshot of matching non-terminal instances. Every command
 * is enqueued onto the instance's own control thread via {@link WorkflowExecution}, so calls are asynchronous and do not
 * block on the instances reaching their terminal state.
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
    public WorkflowHandle workflow(@Nonnull String workflowId) {
        Objects.requireNonNull(workflowId, "Workflow id must not be null");
        return new DefaultWorkflowHandle(workflowId,
                                         () -> workflowExecutionRepository.findById(workflowId).orElse(null));
    }

    @Nonnull
    @Override
    public WorkflowSelection workflows(@Nonnull Predicate<WorkflowState> selector) {
        Objects.requireNonNull(selector, "Selector must not be null");
        List<WorkflowHandle> handles = workflowExecutionRepository
                .findAll()
                .stream()
                .filter(execution -> !execution.state().workflowStatus().isTerminal())
                .filter(execution -> selector.test(execution.state()))
                .<WorkflowHandle>map(execution -> new DefaultWorkflowHandle(execution.workflowId(), () -> execution))
                .toList();
        return new DefaultWorkflowSelection(handles);
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

    /**
     * Safe facade over a single {@link WorkflowExecution}, resolved lazily through a supplier so it can back either a
     * re-resolving single-id handle or a fixed snapshot handle. Never exposes the execution or its task queue.
     */
    private static final class DefaultWorkflowHandle implements WorkflowHandle {

        private final String workflowId;
        private final Supplier<WorkflowExecution> resolver;

        private DefaultWorkflowHandle(@Nonnull String workflowId, @Nonnull Supplier<WorkflowExecution> resolver) {
            this.workflowId = workflowId;
            this.resolver = resolver;
        }

        @Nonnull
        @Override
        public String id() {
            return workflowId;
        }

        @Nonnull
        @Override
        public Optional<WorkflowState> state() {
            var execution = resolver.get();
            return execution == null ? Optional.empty() : Optional.of(execution.state());
        }

        @Override
        public boolean cancel(@Nonnull CancellationReason reason) {
            var execution = resolver.get();
            if (execution == null || execution.state().workflowStatus().isTerminal()) {
                return false;
            }
            execution.requestWorkflowCancellation(effectiveCause(reason));
            return true;
        }

        @Override
        public boolean cancelStep(@Nonnull String stepName, @Nonnull CancellationReason reason) {
            var execution = resolver.get();
            if (execution == null) {
                return false;
            }
            return execution.requestStepCancellation(stepName, effectiveStepCause(reason));
        }

        @Override
        public int cancelAllRunningSteps(@Nonnull CancellationReason reason) {
            var execution = resolver.get();
            if (execution == null) {
                return 0;
            }
            return execution.requestAllRunningStepsCancellation(effectiveStepCause(reason));
        }
    }

    /**
     * Point-in-time selection that applies a per-handle command to every handle and aggregates the outcome.
     */
    private record DefaultWorkflowSelection(@Nonnull List<WorkflowHandle> handles) implements WorkflowSelection {

        @Nonnull
        @Override
        public Iterator<WorkflowHandle> iterator() {
            return handles.iterator();
        }

        @Nonnull
        @Override
        public Stream<WorkflowHandle> stream() {
            return handles.stream();
        }

        @Nonnull
        @Override
        public CancellationResult cancel(@Nonnull CancellationReason reason) {
            return aggregate(handle -> handle.cancel(reason));
        }

        @Nonnull
        @Override
        public CancellationResult cancelStep(@Nonnull String stepName, @Nonnull CancellationReason reason) {
            return aggregate(handle -> handle.cancelStep(stepName, reason));
        }

        @Nonnull
        @Override
        public CancellationResult cancelAllRunningSteps(@Nonnull CancellationReason reason) {
            return aggregate(handle -> handle.cancelAllRunningSteps(reason) > 0);
        }

        private CancellationResult aggregate(@Nonnull Predicate<WorkflowHandle> action) {
            var affectedIds = new ArrayList<String>(handles.size());
            for (var handle : handles) {
                if (action.test(handle)) {
                    affectedIds.add(handle.id());
                }
            }
            return new CancellationResult(handles.size(), affectedIds.size(), List.copyOf(affectedIds));
        }
    }
}
