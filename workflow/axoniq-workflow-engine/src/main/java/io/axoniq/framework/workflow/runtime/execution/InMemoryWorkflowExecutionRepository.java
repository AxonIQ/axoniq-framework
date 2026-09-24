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

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * In-memory implementation of {@link WorkflowExecutionRepository} backed by a {@link ConcurrentHashMap}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public class InMemoryWorkflowExecutionRepository implements WorkflowExecutionRepository {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryWorkflowExecutionRepository.class);
    private final ConcurrentHashMap<String, WorkflowExecution> workflowExecutions = new ConcurrentHashMap<>();

    /**
     * Creates a new instance of .
     */
    public InMemoryWorkflowExecutionRepository() {
        logger.info("Using in-memory workflow execution repository.");
    }

    @Override
    public Optional<WorkflowExecution> findById(String workflowId) {
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        return Optional.ofNullable(workflowExecutions.get(workflowId));
    }

    @Override
    public Set<WorkflowExecution> findAll(Predicate<WorkflowExecution> predicate) {
        return Set.copyOf(workflowExecutions.values().stream().filter(predicate).toList());
    }

    @Override
    public CompletableFuture<Set<WorkflowExecution>> findAll(WorkflowStateQuery query) {
        return CompletableFuture.completedFuture(
                Set.copyOf(workflowExecutions.values()
                                             .stream()
                                             .filter(execution -> WorkflowStateQueryMatcher.matches(
                                                     query, execution.state()
                                             ))
                                             .toList()));
    }

    @Override
    public WorkflowExecution save(String workflowId, Supplier<WorkflowExecution> factory) {
        Objects.requireNonNull(factory, "factory must not be null");
        return workflowExecutions.computeIfAbsent(workflowId, s -> factory.get());
    }

    @Override
    public WorkflowExecution remove(String workflowId) {
        return workflowExecutions.remove(workflowId);
    }

    @Override
    public void removeAll(Predicate<WorkflowExecution> predicate) {
        workflowExecutions.values().removeIf(predicate);
    }

    @Override
    public void clear() {
        workflowExecutions.clear();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("size", workflowExecutions.size());
        descriptor.describeProperty("workflowIds", workflowExecutions.keySet().stream().toList());
    }
}
