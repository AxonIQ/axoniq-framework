/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.infra.ComponentDescriptor;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Verifies that a {@link WorkflowExecutionRepository} implemented against the interface as released in 5.4, which
 * implements only the methods abstract in that release, works with the methods the interface gained since.
 *
 * @author Allard Buijze
 */
class WorkflowExecutionRepositoryTest {

    private ReleasedContractRepository repository;

    @BeforeEach
    void setUp() {
        repository = new ReleasedContractRepository();
    }

    // The repository compares executions by identity only, so a bare stand-in serves as an execution.
    private static WorkflowExecution anExecution() {
        return mock(WorkflowExecution.class);
    }

    @Nested
    class RemovingAGivenExecution {

        @Test
        void removesTheExecutionStoredForTheIdentifier() {
            // given
            var stored = anExecution();
            var other = anExecution();
            repository.save("order-42", () -> stored);
            repository.save("order-43", () -> other);

            // when
            var removed = repository.remove("order-42", stored);

            // then
            assertThat(removed).isTrue();
            assertThat(repository.findById("order-42")).isEmpty();
            assertThat(repository.findById("order-43")).containsSame(other);
        }

        @Test
        void keepsTheStoredExecutionWhenGivenAnotherExecutionOfTheSameWorkflow() {
            // given
            var stored = anExecution();
            var replaced = anExecution();
            repository.save("order-42", () -> stored);

            // when
            var removed = repository.remove("order-42", replaced);

            // then
            assertThat(removed).isFalse();
            assertThat(repository.findById("order-42")).containsSame(stored);
        }

        @Test
        void removesNothingForAnUnknownIdentifier() {
            // given
            var stored = anExecution();
            repository.save("order-42", () -> stored);

            // when
            var removed = repository.remove("unknown", stored);

            // then
            assertThat(removed).isFalse();
            assertThat(repository.findById("order-42")).containsSame(stored);
        }
    }

    /**
     * Implements exactly the methods of {@link WorkflowExecutionRepository} that are abstract in its 5.4 release.
     */
    private static final class ReleasedContractRepository implements WorkflowExecutionRepository {

        private final Map<String, WorkflowExecution> executions = new ConcurrentHashMap<>();

        @Override
        public Optional<WorkflowExecution> findById(String workflowId) {
            return Optional.ofNullable(executions.get(workflowId));
        }

        @Override
        public CompletableFuture<Set<WorkflowExecution>> findAll(WorkflowStateQuery query) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("Not used by these tests"));
        }

        @Override
        public Set<WorkflowExecution> findAll(Predicate<WorkflowExecution> predicate) {
            return Set.copyOf(executions.values().stream().filter(predicate).toList());
        }

        @Override
        public WorkflowExecution save(String workflowId, Supplier<WorkflowExecution> factory) {
            return executions.computeIfAbsent(workflowId, id -> factory.get());
        }

        @Override
        public WorkflowExecution remove(String workflowId) {
            return executions.remove(workflowId);
        }

        @Override
        public void removeAll(Predicate<WorkflowExecution> predicate) {
            executions.values().removeIf(predicate);
        }

        @Override
        public void clear() {
            executions.clear();
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("size", executions.size());
        }
    }
}
