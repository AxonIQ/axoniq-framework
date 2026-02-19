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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.repository;

import io.axoniq.workflow.runtime.api.WorkflowHandle;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

class InMemoryWorkflowRepositoryTest {

    private InMemoryWorkflowRepository repository;

    @BeforeEach
    void setUp() {
        repository = new InMemoryWorkflowRepository();
    }

    @Test
    void findByIdReturnsEmptyWhenNotFound() {
        assertThat(repository.findById("non-existent")).isEmpty();
    }

    @Test
    void findByIdReturnsStoredHandle() {
        var handle = createHandle();
        repository.storeIfAbsent("wf-1", id -> handle);

        assertThat(repository.findById("wf-1")).isPresent().containsSame(handle);
    }

    @Test
    void findByIdRejectsNullWorkflowId() {
        assertThatNullPointerException()
                .isThrownBy(() -> repository.findById(null))
                .withMessageContaining("workflowId");
    }

    @Test
    void storeIfAbsentCreatesNewHandle() {
        var handle = createHandle();
        var result = repository.storeIfAbsent("wf-1", id -> handle);

        assertThat(result).isSameAs(handle);
        assertThat(repository.findById("wf-1")).containsSame(handle);
    }

    @Test
    void storeIfAbsentReturnsExistingHandle() {
        var existing = createHandle();
        var replacement = createHandle();
        repository.storeIfAbsent("wf-1", id -> existing);

        var result = repository.storeIfAbsent("wf-1", id -> replacement);

        assertThat(result).isSameAs(existing);
    }

    @Test
    void storeIfAbsentDoesNotCallFactoryWhenExists() {
        var existing = createHandle();
        repository.storeIfAbsent("wf-1", id -> existing);

        var factoryCalled = new AtomicBoolean(false);
        repository.storeIfAbsent("wf-1", id -> {
            factoryCalled.set(true);
            return createHandle();
        });

        assertThat(factoryCalled).isFalse();
    }

    @Test
    void storeIfAbsentRejectsNullWorkflowId() {
        assertThatNullPointerException()
                .isThrownBy(() -> repository.storeIfAbsent(null, id -> createHandle()))
                .withMessageContaining("workflowId");
    }

    @Test
    void storeIfAbsentRejectsNullFactory() {
        assertThatNullPointerException()
                .isThrownBy(() -> repository.storeIfAbsent("wf-1", null))
                .withMessageContaining("factory");
    }

    @Test
    void findAllReturnsAllHandles() {
        var handle1 = createHandle();
        var handle2 = createHandle();
        repository.storeIfAbsent("wf-1", id -> handle1);
        repository.storeIfAbsent("wf-2", id -> handle2);

        var all = repository.findAll();

        assertThat(all).containsExactlyInAnyOrder(handle1, handle2);
    }

    @Test
    void findAllReturnsEmptyWhenNoHandles() {
        assertThat(repository.findAll()).isEmpty();
    }

    @Test
    void findAllReturnsUnmodifiableCollection() {
        repository.storeIfAbsent("wf-1", id -> createHandle());

        var all = repository.findAll();

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> all.add(createHandle()));
    }

    @Test
    void findAllAsMapReturnsAllHandles() {
        var handle1 = createHandle();
        var handle2 = createHandle();
        repository.storeIfAbsent("wf-1", id -> handle1);
        repository.storeIfAbsent("wf-2", id -> handle2);

        var map = repository.findAllAsMap();

        assertThat(map).hasSize(2);
        assertThat(map.get("wf-1")).isSameAs(handle1);
        assertThat(map.get("wf-2")).isSameAs(handle2);
    }

    @Test
    void findAllAsMapReturnsUnmodifiableMap() {
        repository.storeIfAbsent("wf-1", id -> createHandle());

        var map = repository.findAllAsMap();

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> map.put("wf-2", createHandle()));
    }

    @Test
    void clearRemovesAllHandles() {
        repository.storeIfAbsent("wf-1", id -> createHandle());
        repository.storeIfAbsent("wf-2", id -> createHandle());

        repository.clear();

        assertThat(repository.findAll()).isEmpty();
        assertThat(repository.findById("wf-1")).isEmpty();
        assertThat(repository.findById("wf-2")).isEmpty();
    }

    private static WorkflowHandle createHandle() {
        return new WorkflowHandle(mock(), mock(), mock());
    }
}
