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
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * In-memory implementation of {@link MutableWorkflowRepository} backed by a {@link ConcurrentHashMap}.
 */
public class InMemoryWorkflowRepository implements MutableWorkflowRepository {

    private final ConcurrentHashMap<String, WorkflowHandle> workflowHandles = new ConcurrentHashMap<>();

    @Nonnull
    @Override
    public Optional<WorkflowHandle> findById(@Nonnull String workflowId) {
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        return Optional.ofNullable(workflowHandles.get(workflowId));
    }

    @Nonnull
    @Override
    public Collection<WorkflowHandle> findAll() {
        return Collections.unmodifiableCollection(workflowHandles.values());
    }

    @Nonnull
    @Override
    public WorkflowHandle storeIfAbsent(@Nonnull String workflowId,
                                         @Nonnull Function<String, WorkflowHandle> factory) {
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        Objects.requireNonNull(factory, "factory must not be null");
        return workflowHandles.computeIfAbsent(workflowId, factory);
    }

    @Nonnull
    @Override
    public Map<String, WorkflowHandle> findAllAsMap() {
        return Collections.unmodifiableMap(workflowHandles);
    }

    @Override
    public void clear() {
        workflowHandles.clear();
    }

    @Override
    public void describeTo(@NotNull ComponentDescriptor descriptor) {
        descriptor.describeProperty("size", workflowHandles.size());
        descriptor.describeProperty("workflowIds", workflowHandles.keySet().stream().toList());
    }
}
