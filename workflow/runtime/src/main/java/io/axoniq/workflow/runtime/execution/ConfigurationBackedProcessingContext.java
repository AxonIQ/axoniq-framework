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

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Lightweight processing context backed by configuration components and mutable resources.
 * Used for restored workflow executions after startup rehydration, outside the bootstrap unit of work.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class ConfigurationBackedProcessingContext implements ProcessingContext {

    private final Configuration configuration;
    private final Map<Context.ResourceKey<?>, Object> resources = new ConcurrentHashMap<>();

    public ConfigurationBackedProcessingContext(@Nonnull Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "Configuration must not be null");
    }

    @Override
    public boolean isStarted() {
        return true;
    }

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public boolean isCommitted() {
        return false;
    }

    @Override
    public boolean isCompleted() {
        return false;
    }

    @Override
    public ProcessingLifecycle on(ProcessingLifecycle.Phase phase,
                                  Function<ProcessingContext, CompletableFuture<?>> action) {
        return this;
    }

    @Override
    public ProcessingLifecycle onError(ProcessingLifecycle.ErrorHandler errorHandler) {
        return this;
    }

    @Override
    public ProcessingLifecycle whenComplete(java.util.function.Consumer<ProcessingContext> consumer) {
        consumer.accept(this);
        return this;
    }

    @Override
    @Nonnull
    public <C> C component(@Nonnull Class<C> componentType, String name) {
        return name == null ? configuration.getComponent(componentType)
                            : configuration.getComponent(componentType, name);
    }

    @Override
    public boolean containsResource(Context.ResourceKey<?> resourceKey) {
        return resources.containsKey(resourceKey);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getResource(Context.ResourceKey<T> resourceKey) {
        return (T) resources.get(resourceKey);
    }

    @Override
    public Map<Context.ResourceKey<?>, Object> resources() {
        return resources;
    }

    @Override
    public <T> T putResource(Context.ResourceKey<T> resourceKey, T value) {
        @SuppressWarnings("unchecked")
        T previous = (T) resources.put(resourceKey, value);
        return previous;
    }

    @Override
    public <T> T updateResource(Context.ResourceKey<T> resourceKey, UnaryOperator<T> operator) {
        @SuppressWarnings("unchecked")
        T updated = (T) resources.compute(resourceKey, (k, existing) -> operator.apply((T) existing));
        return updated;
    }

    @Override
    public <T> T putResourceIfAbsent(Context.ResourceKey<T> resourceKey, T value) {
        @SuppressWarnings("unchecked")
        T previous = (T) resources.putIfAbsent(resourceKey, value);
        return previous;
    }

    @Override
    public <T> T computeResourceIfAbsent(Context.ResourceKey<T> resourceKey, java.util.function.Supplier<T> supplier) {
        @SuppressWarnings("unchecked")
        T value = (T) resources.computeIfAbsent(resourceKey, ignored -> supplier.get());
        return value;
    }

    @Override
    public <T> T removeResource(Context.ResourceKey<T> resourceKey) {
        @SuppressWarnings("unchecked")
        T previous = (T) resources.remove(resourceKey);
        return previous;
    }

    @Override
    public <T> boolean removeResource(Context.ResourceKey<T> resourceKey, T value) {
        return resources.remove(resourceKey, value);
    }
}
