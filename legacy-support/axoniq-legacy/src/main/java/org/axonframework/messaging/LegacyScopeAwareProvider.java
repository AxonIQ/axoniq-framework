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

package org.axonframework.messaging;

import org.axonframework.common.lifecycle.Phase;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * The {@link ScopeAwareProvider} of a configuration, providing the {@link ScopeAware} components that a fired deadline
 * is delivered to: the Saga managers, and the component translating an aggregate's deadline into a command.
 * <p>
 * The configuration creates this provider through the {@link LegacyScopeAwareProviderConfigurationEnhancer}, and the
 * components {@link #register(ScopeAware) register} themselves with it as the configuration builds them. Applications
 * do not create a provider. A deadline manager uses the configuration's provider instead: in Spring, by autowiring the
 * {@link ScopeAwareProvider} bean, and otherwise through the configuration:
 * <pre>{@code
 * JobRunrDeadlineManager.builder()
 *                       .scopeAwareProvider(configuration.getComponent(ScopeAwareProvider.class))
 *                       // ...
 *                       .build();
 * }</pre>
 * Axon Framework builds a Saga manager when the event processor handling its Saga starts. A persistent deadline manager
 * may fire an overdue deadline before that. Until the configuration has started its event processors, this provider
 * therefore waits before it provides any component, at most for the
 * {@link ScopeAwareProviderSettings#readinessTimeout() readiness timeout}. After that, it throws a
 * {@link ScopeAwareProviderNotReadyException}, upon which the deadline manager retries the deadline.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
public class LegacyScopeAwareProvider implements ScopeAwareProvider {

    private final List<ScopeAware> scopeAwareComponents = new CopyOnWriteArrayList<>();
    private final CompletableFuture<Void> ready = new CompletableFuture<>();
    private final Duration readinessTimeout;

    /**
     * Initializes a provider without any {@link ScopeAware} components, which waits for at most the given
     * {@code readinessTimeout} until it is {@link #markReady() ready}.
     * <p>
     * Package-private, as only the {@link LegacyScopeAwareProviderConfigurationEnhancer} creates a provider: a provider
     * outside the configuration would receive no components.
     *
     * @param readinessTimeout how long the provider waits for the configuration to start before it fails
     */
    LegacyScopeAwareProvider(Duration readinessTimeout) {
        this.readinessTimeout = Objects.requireNonNull(readinessTimeout, "The readiness timeout may not be null.");
    }

    /**
     * Registers the given {@code scopeAware} component, so that this provider provides it to fired deadlines.
     *
     * @param scopeAware the component to provide to fired deadlines
     */
    public void register(ScopeAware scopeAware) {
        scopeAwareComponents.add(Objects.requireNonNull(scopeAware, "The ScopeAware component may not be null."));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Provides every registered component once the configuration has started its event processors. Before that, it
     * waits at most for the readiness timeout.
     *
     * @throws ScopeAwareProviderNotReadyException if the configuration did not start within the readiness timeout, or
     *                                             shut down without starting
     */
    @Override
    public Stream<ScopeAware> provideScopeAwareStream(ScopeDescriptor scopeDescriptor) {
        awaitReadiness();
        return scopeAwareComponents.stream();
    }

    private void awaitReadiness() {
        try {
            // A copy, as orTimeout completes the future it is called on: a timed-out caller would fail the shared one.
            ready.copy().orTimeout(readinessTimeout.toMillis(), TimeUnit.MILLISECONDS).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                throw new ScopeAwareProviderNotReadyException(
                        "The configuration did not start within " + readinessTimeout
                                + ", so not every component a deadline is delivered to may be registered yet."
                );
            }
            throw new ScopeAwareProviderNotReadyException(
                    "The configuration shut down before it started, so no deadline can be delivered.", e.getCause()
            );
        }
    }

    /**
     * Marks this provider ready, as the configuration has started its event processors, and thereby built every Saga
     * manager. Called in a start phase after {@link Phase#INBOUND_EVENT_CONNECTORS}.
     */
    void markReady() {
        ready.complete(null);
    }

    /**
     * Releases callers waiting for this provider to become ready, as the configuration shuts down. Has no effect once
     * the provider is ready.
     */
    void release() {
        ready.completeExceptionally(new IllegalStateException(
                "The configuration shut down before the provider was ready."));
    }
}
