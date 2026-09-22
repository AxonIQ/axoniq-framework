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

package io.axoniq.framework.integrationtests.testsuite.infrastructure;

import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * {@link TestInfrastructure} implementation backed by Axon Framework's {@link org.axonframework.integrationtests.testsuite.infrastructure.InMemoryTestInfrastructure}.
 * <p>
 * Leaf test classes should hold a {@code private static final} instance of this class:
 * <pre>{@code
 * private static final TestInfrastructure INFRASTRUCTURE = new InMemoryTestInfrastructure();
 *
 * @Override
 * protected TestInfrastructure testInfrastructure() {
 *     return INFRASTRUCTURE;
 * }
 * }</pre>
 *
 * @author Jakob Hatzl
 * @since 5.3.0
 */
public final class InMemoryTestInfrastructure implements TestInfrastructure {

    private final org.axonframework.integrationtests.testsuite.infrastructure.InMemoryTestInfrastructure delegate = new org.axonframework.integrationtests.testsuite.infrastructure.InMemoryTestInfrastructure();
    private final List<Consumer<ComponentRegistry>> infrastructureConfigurators;

    /**
     * Creates a new {@code InMemoryTestInfrastructure} instance.
     *
     * @param infrastructureConfigurators to be executed when {@link #configureInfrastructure(ComponentRegistry)} is
     *                                    called
     */
    @SafeVarargs
    public InMemoryTestInfrastructure(Consumer<ComponentRegistry>... infrastructureConfigurators) {
        this(Arrays.asList(infrastructureConfigurators));
    }

    /**
     * Creates a new {@code InMemoryTestInfrastructure} instance.
     *
     * @param infrastructureConfigurators to be executed when {@link #configureInfrastructure(ComponentRegistry)} is
     *                                    called
     */
    public InMemoryTestInfrastructure(List<Consumer<ComponentRegistry>> infrastructureConfigurators) {
        this.infrastructureConfigurators = List.copyOf(infrastructureConfigurators);
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void configureInfrastructure(ComponentRegistry registry) {
        delegate.configureInfrastructure(registry);
        infrastructureConfigurators.forEach(configurator -> configurator.accept(registry));
    }

    @Override
    public void purgeData() {
        delegate.purgeData();
    }

    @Override
    public void stop() {
        delegate.stop();
    }
}
