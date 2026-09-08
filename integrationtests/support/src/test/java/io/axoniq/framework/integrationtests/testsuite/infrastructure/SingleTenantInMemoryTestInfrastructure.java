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
import org.axonframework.integrationtests.testsuite.infrastructure.InMemoryTestInfrastructure;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;

/**
 * {@link TestInfrastructure} implementation backed by Axon Framework's {@link InMemoryTestInfrastructure}, with
 * multi-tenancy switched off.
 * <p>
 * Tenants are Axon Server contexts, so multi-tenancy cannot function on an in-memory infrastructure at all: its tenant
 * provider resolves an Axon Server connection manager and fails when there is none. Leaf tests therefore use this class
 * rather than {@link InMemoryTestInfrastructure} directly.
 * <p>
 * Leaf test classes should hold a {@code private static final} instance of this class:
 * <pre>{@code
 * private static final TestInfrastructure INFRASTRUCTURE = new SingleTenantInMemoryTestInfrastructure();
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
public final class SingleTenantInMemoryTestInfrastructure implements TestInfrastructure {

    private final InMemoryTestInfrastructure delegate = new InMemoryTestInfrastructure();

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void configureInfrastructure(ComponentRegistry registry) {
        delegate.configureInfrastructure(registry);
        AxonIntegrationTestSupport.disableMultiTenancy.accept(registry);
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
