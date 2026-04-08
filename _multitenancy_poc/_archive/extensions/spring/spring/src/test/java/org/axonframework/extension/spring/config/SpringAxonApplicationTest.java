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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.extension.spring.config;

import org.axonframework.common.configuration.ApplicationConfigurerTestSuite;
import org.axonframework.common.configuration.AxonConfiguration;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Test suite implementation validating the {@link SpringAxonApplication}.
 * <p>
 * Overrides {@link #supportsOverriding()} to return {@code false}, since Spring does not allow bean overriding.
 * <p>
 * Overrides {@link #doesOwnLifecycleManagement()} to return {@code false}, since all lifecycle management is given to
 * Spring instead of done manually.
 *
 * @author Steven van Beelen
 */
class SpringAxonApplicationTest extends ApplicationConfigurerTestSuite<SpringAxonApplication> {

    private SpringComponentRegistry componentRegistry;

    @Override
    public SpringAxonApplication createConfigurer() {
        ConfigurableListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        SpringLifecycleRegistry lifecycleRegistry = new SpringLifecycleRegistry();
        lifecycleRegistry.setBeanFactory(beanFactory);
        componentRegistry = new SpringComponentRegistry(beanFactory, lifecycleRegistry);
        componentRegistry.postProcessBeanFactory(beanFactory);
        return new SpringAxonApplication(componentRegistry, lifecycleRegistry);
    }

    @Override
    protected void initialize(SpringAxonApplication testSubject) {
        componentRegistry.postProcessAfterInitialization(new Object(), "something");
    }

    @Override
    public boolean supportsOverriding() {
        return false;
    }

    @Override
    public boolean supportsComponentFactories() {
        return false;
    }

    @Override
    public boolean doesOwnLifecycleManagement() {
        return false;
    }

    @Test
    void getOptionalComponentShouldNeverThrowsExceptions() {
        AxonConfiguration config = testSubject.build();

        assertThatNoException().isThrownBy(
                () -> config.getOptionalComponent(TestComponent.class)
        );
    }
}