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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.postgresql.PostgresqlEventStorageEngine;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link PostgresqlAutoConfiguration}.
 *
 * @author Steven van Beelen
 */
class PostgresqlAutoConfigurationTest {

    private final ApplicationContextRunner testContext = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PostgresqlAutoConfiguration.class));

    @Test
    void disablerCreatedWhenPropertyIsFalse() {
        testContext.withPropertyValues("axon.postgresql.enabled=false")
                   .run(context -> assertThat(context).hasSingleBean(ConfigurationEnhancer.class));
    }

    @Test
    void disablerNotCreatedByDefault() {
        testContext.run(context -> assertThat(context).doesNotHaveBean(ConfigurationEnhancer.class));
    }

    @Test
    void disablerNotCreatedWhenPropertyIsTrue() {
        testContext.withPropertyValues("axon.postgresql.enabled=true")
                   .run(context -> assertThat(context).doesNotHaveBean(ConfigurationEnhancer.class));
    }

    @Test
    void backsOffWhenPostgresqlEventStorageEngineClassIsAbsent() {
        testContext.withClassLoader(new FilteredClassLoader(PostgresqlEventStorageEngine.class))
                   .withPropertyValues("axon.postgresql.enabled=false")
                   .run(context -> assertThat(context).doesNotHaveBean(ConfigurationEnhancer.class));
    }
}