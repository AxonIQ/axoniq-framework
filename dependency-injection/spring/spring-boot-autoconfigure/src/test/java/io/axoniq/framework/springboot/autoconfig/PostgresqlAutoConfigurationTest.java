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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.postgresql.PostgresqlEventStorageEngine;
import io.axoniq.framework.postgresql.SchemaInitialization;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link PostgresqlAutoConfiguration}.
 * <p>
 * The Spring layer is a thin properties-to-component translation: the schema-initialization assertions therefore
 * run at the framework-component level, similarly to {@link DistributedTracingAutoConfigurationTest} -- the enhancer
 * bean participates in a {@link MessagingConfigurer} build, and the resulting {@link AxonConfiguration} is inspected
 * for the {@link SchemaInitialization} component read by the {@code PostgresqlConfigurationEnhancer}.
 *
 * @author Steven van Beelen
 * @author John Hendrikx
 */
class PostgresqlAutoConfigurationTest {

    private final ApplicationContextRunner testContext = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PostgresqlAutoConfiguration.class));

    private static AxonConfiguration configurationFrom(ApplicationContext context) {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> context.getBeansOfType(ConfigurationEnhancer.class)
                                                                        .values()
                                                                        .forEach(e -> e.enhance(registry)))
                                  .build();
    }

    @Test
    void disablerCreatedWhenPropertyIsFalse() {
        testContext.withPropertyValues("axon.postgresql.enabled=false")
                   .run(context -> assertThat(context).hasBean("disablePostgresqlConfigurationEnhancer"));
    }

    @Test
    void disablerNotCreatedByDefault() {
        testContext.run(context -> assertThat(context).doesNotHaveBean("disablePostgresqlConfigurationEnhancer"));
    }

    @Test
    void disablerNotCreatedWhenPropertyIsTrue() {
        testContext.withPropertyValues("axon.postgresql.enabled=true")
                   .run(context -> assertThat(context).doesNotHaveBean("disablePostgresqlConfigurationEnhancer"));
    }

    @Test
    void backsOffWhenPostgresqlEventStorageEngineClassIsAbsent() {
        testContext.withClassLoader(new FilteredClassLoader(PostgresqlEventStorageEngine.class))
                   .withPropertyValues("axon.postgresql.enabled=false")
                   .run(context -> assertThat(context).doesNotHaveBean(ConfigurationEnhancer.class));
    }

    @Test
    void schemaInitializationDefaultsToCreateIfMissing() {
        testContext.run(context -> {
            AxonConfiguration configuration = configurationFrom(context);

            assertThat(configuration.getComponent(SchemaInitialization.class))
                    .isEqualTo(SchemaInitialization.CREATE_IF_MISSING);
        });
    }

    @Test
    void schemaInitializationPropertyReachesTheComponent() {
        testContext.withPropertyValues("axon.postgresql.schema-initialization=validate")
                   .run(context -> {
                       AxonConfiguration configuration = configurationFrom(context);

                       assertThat(configuration.getComponent(SchemaInitialization.class))
                               .isEqualTo(SchemaInitialization.VALIDATE);
                   });
    }

    @Test
    void schemaInitializationEnhancerNotCreatedWhenExtensionIsDisabled() {
        testContext.withPropertyValues("axon.postgresql.enabled=false")
                   .run(context -> assertThat(context)
                           .doesNotHaveBean("postgresqlSchemaInitializationConfigurationEnhancer"));
    }

    @Test
    void userRegisteredSchemaInitializationWinsOverThePropertyTranslation() {
        // given a user-registered SchemaInitialization component and a conflicting property
        testContext.withPropertyValues("axon.postgresql.schema-initialization=validate")
                   .run(context -> {
                       AxonConfiguration configuration = MessagingConfigurer.create()
                               .componentRegistry(registry -> {
                                   registry.registerComponent(SchemaInitialization.class,
                                                              c -> SchemaInitialization.SKIP);
                                   context.getBeansOfType(ConfigurationEnhancer.class).values()
                                          .forEach(e -> e.enhance(registry));
                               })
                               .build();

                       // when / then -- registerIfNotPresent leaves the user registration in place
                       assertThat(configuration.getComponent(SchemaInitialization.class))
                               .isEqualTo(SchemaInitialization.SKIP);
                   });
    }
}