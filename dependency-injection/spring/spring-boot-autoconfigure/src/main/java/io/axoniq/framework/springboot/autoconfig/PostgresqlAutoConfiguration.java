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

import io.axoniq.framework.postgresql.PostgresqlConfigurationEnhancer;
import io.axoniq.framework.postgresql.PostgresqlEventStorageEngine;
import io.axoniq.framework.postgresql.SchemaInitialization;
import io.axoniq.framework.springboot.PostgresqlProperties;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration class for the Axon Framework PostgreSQL extension.
 * <p>
 * Note that the {@link PostgresqlConfigurationEnhancer} already sets this extension's defaults, like the
 * {@link PostgresqlEventStorageEngine}, through the service-loading mechanism. This autoconfiguration provides the
 * ability to disable the {@code PostgresqlConfigurationEnhancer} by setting the {@code axon.postgresql.enabled}
 * property to {@code false}, and to translate {@code axon.postgresql.schema-initialization} into the
 * {@link SchemaInitialization} the engine is constructed with.
 *
 * @author Steven van Beelen
 * @author John Hendrikx
 * @since 5.1.0
 */
@AutoConfiguration(
        afterName = "io.axoniq.framework.springboot.autoconfig.AxonServerAutoConfiguration",
        beforeName = "org.axonframework.extension.springboot.autoconfig.JpaEventStoreAutoConfiguration"
)
@ConditionalOnClass(PostgresqlEventStorageEngine.class)
@EnableConfigurationProperties(PostgresqlProperties.class)
public class PostgresqlAutoConfiguration {

    /**
     * Bean creation method for a {@link ConfigurationEnhancer} that disables the
     * {@link PostgresqlConfigurationEnhancer} when the {@code axon.postgresql.enabled} property is set to
     * {@code false}.
     *
     * @return a configuration enhancer that disables the PostgreSQL configuration enhancer
     */
    @Bean
    @ConditionalOnProperty(name = "axon.postgresql.enabled", havingValue = "false")
    public ConfigurationEnhancer disablePostgresqlConfigurationEnhancer() {
        return new ConfigurationEnhancer() {
            @Override
            public void enhance(ComponentRegistry registry) {
                registry.disableEnhancer(PostgresqlConfigurationEnhancer.class);
            }

            @Override
            public int order() {
                return Integer.MIN_VALUE;
            }
        };
    }

    /**
     * Bean creation method for a {@link ConfigurationEnhancer} translating the
     * {@code axon.postgresql.schema-initialization} property into the {@link SchemaInitialization} component read
     * by the {@link PostgresqlConfigurationEnhancer} when it constructs the
     * {@link PostgresqlEventStorageEngine}. Registration uses {@code registerIfNotPresent}, so a component
     * registered directly through the {@code ApplicationConfigurer} always wins over this property translation.
     *
     * @param properties the bound {@link PostgresqlProperties}
     * @return a configuration enhancer registering the configured {@link SchemaInitialization}
     */
    @Bean
    @ConditionalOnProperty(name = "axon.postgresql.enabled", matchIfMissing = true)
    public ConfigurationEnhancer postgresqlSchemaInitializationConfigurationEnhancer(PostgresqlProperties properties) {
        return registry -> registry.registerIfNotPresent(
                SchemaInitialization.class,
                c -> properties.getSchemaInitialization()
        );
    }
}