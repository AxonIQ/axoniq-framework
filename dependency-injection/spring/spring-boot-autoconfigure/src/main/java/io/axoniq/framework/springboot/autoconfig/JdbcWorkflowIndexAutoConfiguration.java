/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.springboot.WorkflowProperties;
import io.axoniq.framework.workflow.index.jdbc.JdbcWorkflowIndex;
import io.axoniq.framework.workflow.index.jdbc.JdbcWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.index.jdbc.WorkflowIndexSchema;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

/**
 * Spring Boot autoconfiguration that registers a {@link JdbcWorkflowIndex} on the application's {@code DataSource}.
 * <p>
 * It runs after the {@link JpaWorkflowIndexAutoConfiguration}, so the JPA index wins when both apply, unless the token
 * store is a {@link JdbcTokenStore}. Set {@code axon.workflow.index.enabled=false} to turn it off, or declare your own
 * {@link WorkflowIndex} bean to replace it. The tables are not created: create them with
 * {@link JdbcWorkflowIndex#createSchema}, or with the migration tool of the application.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@AutoConfiguration(
        afterName = {
                "org.axonframework.extension.springboot.autoconfig.JdbcAutoConfiguration",
                "org.axonframework.extension.springboot.autoconfig.ConverterAutoConfiguration",
                "io.axoniq.framework.springboot.autoconfig.JpaWorkflowIndexAutoConfiguration"
        }
)
@ConditionalOnClass({DataSource.class, WorkflowEngine.class})
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix = "axon.workflow", name = "enabled", matchIfMissing = true)
@ConditionalOnProperty(prefix = "axon.workflow.index", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(WorkflowProperties.class)
public class JdbcWorkflowIndexAutoConfiguration {

    /**
     * Creates the default {@link WorkflowIndexSchema}. Declare your own bean to rename tables or columns.
     *
     * @return the schema with the default table and column names
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkflowIndexSchema workflowIndexSchema() {
        return WorkflowIndexSchema.defaultSchema();
    }

    /**
     * Creates a {@link JdbcWorkflowIndex} on the given {@code dataSource}.
     *
     * @param dataSource         the {@link DataSource} the index writes to
     * @param converter          the {@link GeneralConverter} for the associations of an open wait
     * @param workflowProperties the {@link WorkflowProperties} holding the workflow index settings
     * @param tokenStore         the token store, used when {@code axon.workflow.index.stores-with-token} is not set
     * @param schema             the table and column names of the index
     * @return a {@link WorkflowIndex} backed by JDBC
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(GeneralConverter.class)
    public WorkflowIndex jdbcWorkflowIndex(DataSource dataSource,
                                           GeneralConverter converter,
                                           WorkflowProperties workflowProperties,
                                           ObjectProvider<TokenStore> tokenStore,
                                           WorkflowIndexSchema schema) {
        Boolean configured = workflowProperties.getIndex().getStoresWithToken();
        // Not set: only a JDBC token store shares the transaction the index writes in.
        boolean storesWithToken = configured != null
                ? configured
                : tokenStore.getIfAvailable() instanceof JdbcTokenStore;
        return new JdbcWorkflowIndex(
                new JdbcTransactionalExecutorProvider(dataSource),
                converter,
                JdbcWorkflowIndexConfiguration.DEFAULT.storesWithToken(storesWithToken).schema(schema)
        );
    }
}
