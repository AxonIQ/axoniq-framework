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
package io.axoniq.demo.bikerental;

import io.axoniq.workflow.configuration.WorkflowConfigurationDefaults;
import jakarta.annotation.Nonnull;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.GenericTokenTableFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.SAFE_POINT_TOKEN_STORE_JDBC_SCHEMA;

/**
 * Main class of the application.
 */
@SpringBootApplication
public class BikeRentalApplication {

    /**
     * Use JDBC Token store for safe point persistence.
     *
     * @param converter  converter for persistence.
     * @param dataSource data store.
     * @return JDBC Token store.
     */
    @Bean(WorkflowConfigurationDefaults.COMPONENT_SAFE_POINT_TOKEN_STORE)
    @Qualifier(WorkflowConfigurationDefaults.COMPONENT_SAFE_POINT_TOKEN_STORE)
    public JdbcTokenStore workflowEngineTokenStore(
            @Nonnull GeneralConverter converter,
            DataSource dataSource) {
        var store = new JdbcTokenStore(
                new JdbcTransactionalExecutorProvider(dataSource),
                converter,
                JdbcTokenStoreConfiguration.DEFAULT.schema(
                        SAFE_POINT_TOKEN_STORE_JDBC_SCHEMA
                )
        );
        store.createSchema(GenericTokenTableFactory.INSTANCE);
        return store;
    }

    /**
     * Use JPA Token store as a default token store for projections.
     *
     * @param converter            messenger converter.
     * @param entityManagerFactory entity manager factory.
     * @return JPA Token store.
     */
    @Bean
    @Primary
    public TokenStore tokenStore(
            @Nonnull GeneralConverter converter,
            @Nonnull EntityManagerFactory entityManagerFactory) {
        return new JpaTokenStore(
                new JpaTransactionalExecutorProvider(entityManagerFactory),
                converter,
                JpaTokenStoreConfiguration.DEFAULT
        );
    }

    /**
     * Main method of the application.
     *
     * @param args arguments to pass to main method.
     */
    public static void main(String[] args) {
        SpringApplication.run(BikeRentalApplication.class, args);
    }
}
