/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.springboot.WorkflowProperties;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.extension.springboot.util.RegisterDefaultEntities;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot autoconfiguration that registers a {@link JpaWorkflowIndex} on the application's
 * {@code EntityManagerFactory}, and adds its entities to the persistence unit.
 * <p>
 * Set {@code axon.workflow.index.enabled=false} to turn it off, or declare your own {@link WorkflowIndex} bean to
 * replace it. When the token store uses another database, set {@code axon.workflow.index.stores-with-token=false}.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@AutoConfiguration(
        afterName = {
                "org.axonframework.extension.springboot.autoconfig.JpaAutoConfiguration",
                "org.axonframework.extension.springboot.autoconfig.ConverterAutoConfiguration"
        }
)
@ConditionalOnClass({EntityManagerFactory.class, WorkflowEngine.class})
@ConditionalOnBean(EntityManagerFactory.class)
@ConditionalOnProperty(prefix = "axon.workflow", name = "enabled", matchIfMissing = true)
@ConditionalOnProperty(prefix = "axon.workflow.index", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(WorkflowProperties.class)
@RegisterDefaultEntities(packages = {
        "io.axoniq.framework.workflow.index.jpa",
})
public class JpaWorkflowIndexAutoConfiguration {

    /**
     * Creates a {@link JpaWorkflowIndex} on the given {@code entityManagerFactory}.
     *
     * @param entityManagerFactory the JPA {@link EntityManagerFactory} used for persistence
     * @param converter            the {@link GeneralConverter} for the associations of an open wait
     * @param workflowProperties   the {@link WorkflowProperties} holding the workflow index settings
     * @param tokenStore           the token store, used when {@code axon.workflow.index.stores-with-token} is not set
     * @return a {@link WorkflowIndex} backed by JPA
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(GeneralConverter.class)
    public WorkflowIndex jpaWorkflowIndex(EntityManagerFactory entityManagerFactory,
                                          GeneralConverter converter,
                                          WorkflowProperties workflowProperties,
                                          ObjectProvider<TokenStore> tokenStore) {
        Boolean configured = workflowProperties.getIndex().getStoresWithToken();
        // Not set: only a JPA token store shares the transaction the index writes in.
        boolean storesWithToken = configured != null
                ? configured
                : tokenStore.getIfAvailable() instanceof JpaTokenStore;
        return new JpaWorkflowIndex(
                new JpaTransactionalExecutorProvider(entityManagerFactory),
                converter,
                JpaWorkflowIndexConfiguration.DEFAULT.storesWithToken(storesWithToken)
        );
    }
}
