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
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot autoconfiguration that registers a JPA-backed {@link WorkflowIndex} bean.
 * <p>
 * This configuration activates when {@code axon.workflow.index.enabled} is {@code true}, workflows are not switched
 * off through {@code axon.workflow.enabled}, and an {@code EntityManagerFactory} bean is present. The registered
 * {@link JpaWorkflowIndex} uses the application's {@code EntityManagerFactory}, like the JPA token store does, so the
 * workflow engine writes the rows of the workflow index in the transaction that stores the token of its event
 * processor. The entities of the workflow index are registered with the persistence unit of the application.
 * <p>
 * To enable the workflow index, set:
 * <pre>{@code
 * axon.workflow.index.enabled=true
 * }</pre>
 * <p>
 * When the token store uses another database than the workflow index, also set:
 * <pre>{@code
 * axon.workflow.index.stores-with-token=false
 * }</pre>
 * <p>
 * To replace the JPA workflow index with another one, declare your own {@link WorkflowIndex} bean. The
 * {@code @ConditionalOnMissingBean} guard on the default yields to it.
 *
 * @author Stefan Dragisic
 * @see WorkflowIndex
 * @see JpaWorkflowIndex
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
@ConditionalOnProperty(prefix = "axon.workflow.index", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(WorkflowProperties.class)
@RegisterDefaultEntities(packages = {
        "io.axoniq.framework.workflow.index.jpa",
})
public class JpaWorkflowIndexAutoConfiguration {

    /**
     * Creates a {@link JpaWorkflowIndex} that reads and writes its rows through the given
     * {@code entityManagerFactory}.
     *
     * @param entityManagerFactory the JPA {@link EntityManagerFactory} used for persistence
     * @param converter            the {@link GeneralConverter} used to convert the associations of an open wait
     * @param workflowProperties   the {@link WorkflowProperties} holding the workflow index settings
     * @return a {@link WorkflowIndex} backed by JPA
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(GeneralConverter.class)
    public WorkflowIndex jpaWorkflowIndex(EntityManagerFactory entityManagerFactory,
                                          GeneralConverter converter,
                                          WorkflowProperties workflowProperties) {
        return new JpaWorkflowIndex(
                new JpaTransactionalExecutorProvider(entityManagerFactory),
                converter,
                JpaWorkflowIndexConfiguration.DEFAULT.storesWithToken(workflowProperties.getIndex().isStoresWithToken())
        );
    }
}
