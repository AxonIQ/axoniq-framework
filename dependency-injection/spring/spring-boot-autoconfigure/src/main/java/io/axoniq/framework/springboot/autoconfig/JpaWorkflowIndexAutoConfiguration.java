/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.extension.springboot.util.RegisterDefaultEntities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot auto-configuration that keeps the index of running workflow instances in the application's database
 * through a {@link JpaWorkflowIndex}, when the application selects it with {@code axon.workflow.index=jpa}, or defines
 * a {@code JpaWorkflowIndex} bean of its own.
 * <p>
 * The index is opt-in: without the property, the workflow engine keeps no index, and every claim of a segment reads the
 * running workflow instances from the event store. The index must be written by every node that runs the workflow
 * modules, as a node without it advances the tokens without writing the index. Select it once every node runs a
 * version that supports it, so a rolling upgrade has finished, and empty its tables if nodes without the index advanced
 * the tokens meanwhile. The reference guide describes introducing the index into a running application.
 * <p>
 * With {@code axon.workflow.index=jpa} and an {@code EntityManagerFactory} bean, this configuration registers the
 * entities of the index with the persistence unit, and a {@link JpaWorkflowIndex} bean using the application's
 * {@code EntityManagerFactory}. The workflow engine then restores the running workflow instances of a claimed segment
 * from the stored index plus the lifecycle events after the token the segment resumes from, also after a restart and on
 * other nodes, instead of reading every workflow lifecycle event. While the database is unavailable, the segments with
 * index changes that are not written yet do not advance their tokens. Without an {@code EntityManagerFactory} bean, it
 * registers nothing, and logs a warning.
 * <p>
 * The entities are registered whenever the index is selected, also when the application defines a
 * {@link WorkflowIndex} bean of its own, just like the entities of the JPA token store. They are registered as well
 * when the application defines a bean of type {@link JpaWorkflowIndex} without selecting the index. That relies on the
 * declared type of the bean: a {@code @Bean} method has to declare {@code JpaWorkflowIndex} as its return type, as a
 * {@code JpaWorkflowIndex} returned by a method declaring {@code WorkflowIndex} is only known as such once it is
 * created, after the persistence unit was set up. Applications that manage their
 * schema, for example through Flyway, Liquibase or {@code spring.jpa.hibernate.ddl-auto=validate}, therefore create the
 * tables of {@link io.axoniq.framework.workflow.index.jpa.WorkflowIndexEntry} and
 * {@link io.axoniq.framework.workflow.index.jpa.WorkflowIndexInitialization} themselves before selecting the index, as
 * described by the Javadoc of {@link JpaWorkflowIndex}; the tables follow the JPA provider's naming strategy.
 * <p>
 * A bean of type {@link WorkflowIndex} the application defines is used whatever {@code axon.workflow.index} says; the
 * {@code @ConditionalOnMissingBean} guard on the JPA index yields to it.
 *
 * @author Allard Buijze
 * @since 5.5.0
 * @see JpaWorkflowIndex
 */
@AutoConfiguration(
        afterName = "org.axonframework.extension.springboot.autoconfig.JpaAutoConfiguration"
)
@ConditionalOnClass({EntityManagerFactory.class, JpaWorkflowIndex.class})
@ConditionalOnProperty(prefix = "axon.workflow", name = "enabled", matchIfMissing = true)
public class JpaWorkflowIndexAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(JpaWorkflowIndexAutoConfiguration.class);

    /**
     * Registers the entities of the index when the application has an {@code EntityManagerFactory}, and either selects
     * the index or defines a {@link JpaWorkflowIndex} bean.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(EntityManagerFactory.class)
    @Conditional(JpaWorkflowIndexInUse.class)
    @RegisterDefaultEntities(packages = {
            "io.axoniq.framework.workflow.index.jpa"
    })
    static class JpaWorkflowIndexEntitiesConfiguration {

    }

    /**
     * Registers the {@link JpaWorkflowIndex} when the application selects the index and has an
     * {@code EntityManagerFactory}.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(EntityManagerFactory.class)
    @ConditionalOnProperty(prefix = "axon.workflow", name = "index", havingValue = "jpa")
    static class JpaWorkflowIndexConfiguration {

        /**
         * Creates a {@link JpaWorkflowIndex} keeping the index of running workflow instances in the database of the
         * given {@code entityManagerFactory}.
         *
         * @param entityManagerFactory the {@link EntityManagerFactory} of the persistence unit holding the index
         * @return a {@link WorkflowIndex} backed by JPA
         */
        @Bean
        @ConditionalOnMissingBean
        public WorkflowIndex workflowIndex(EntityManagerFactory entityManagerFactory) {
            return JpaWorkflowIndex.builder()
                                   .entityManagerFactory(entityManagerFactory)
                                   .build();
        }
    }

    /**
     * Warns that the JPA index was selected while the application has no {@code EntityManagerFactory}, in which case
     * the engine keeps no index. Silent when the application defines a {@link WorkflowIndex} of its own, which the
     * engine uses instead.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "axon.workflow", name = "index", havingValue = "jpa")
    @ConditionalOnMissingBean({EntityManagerFactory.class, WorkflowIndex.class})
    static class WithoutEntityManagerFactoryConfiguration {

        WithoutEntityManagerFactoryConfiguration() {
            logger.warn("The property axon.workflow.index=jpa selects the JPA workflow index, but the application has "
                                + "no EntityManagerFactory bean. The workflow engine keeps no index.");
        }
    }

    /**
     * Matches when the application selects the JPA index, or defines a bean whose declared type is
     * {@link JpaWorkflowIndex}.
     */
    static class JpaWorkflowIndexInUse extends AnyNestedCondition {

        JpaWorkflowIndexInUse() {
            super(ConfigurationPhase.REGISTER_BEAN);
        }

        @ConditionalOnProperty(prefix = "axon.workflow", name = "index", havingValue = "jpa")
        static class Selected {

        }

        @ConditionalOnBean(JpaWorkflowIndex.class)
        static class Defined {

        }
    }
}
