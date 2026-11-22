/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.index.api.WorkflowIndex;
import io.axoniq.framework.workflow.index.api.WorkflowIndex.IndexDelta;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexEntry;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexInitialization;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link JpaWorkflowIndexAutoConfiguration}.
 *
 * @author Allard Buijze
 */
class JpaWorkflowIndexAutoConfigurationTest {

    @Nested
    class OnItsOwn {

        // Logs the warning that the engine keeps no index on creation.
        private static final Class<?> WITHOUT_ENTITY_MANAGER_FACTORY_WARNING =
                JpaWorkflowIndexAutoConfiguration.WithoutEntityManagerFactoryConfiguration.class;

        private final ApplicationContextRunner contextRunner =
                new ApplicationContextRunner()
                        .withConfiguration(AutoConfigurations.of(JpaWorkflowIndexAutoConfiguration.class));

        @Test
        void registersNoWorkflowIndexUnlessTheApplicationSelectsIt() {
            contextRunner.withBean(EntityManagerFactory.class, () -> mock(EntityManagerFactory.class))
                         .run(context -> assertThat(context).doesNotHaveBean(WorkflowIndex.class));
        }

        @Test
        void registersAJpaWorkflowIndexWhenSelectedAndAnEntityManagerFactoryIsPresent() {
            contextRunner.withBean(EntityManagerFactory.class, () -> mock(EntityManagerFactory.class))
                         .withPropertyValues("axon.workflow.index=jpa")
                         .run(context -> assertThat(context).getBean(WorkflowIndex.class)
                                                            .isInstanceOf(JpaWorkflowIndex.class));
        }

        @Test
        void yieldsToAWorkflowIndexTheApplicationDefines() {
            var custom = WorkflowIndex.none();

            contextRunner.withBean(EntityManagerFactory.class, () -> mock(EntityManagerFactory.class))
                         .withBean(WorkflowIndex.class, () -> custom)
                         .withPropertyValues("axon.workflow.index=jpa")
                         .run(context -> assertThat(context).getBean(WorkflowIndex.class).isSameAs(custom));
        }

        @Test
        void registersNoWorkflowIndexWhenSelectedWithoutAnEntityManagerFactory() {
            contextRunner.withPropertyValues("axon.workflow.index=jpa")
                         .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(WorkflowIndex.class));
        }

        @Test
        void warnsWhenSelectedWithoutAnEntityManagerFactory() {
            contextRunner.withPropertyValues("axon.workflow.index=jpa")
                         .run(context -> assertThat(context).hasSingleBean(WITHOUT_ENTITY_MANAGER_FACTORY_WARNING));
        }

        @Test
        void doesNotWarnWithoutAnEntityManagerFactoryWhenTheApplicationDefinesAWorkflowIndex() {
            var custom = new InMemoryWorkflowIndex();

            contextRunner.withBean(WorkflowIndex.class, () -> custom)
                         .withPropertyValues("axon.workflow.index=jpa")
                         .run(context -> {
                             assertThat(context).getBean(WorkflowIndex.class).isSameAs(custom);
                             assertThat(context).doesNotHaveBean(WITHOUT_ENTITY_MANAGER_FACTORY_WARNING);
                         });
        }

        @Test
        void registersNoWorkflowIndexWhenWorkflowsAreDisabled() {
            contextRunner.withBean(EntityManagerFactory.class, () -> mock(EntityManagerFactory.class))
                         .withPropertyValues("axon.workflow.enabled=false", "axon.workflow.index=jpa")
                         .run(context -> assertThat(context).doesNotHaveBean(WorkflowIndex.class));
        }
    }

    @Nested
    class InTheAxonConfiguration {

        private final ApplicationContextRunner contextRunner =
                new ApplicationContextRunner()
                        .withUserConfiguration(TestContext.class)
                        .withPropertyValues(
                                "axon.workflow.index=jpa",
                                "axon.axonserver.enabled=false",
                                "axon.postgresql.enabled=false",
                                "axon.eventstorage.jpa.polling-interval=0",
                                "spring.datasource.generate-unique-name=true"
                        );

        @Test
        void keepsTheIndexInTheApplicationDatabase() {
            contextRunner.run(context -> {
                // given
                var workflowIndex = context.getBean(AxonConfiguration.class).getComponent(WorkflowIndex.class);

                // when
                workflowIndex.initialize("orders", Set.of("order-1")).orTimeout(5, TimeUnit.SECONDS).join();
                workflowIndex.store("orders", new IndexDelta(Set.of("order-2"), Set.of()))
                             .orTimeout(5, TimeUnit.SECONDS)
                             .join();

                // then - the entities are part of the application's persistence unit
                assertThat(workflowIndex).isInstanceOf(JpaWorkflowIndex.class)
                                         .isSameAs(context.getBean(WorkflowIndex.class));
                var index = workflowIndex.load("orders", Segment.ROOT_SEGMENT)
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join();
                assertThat(index).hasValue(Set.of(WorkflowIndex.Entry.running("order-1"),
                                                  WorkflowIndex.Entry.running("order-2")));
            });
        }

        @Test
        void usesTheWorkflowIndexTheApplicationDefines() {
            contextRunner.withUserConfiguration(CustomWorkflowIndexContext.class)
                         .run(context -> assertThat(
                                 context.getBean(AxonConfiguration.class).getComponent(WorkflowIndex.class)
                         ).isSameAs(context.getBean(CustomWorkflowIndexContext.CUSTOM, WorkflowIndex.class)));
        }

        @Test
        void keepsNoIndexAndRegistersNoEntitiesUnlessTheApplicationSelectsTheIndex() {
            contextRunner.withPropertyValues("axon.workflow.index=none")
                         .run(context -> {
                             assertThat(context).doesNotHaveBean(JpaWorkflowIndex.class);
                             assertThat(context.getBean(AxonConfiguration.class).getComponent(WorkflowIndex.class))
                                     .isSameAs(WorkflowIndex.none());
                             var entities = context.getBean(EntityManagerFactory.class).getMetamodel().getEntities();
                             assertThat(entities).extracting(entity -> entity.getJavaType().getName())
                                                 .isNotEmpty()
                                                 .doesNotContain(WorkflowIndexEntry.class.getName(),
                                                                 WorkflowIndexInitialization.class.getName());
                         });
        }

        @Test
        void keepsNoIndexWithoutJpa() {
            contextRunner.withPropertyValues(
                                 "spring.autoconfigure.exclude="
                                         + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
                                 "spring.data.jpa.repositories.enabled=false"
                         )
                         .run(context -> {
                             assertThat(context).doesNotHaveBean(EntityManagerFactory.class)
                                                .doesNotHaveBean(JpaWorkflowIndex.class);
                             assertThat(context.getBean(AxonConfiguration.class).getComponent(WorkflowIndex.class))
                                     .isSameAs(WorkflowIndex.none());
                         });
        }

        @Test
        void startsAWorkflowModuleWhoseTokensAreInTheApplicationDatabase() {
            contextRunner.withUserConfiguration(WorkflowContext.class)
                         .run(context -> {
                             assertThat(context).hasNotFailed();
                             assertThat(context.getBean(AxonConfiguration.class).getComponent(TokenStore.class))
                                     .isInstanceOf(JpaTokenStore.class);
                         });
        }

        @Test
        void refusesToStartAWorkflowModuleWhoseTokensAreInMemory() {
            contextRunner.withUserConfiguration(WorkflowContext.class)
                         .withBean(TokenStore.class, InMemoryTokenStore::new)
                         .run(context -> assertThat(context).hasFailed()
                                                            .getFailure()
                                                            .hasRootCauseInstanceOf(AxonConfigurationException.class)
                                                            .rootCause()
                                                            .hasMessageContaining("InMemoryTokenStore")
                                                            .hasMessageContaining(JpaWorkflowIndex.class.getName()));
        }
    }

    @Nested
    class WithAJpaWorkflowIndexOfTheApplication {

        // Without axon.workflow.index.
        private final ApplicationContextRunner contextRunner =
                new ApplicationContextRunner()
                        .withUserConfiguration(TestContext.class)
                        .withPropertyValues(
                                "axon.axonserver.enabled=false",
                                "axon.postgresql.enabled=false",
                                "axon.eventstorage.jpa.polling-interval=0",
                                "spring.datasource.generate-unique-name=true"
                        );

        @Test
        void registersTheEntitiesOfTheIndexForABeanDeclaredAsJpaWorkflowIndex() {
            contextRunner.withUserConfiguration(DeclaredJpaWorkflowIndexContext.class).run(context -> {
                // given
                var workflowIndex = context.getBean(AxonConfiguration.class).getComponent(WorkflowIndex.class);
                assertThat(workflowIndex).isSameAs(context.getBean(JpaWorkflowIndex.class));

                // when
                workflowIndex.initialize("orders", Set.of("order-1")).orTimeout(5, TimeUnit.SECONDS).join();
                workflowIndex.store("orders", new IndexDelta(Set.of("order-2"), Set.of()))
                             .orTimeout(5, TimeUnit.SECONDS)
                             .join();

                // then
                var index = workflowIndex.load("orders", Segment.ROOT_SEGMENT)
                                         .orTimeout(5, TimeUnit.SECONDS)
                                         .join();
                assertThat(index).hasValue(Set.of(WorkflowIndex.Entry.running("order-1"),
                                                  WorkflowIndex.Entry.running("order-2")));
            });
        }

        @Test
        void registersNoEntitiesForAJpaWorkflowIndexBeanDeclaredAsWorkflowIndex() {
            // The auto-configuration only sees the declared type of a bean before the persistence unit is set up.
            contextRunner.withUserConfiguration(UndeclaredJpaWorkflowIndexContext.class).run(context -> {
                assertThat(context.getBean(AxonConfiguration.class).getComponent(WorkflowIndex.class))
                        .isInstanceOf(JpaWorkflowIndex.class);
                var entities = context.getBean(EntityManagerFactory.class).getMetamodel().getEntities();
                assertThat(entities).extracting(entity -> entity.getJavaType().getName())
                                    .isNotEmpty()
                                    .doesNotContain(WorkflowIndexEntry.class.getName(),
                                                    WorkflowIndexInitialization.class.getName());
            });
        }
    }

    @Configuration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class TestContext {

    }

    @Configuration
    static class DeclaredJpaWorkflowIndexContext {

        @Bean
        public JpaWorkflowIndex jpaWorkflowIndex(EntityManagerFactory entityManagerFactory) {
            return JpaWorkflowIndex.builder().entityManagerFactory(entityManagerFactory).build();
        }
    }

    @Configuration
    static class UndeclaredJpaWorkflowIndexContext {

        @Bean
        public WorkflowIndex jpaWorkflowIndex(EntityManagerFactory entityManagerFactory) {
            return JpaWorkflowIndex.builder().entityManagerFactory(entityManagerFactory).build();
        }
    }

    @Configuration
    static class CustomWorkflowIndexContext {

        static final String CUSTOM = "customWorkflowIndex";

        @Bean(CUSTOM)
        public WorkflowIndex customWorkflowIndex() {
            return new InMemoryWorkflowIndex();
        }
    }

    @Configuration
    static class WorkflowContext {

        @Bean
        public PaymentWorkflow paymentWorkflow() {
            return new PaymentWorkflow();
        }
    }

    public static class PaymentWorkflow {

        @Workflow(workflowName = "PaymentWorkflow", startOnEventName = "io.axoniq.test.PaymentRequested")
        public void pay(SimpleWorkflowContext context) {
        }
    }
}
