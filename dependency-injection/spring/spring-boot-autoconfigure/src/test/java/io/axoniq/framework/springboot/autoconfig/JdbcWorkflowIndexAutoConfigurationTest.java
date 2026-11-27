/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.workflow.index.jdbc.JdbcWorkflowIndex;
import io.axoniq.framework.workflow.index.jdbc.WorkflowIndexSchema;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.conversion.DelegatingGeneralConverter;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests which workflow index the {@link JpaWorkflowIndexAutoConfiguration} and the
 * {@link JdbcWorkflowIndexAutoConfiguration} register: the one of the type of the token store, the JPA index when the
 * token store does not tell, and none when the application defines its own.
 *
 * @author Stefan Dragisic
 */
class JdbcWorkflowIndexAutoConfigurationTest {

    private final GeneralConverter converter = new DelegatingGeneralConverter(new JacksonConverter());
    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        var dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:workflowindexautoconfiguration");
        testContext = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(JpaWorkflowIndexAutoConfiguration.class,
                                                         JdbcWorkflowIndexAutoConfiguration.class))
                .withBean(DataSource.class, () -> dataSource)
                .withBean(GeneralConverter.class, () -> converter);
    }

    @Nested
    class WithoutJpa {

        @Test
        void registersAJdbcIndexThatStoresWithAJdbcTokenStore() {
            testContext.withBean(JdbcTokenStore.class, JdbcWorkflowIndexAutoConfigurationTest.this::jdbcTokenStore)
                       .run(context -> {
                           assertThat(context.getBean(WorkflowIndex.class)).isInstanceOf(JdbcWorkflowIndex.class);
                           assertThat(context.getBean(WorkflowIndex.class).storesWithToken()).isTrue();
                       });
        }

        @Test
        void registersAJdbcIndexThatStoresOnItsOwnWithAnotherTokenStore() {
            testContext.withBean(InMemoryTokenStore.class, InMemoryTokenStore::new)
                       .run(context -> {
                           assertThat(context.getBean(WorkflowIndex.class)).isInstanceOf(JdbcWorkflowIndex.class);
                           assertThat(context.getBean(WorkflowIndex.class).storesWithToken()).isFalse();
                       });
        }

        @Test
        void usesTheDefaultSchemaUnlessTheApplicationDefinesOne() {
            var schema = WorkflowIndexSchema.builder().workflowTable("workflow_index_entry").build();
            testContext.run(context -> assertThat(context.getBean(WorkflowIndexSchema.class).workflowTable())
                    .isEqualTo("WorkflowIndexEntry"));
            testContext.withBean(WorkflowIndexSchema.class, () -> schema)
                       .run(context -> assertThat(context.getBean(WorkflowIndexSchema.class)).isSameAs(schema));
        }
    }

    @Nested
    class WithJpa {

        @Test
        void registersAJpaIndexWhenTheTokenStoreDoesNotTell() {
            withJpa().withBean(InMemoryTokenStore.class, InMemoryTokenStore::new)
                     .run(context -> assertThat(context.getBean(WorkflowIndex.class))
                             .isInstanceOf(JpaWorkflowIndex.class));
        }

        @Test
        void registersAJpaIndexThatStoresWithAJpaTokenStore() {
            withJpa().withBean(JpaTokenStore.class, () -> new JpaTokenStore(
                             new JpaTransactionalExecutorProvider(mock(EntityManagerFactory.class)),
                             converter,
                             JpaTokenStoreConfiguration.DEFAULT
                     ))
                     .run(context -> {
                         assertThat(context.getBean(WorkflowIndex.class)).isInstanceOf(JpaWorkflowIndex.class);
                         assertThat(context.getBean(WorkflowIndex.class).storesWithToken()).isTrue();
                     });
        }

        @Test
        void registersAJdbcIndexThatStoresWithAJdbcTokenStore() {
            withJpa().withBean(JdbcTokenStore.class, JdbcWorkflowIndexAutoConfigurationTest.this::jdbcTokenStore)
                     .run(context -> {
                         assertThat(context.getBean(WorkflowIndex.class)).isInstanceOf(JdbcWorkflowIndex.class);
                         assertThat(context.getBean(WorkflowIndex.class).storesWithToken()).isTrue();
                     });
        }

        private ApplicationContextRunner withJpa() {
            return testContext.withBean(EntityManagerFactory.class, () -> mock(EntityManagerFactory.class));
        }
    }

    @Test
    void yieldsToAWorkflowIndexOfTheApplication() {
        var own = new JpaWorkflowIndex(new JpaTransactionalExecutorProvider(mock(EntityManagerFactory.class)),
                                       converter,
                                       JpaWorkflowIndexConfiguration.DEFAULT);
        testContext.withBean(WorkflowIndex.class, () -> own)
                   .run(context -> assertThat(context.getBean(WorkflowIndex.class)).isSameAs(own));
    }

    @Test
    void registersNoIndexWhenThePropertyIsOff() {
        testContext.withPropertyValues("axon.workflow.index.enabled=false")
                   .run(context -> assertThat(context).doesNotHaveBean(WorkflowIndex.class));
    }

    private JdbcTokenStore jdbcTokenStore() {
        return new JdbcTokenStore(new JdbcTransactionalExecutorProvider(mock(DataSource.class)),
                                  converter,
                                  JdbcTokenStoreConfiguration.DEFAULT);
    }
}
