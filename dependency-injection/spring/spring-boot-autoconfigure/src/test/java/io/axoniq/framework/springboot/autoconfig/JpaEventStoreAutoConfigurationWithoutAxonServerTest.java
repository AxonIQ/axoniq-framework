/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.jpa.AggregateBasedJpaEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests JPA EventStore auto-configuration
 *
 * @author Sara Pellegrini
 * @author Simon Zambrovski
 */
class JpaEventStoreAutoConfigurationWithoutAxonServerTest {

    @Test
    void construct() {
        new ApplicationContextRunner()
                .withPropertyValues(
                        "axon.axonserver.enabled=false",
                        "axon.postgresql.enabled=false",
                        "axon.workflow.enabled=false",
                        "axon.eventstorage.jpa.polling-interval=0"
                )
                .withUserConfiguration(EmptyTestContext.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(EventStore.class);
                    assertThat(context).getBean(EventStorageEngine.class)
                                       .isInstanceOf(AggregateBasedJpaEventStorageEngine.class);
                });
    }

    @Test
    void axonServerWinsIfEnabled() {
        new ApplicationContextRunner()
                // Multi-tenancy is default-on with Axon Server; disable it so this test isolates
                // the Axon-Server-over-JPA storage engine precedence rather than the tenant-routing wrapper.
                .withPropertyValues(
                    "axon.axonserver.enabled=true",
                    "axon.multitenancy.enabled=false",
                    "axon.workflow.enabled=false"
                )
                .withUserConfiguration(EmptyTestContext.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(EventStore.class);
                    // AxonServerEventStorageEngine is also a SnapshotStore, so the same bean is exposed under both
                    assertThat(context.getBean(EventStorageEngine.class.getName(), EventStorageEngine.class))
                            .isInstanceOf(AxonServerEventStorageEngine.class)
                            .isSameAs(context.getBean(SnapshotStore.class.getName(), SnapshotStore.class));
                });
    }

    @Test
    void notConstructIfEventStoreDefinitionPresent() {
        new ApplicationContextRunner()
                .withPropertyValues(
                    "axon.axonserver.enabled=false",
                    "axon.postgresql.enabled=false",
                    "axon.workflow.enabled=false"
                )
                .withUserConfiguration(ContextWithStore.class, EmptyTestContext.class)
                .run(context -> {
                    assertThat(context)
                            .hasBean(ContextWithStore.STORE_NAME)
                            .doesNotHaveBean(AggregateBasedJpaEventStorageEngine.class);
                    assertThat(context).getBeanNames(EventStore.class).containsExactly(ContextWithStore.STORE_NAME);
                });
    }

    @Test
    void notConstructIfEventStorageEngineDefinitionPresent() {
        new ApplicationContextRunner()
                .withPropertyValues(
                    "axon.axonserver.enabled=false",
                    "axon.postgresql.enabled=false",
                    "axon.workflow.enabled=false"
                )
                .withUserConfiguration(ContextWithEngine.class, EmptyTestContext.class)
                .run(context -> {
                    assertThat(context)
                            .hasBean(ContextWithEngine.ENGINE_NAME)
                            .doesNotHaveBean(AggregateBasedJpaEventStorageEngine.class);
                    assertThat(context).getBeanNames(EventStorageEngine.class)
                                       .containsExactly(ContextWithEngine.ENGINE_NAME);
                });
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    private static class EmptyTestContext {

        @Bean
        public EntityManagerFactory entityManagerFactory() {
            return mock();
        }
    }

    private static class ContextWithStore {

        static final String STORE_NAME = "StoreName";

        @Bean(STORE_NAME)
        public EventStore eventStore() {
            return new StorageEngineBackedEventStore(mock(), mock(), mock());
        }
    }

    private static class ContextWithEngine {

        static final String ENGINE_NAME = "EngineName";

        @Bean(ENGINE_NAME)
        public EventStorageEngine mockEventStorageEngine() {
            return mock();
        }
    }
}
