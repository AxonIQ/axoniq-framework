/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.jpa.AggregateBasedJpaEventStorageEngine;
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
                .withPropertyValues("axon.axonserver.enabled=true")
                .withUserConfiguration(EmptyTestContext.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(EventStore.class);
                    assertThat(context).getBean(EventStorageEngine.class).isInstanceOf(
                            AxonServerEventStorageEngine.class);
                });
    }


    @Test
    void notConstructIfEventStoreDefinitionPresent() {
        new ApplicationContextRunner()
                .withPropertyValues("axon.axonserver.enabled=false", "axon.postgresql.enabled=false")
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
                .withPropertyValues("axon.axonserver.enabled=false", "axon.postgresql.enabled=false")
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
