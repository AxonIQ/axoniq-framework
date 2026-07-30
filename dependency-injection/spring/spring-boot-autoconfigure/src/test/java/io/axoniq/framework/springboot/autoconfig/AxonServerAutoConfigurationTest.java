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

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.TagsConfiguration;
import io.axoniq.framework.axonserver.connector.command.AxonServerCommandBusConnector;
import io.axoniq.framework.axonserver.connector.configuration.ManagedChannelCustomizer;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBusConfiguration;
import io.axoniq.framework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;
import io.axoniq.framework.springboot.util.GrpcServerStub;
import io.axoniq.framework.springboot.util.TcpUtils;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating that the
 * {@link io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer} are registered and
 * customizable when using Spring Boot.
 *
 * @author Steven van Beelen
 */
class AxonServerAutoConfigurationTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withUserConfiguration(TestContext.class)
                .withPropertyValues("axon.axonserver.enabled=true");
    }

    @BeforeAll
    static void beforeAll() {
        System.setProperty("axon.axonserver.servers", GrpcServerStub.DEFAULT_HOST + ":" + TcpUtils.findFreePort());
    }

    @AfterAll
    static void afterAll() {
        System.clearProperty("axon.axonserver.servers");
    }

    @Test
    void disablingAxonServerDisabledAllAxonServerComponents() {
        testContext.withPropertyValues(
                           "axon.axonserver.enabled=false",
                           "axon.postgresql.enabled=false",
                           "axon.eventstorage.jpa.polling-interval=0")
                   .run(
                           context -> {
                               assertThat(context).doesNotHaveBean(AxonServerConnectionManager.class);
                               assertThat(context).doesNotHaveBean(ManagedChannelCustomizer.class);
                               assertThat(context).doesNotHaveBean(AxonServerEventStorageEngine.class);
                               assertThat(context).doesNotHaveBean(PayloadConvertingCommandBusConnector.class);
                           }
                   );
    }

    @Test
    void disablingEventStoreViaProperty() {
        testContext.withPropertyValues("axon.axonserver.event-store.enabled=false").run(
                context -> {
                    assertThat(context.getBean(AxonServerConfiguration.class).getEventStore().isEnabled())
                            .isFalse();
                });
    }

    @Test
    void axonServerConfigurationContainsApplicationIdAsComponentName() {
        String expectedComponentName = "my-awesome-app";
        testContext.withInitializer(context -> context.setId(expectedComponentName)).run(context -> {
            assertThat(context).hasSingleBean(AxonServerConfiguration.class);
            // Default name for beans from an @EnableConfigurationProperties contain their prefix.
            assertThat(context).hasBean("axon.axonserver-" + AxonServerConfiguration.class.getName());

            assertThat(context.getBean(AxonServerConfiguration.class).getComponentName())
                    .isEqualTo(expectedComponentName);
        });
    }

    @Test
    void distributedCommandBusThreadCountIsAdjustableThroughAxonServerConfiguration() {
        testContext.withPropertyValues("axon.axonserver.command-threads=42").run(context -> {
            assertThat(context).hasSingleBean(AxonServerConfiguration.class);
            assertThat(context).hasSingleBean(DistributedCommandBusConfiguration.class);

            int numberOfThreads = context.getBean(DistributedCommandBusConfiguration.class).commandThreads();
            int commandThreads = context.getBean(AxonServerConfiguration.class).getCommandThreads();
            assertThat(commandThreads).isEqualTo(42);
            assertThat(numberOfThreads).isEqualTo(42);
        });
    }

    @Test
    void distributedQueryBusThreadCountIsAdjustableThroughAxonServerConfiguration() {
        testContext.withPropertyValues("axon.axonserver.query-threads=42").run(context -> {
            assertThat(context).hasSingleBean(AxonServerConfiguration.class);
            assertThat(context).hasSingleBean(DistributedQueryBusConfiguration.class);

            int queryThreads = context.getBean(AxonServerConfiguration.class).getQueryThreads();
            assertThat(queryThreads).isEqualTo(42);
            ExecutorService executorService = context.getBean(DistributedQueryBusConfiguration.class)
                                                     .queryExecutorService();
            assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class)
                                       .hasFieldOrPropertyWithValue("corePoolSize", queryThreads);
        });
    }

    @Test
    void tagPropertiesAreCapturedInTagConfiguration() {
        testContext.withUserConfiguration(TestContext.class)
                   .withPropertyValues(
                           "axon.tags.region=Eu",
                           "axon.tags.country=It",
                           "axon.tags.city=Rome"
                   )
                   .run(context -> {
                       TagsConfiguration tagsConfiguration = context.getBean(TagsConfiguration.class);
                       assertThat(tagsConfiguration).isNotNull();
                       Map<String, String> tags = tagsConfiguration.getTags();
                       assertThat(tags.size()).isEqualTo(3);
                       assertThat(tags).containsEntry("region", "Eu");
                       assertThat(tags).containsEntry("country", "It");
                       assertThat(tags).containsEntry("city", "Rome");
                   });
    }

    @Test
    void defaultAxonServerComponentsArePresent() {
        // Multi-tenancy is default-on with Axon Server and would wrap the engine in a tenant-routing wrapper;
        // disable it so this test verifies the plain Axon Server storage engine wiring.
        testContext.withPropertyValues("axon.multitenancy.enabled=false").run(context -> {
            assertThat(context).hasSingleBean(AxonServerConfiguration.class);
            // Default name for beans from an @EnableConfigurationProperties contain their prefix.
            assertThat(context).hasBean("axon.axonserver-" + AxonServerConfiguration.class.getName());
            assertThat(context).hasSingleBean(AxonServerConnectionManager.class);
            assertThat(context).hasBean(AxonServerConnectionManager.class.getName());
            assertThat(context).hasSingleBean(ManagedChannelCustomizer.class);
            assertThat(context).hasBean(ManagedChannelCustomizer.class.getName());
            assertThat(context).hasSingleBean(EventStorageEngine.class);
            assertThat(context).hasBean(EventStorageEngine.class.getName());
            assertThat(context).getBean(EventStorageEngine.class).extracting("delegate")
                               .isInstanceOf(AxonServerEventStorageEngine.class);
            assertThat(context).hasSingleBean(PayloadConvertingCommandBusConnector.class);
            assertThat(context).hasBean(CommandBusConnector.class.getName());
        });
    }

    @Test
    void overrideDefaultAxonServerComponents() {
        testContext.withUserConfiguration(CustomContext.class).run(context -> {
            assertThat(context).hasSingleBean(AxonServerConnectionManager.class);
            assertThat(context).hasBean("customAxonServerConnectionManager");
            assertThat(context).hasSingleBean(ManagedChannelCustomizer.class);
            assertThat(context).hasBean("customManagedChannelCustomizer");
            assertThat(context).hasSingleBean(EventStorageEngine.class);
            assertThat(context).hasBean("customAxonServerEventStorageEngine");
            assertThat(context).hasSingleBean(CommandBusConnector.class);
            assertThat(context).hasSingleBean(PayloadConvertingCommandBusConnector.class);
            assertThat(context).hasBean("customAxonServerCommandBusConnector");
        });
    }

    @Configuration
    @EnableAutoConfiguration
    public static class TestContext {

        @Bean(initMethod = "start", destroyMethod = "shutdown")
        public GrpcServerStub grpcServerStub(@Value("${axon.axonserver.servers}") String servers) {
            return new GrpcServerStub(Integer.parseInt(servers.split(":")[1]));
        }
    }

    @Configuration
    @EnableAutoConfiguration
    public static class CustomContext {

        @Bean
        public AxonServerConnectionManager customAxonServerConnectionManager() {
            AxonServerConnectionManager mock = mock(AxonServerConnectionManager.class);
            AxonServerConnection connectionMock = mock(AxonServerConnection.class);
            when(connectionMock.controlChannel()).thenReturn(mock());
            when(connectionMock.adminChannel()).thenReturn(mock());
            when(mock.getConnection()).thenReturn(connectionMock);
            when(mock.getConnection(anyString())).thenReturn(connectionMock);
            return mock;
        }

        @Bean
        public ManagedChannelCustomizer customManagedChannelCustomizer() {
            return mock(ManagedChannelCustomizer.class);
        }

        @Bean
        public EventStorageEngine customAxonServerEventStorageEngine() {
            return mock(AxonServerEventStorageEngine.class);
        }

        @Bean
        public CommandBusConnector customAxonServerCommandBusConnector() {
            return mock(AxonServerCommandBusConnector.class);
        }
    }
}
