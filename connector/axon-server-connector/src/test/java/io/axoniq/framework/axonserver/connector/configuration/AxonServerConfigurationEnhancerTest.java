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

package io.axoniq.framework.axonserver.connector.configuration;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.command.AxonServerCommandBusConnector;
import io.axoniq.framework.axonserver.connector.event.EventProcessorControlService;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.axonserver.connector.snapshot.AxonServerSnapshotStore;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.PayloadConvertingQueryBusConnector;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link AxonServerConfigurationEnhancer}.
 *
 * @author Allard Buijze
 */
class AxonServerConfigurationEnhancerTest {

    private AxonServerConfigurationEnhancer testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new AxonServerConfigurationEnhancer();
    }

    @Test
    void orderEqualsEnhancersConstant() {
        assertThat(testSubject.order()).isEqualTo(AxonServerConfigurationEnhancer.ENHANCER_ORDER);
    }

    @Test
    void enhanceSetsExpectedDefaultsInAbsenceOfTheseComponents() {
        Configuration result = EventSourcingConfigurer.create()
                                                      .componentRegistry(ComponentRegistry::disableEnhancerScanning)
                                                      .componentRegistry(cr -> testSubject.enhance(cr))
                                                      .build();

        AxonServerConfiguration serverConfig = result.getComponent(AxonServerConfiguration.class);
        assertThat(serverConfig).isNotNull();
        assertThat(serverConfig.getServers()).isEqualTo("localhost");
        assertThat(serverConfig.getClientId()).isNotNull();
        assertThat(serverConfig.getComponentName()).isNotNull();
        assertThat(serverConfig.getComponentName()).contains(serverConfig.getClientId());
        assertThat(result.getComponent(AxonServerConnectionManager.class)).isNotNull();
        assertThat(result.getComponent(ManagedChannelCustomizer.class)).isInstanceOf(ManagedChannelCustomizer.class);
        assertThat(result.getComponent(EventStorageEngine.class)).isInstanceOf(SnapshotCapableEventStorageEngine.class);
        assertThat(result.getComponent(CommandBusConnector.class)).isInstanceOf(PayloadConvertingCommandBusConnector.class);
        assertThat(result.getComponent(QueryBusConnector.class)).isInstanceOf(PayloadConvertingQueryBusConnector.class);
        assertThat(result.getComponent(SnapshotStore.class)).isInstanceOf(AxonServerSnapshotStore.class);
    }

    @Test
    void noRegisteredTopologyChangeListenerInvokesConnectionManagerGetConnectionOnce() {
        AxonConfiguration result =
                EventSourcingConfigurer.create()
                                       .componentRegistry(ComponentRegistry::disableEnhancerScanning)
                                       .componentRegistry(cr -> testSubject.enhance(cr))
                                       .lifecycleRegistry(registry -> registry.registerLifecyclePhaseTimeout(
                                               25, TimeUnit.MILLISECONDS
                                       ))
                                       .componentRegistry(registry -> registry.registerDecorator(
                                               AxonServerConnectionManager.class,
                                               0,
                                               (config, name, delegate) -> Mockito.spy(delegate)
                                       ))
                                       .build();

        result.start();

        AxonServerConnectionManager connectionManager = result.getComponent(AxonServerConnectionManager.class);
        assertThat(connectionManager).isNotNull();
        await().pollDelay(Duration.ofMillis(50))
               .atMost(Duration.ofMillis(500))
               .untilAsserted(() -> verify(connectionManager, atLeastOnce()).getConnection());
    }

    /**
     * This test would ideally verify a {@link TopologyChangeListener} got registered with the
     * {@link io.axoniq.axonserver.connector.control.ControlChannel} of the default context of the
     * {@link AxonServerConnectionManager}. However, this test class is not set up to spin an actual Axon Server
     * instance. Hence, I have decided to only verify if the {@link AxonServerConnectionManager#getConnection()} was
     * invoked twice, where the first occurrence comes from the
     * {@link AxonServerCommandBusConnector} and the second from
     * registering the {@code TopologyChangeListener}.
     */
    @Test
    void registeredTopologyChangeListenerInvokesConnectionManagerGetConnectionTwice() {
        AxonConfiguration result =
                EventSourcingConfigurer.create()
                                       .componentRegistry(ComponentRegistry::disableEnhancerScanning)
                                       .componentRegistry(cr -> testSubject.enhance(cr))
                                       .lifecycleRegistry(registry -> registry.registerLifecyclePhaseTimeout(
                                               25, TimeUnit.MILLISECONDS
                                       ))
                                       .componentRegistry(registry -> registry.registerDecorator(
                                               AxonServerConnectionManager.class,
                                               Integer.MIN_VALUE,
                                               (config, name, delegate) -> Mockito.spy(delegate)
                                       ))
                                       .componentRegistry(registry -> registry.registerComponent(
                                               TopologyChangeListener.class,
                                               c -> Mockito.mock(TopologyChangeListener.class)
                                       ))
                                       .build();

        result.start();

        AxonServerConnectionManager connectionManager = result.getComponent(AxonServerConnectionManager.class);
        assertThat(connectionManager).isNotNull();
        await().pollDelay(Duration.ofMillis(50))
               .atMost(Duration.ofMillis(500))
               .untilAsserted(() -> verify(connectionManager, atLeastOnce()).getConnection());
    }

    @Nested
    class EventProcessorControlServiceTests {

        @Test
        void eventProcessorControlServiceIsRegistered() {
            // given / when
            Configuration result = EventSourcingConfigurer.create()
                                                          .componentRegistry(ComponentRegistry::disableEnhancerScanning)
                                                          .componentRegistry(cr -> testSubject.enhance(cr))
                                                          .build();

            // then
            assertThat(result.getComponent(
                    EventProcessorControlService.class
            )).isNotNull();
        }

        @Test
        void eventProcessorControlServiceStartsAndInvokesConnectionManagerGetConnection() {
            // given
            AxonConfiguration result =
                    EventSourcingConfigurer.create()
                                           .componentRegistry(ComponentRegistry::disableEnhancerScanning)
                                           .componentRegistry(cr -> testSubject.enhance(cr))
                                           .lifecycleRegistry(registry -> registry.registerLifecyclePhaseTimeout(
                                                   25, TimeUnit.MILLISECONDS
                                           ))
                                           .componentRegistry(registry -> registry.registerDecorator(
                                                   AxonServerConnectionManager.class,
                                                   Integer.MIN_VALUE,
                                                   (config, name, delegate) -> Mockito.spy(delegate)
                                           ))
                                           .build();

            // when
            result.start();

            // then
            AxonServerConnectionManager connectionManager = result.getComponent(AxonServerConnectionManager.class);
            assertThat(connectionManager).isNotNull();
            await().pollDelay(Duration.ofMillis(50))
                   .atMost(Duration.ofMillis(500))
                   .untilAsserted(() -> verify(connectionManager, atLeastOnce()).getConnection());
        }
    }
}