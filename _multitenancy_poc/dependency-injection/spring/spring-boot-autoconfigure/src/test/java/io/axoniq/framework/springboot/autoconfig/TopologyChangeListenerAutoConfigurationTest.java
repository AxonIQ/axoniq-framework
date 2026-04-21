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
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.configuration.TopologyChangeListener;
import io.axoniq.framework.springboot.util.GrpcServerStub;
import io.axoniq.framework.springboot.util.TcpUtils;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ContextConfiguration;

import static org.mockito.Mockito.*;

/**
 * Autoconfiguration test class validating registration of the {@link TopologyChangeListener} with the
 * {@link io.axoniq.axonserver.connector.control.ControlChannel} for the default
 * {@link AxonServerConfiguration#getContext() context}.
 *
 * @author Steven van Beelen
 */
class TopologyChangeListenerAutoConfigurationTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner();
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
    void topologyChangeListenersAreInvokedForCommandHandlerRegistration() {
        testContext.withUserConfiguration(TestContext.class).run(context -> {
            TopologyChangeListener listenerOne = context.getBean("listenerOne", TopologyChangeListener.class);
            TopologyChangeListener listenerTwo = context.getBean("listenerTwo", TopologyChangeListener.class);

            ControlChannel mockedControlChannel = context.getBean("mockedControlChannel", ControlChannel.class);

            verify(mockedControlChannel).registerTopologyChangeHandler(listenerOne);
            verify(mockedControlChannel).registerTopologyChangeHandler(listenerTwo);
        });
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    private static class TestContext {

        @Bean
        public ControlChannel mockedControlChannel() {
            return mock(ControlChannel.class);
        }

        @Bean
        @Primary
        public AxonServerConnectionManager connectionManager(ControlChannel mockedControlChannel) {
            AxonServerConnection mockedConnection = mock(AxonServerConnection.class);
            when(mockedConnection.controlChannel()).thenReturn(mockedControlChannel);
            AxonServerConnectionManager mockedManager = mock(AxonServerConnectionManager.class);
            when(mockedManager.getConnection()).thenReturn(mockedConnection);
            when(mockedManager.getConnection(any())).thenReturn(mockedConnection);
            return mockedManager;
        }

        @Bean
        public TopologyChangeListener listenerOne() {
            return change -> {
                // Unimportant - only exists to validate the change listener registration.
            };
        }

        @Bean
        public TopologyChangeListener listenerTwo() {
            return change -> {
                // Unimportant - only exists to validate the change listener registration.
            };
        }

        @Bean(initMethod = "start", destroyMethod = "shutdown")
        public GrpcServerStub grpcServerStub(@Value("${axon.axonserver.servers}") String servers) {
            return new GrpcServerStub(Integer.parseInt(servers.split(":")[1]));
        }
    }
}