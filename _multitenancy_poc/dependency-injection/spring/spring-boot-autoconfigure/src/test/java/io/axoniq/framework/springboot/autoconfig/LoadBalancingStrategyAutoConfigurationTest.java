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

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.springboot.util.GrpcServerStub;
import io.axoniq.framework.springboot.util.TcpUtils;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating whether Axon Server's load balancing properties are set as expected.
 *
 * @author Steven van Beelen
 */
class LoadBalancingStrategyAutoConfigurationTest {

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
    void loadBalancingStrategyIsTakenIntoAccount() {
        testContext.withUserConfiguration(TestContext.class)
                   .withPropertyValues(
                           "axon.axonserver.eventhandling.processors.my-processor.load-balancing-strategy=PER_THREAD",
                           "axon.axonserver.eventhandling.processors.my-processor.automatic-balancing=true"
                   )
                   .run(context -> {
                       AxonServerConfiguration serverConfig = context.getBean(AxonServerConfiguration.class);
                       assertNotNull(serverConfig);

                       Map<String, AxonServerConfiguration.Eventhandling.ProcessorSettings> processors =
                               serverConfig.getEventhandling().getProcessors();
                       assertFalse(processors.isEmpty());
                       assertTrue(processors.containsKey("my-processor"));

                       AxonServerConfiguration.Eventhandling.ProcessorSettings processorSettings =
                               processors.get("my-processor");
                       assertTrue(processorSettings.isAutomaticBalancing());
                       assertEquals("PER_THREAD", processorSettings.getLoadBalancingStrategy());
                   });
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    private static class TestContext {

        @Bean(initMethod = "start", destroyMethod = "shutdown")
        public GrpcServerStub grpcServerStub(@Value("${axon.axonserver.servers}") String servers) {
            return new GrpcServerStub(Integer.parseInt(servers.split(":")[1]));
        }
    }
}
