/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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

import static org.assertj.core.api.Assertions.assertThat;

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
                       assertThat(serverConfig).isNotNull();

                       Map<String, AxonServerConfiguration.Eventhandling.ProcessorSettings> processors =
                               serverConfig.getEventhandling().getProcessors();
                       assertThat(processors).isNotEmpty();
                       assertThat(processors).containsKey("my-processor");

                       AxonServerConfiguration.Eventhandling.ProcessorSettings processorSettings =
                               processors.get("my-processor");
                       assertThat(processorSettings.isAutomaticBalancing()).isTrue();
                       assertThat(processorSettings.getLoadBalancingStrategy()).isEqualTo("PER_THREAD");
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
