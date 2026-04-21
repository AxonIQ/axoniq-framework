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

import io.axoniq.framework.springboot.util.GrpcServerStub;
import io.axoniq.framework.springboot.util.TcpUtils;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;

/**
 * Test class validating the {@code CommandLoadFactorProvider} is correctly auto-configured.
 *
 * @author Sara Pellegrini
 */
class AxonServerAutoConfigurationLoadFactorTest {

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
    @Disabled("TODO #3074 - Revisit Command Load Factor support")
    void loadFactor() {
//        testContext.withUserConfiguration(TestContext.class)
//                   .withPropertyValues("axon.axonserver.command-load-factor=36")
//                   .run(context -> {
//                       CommandLoadFactorProvider commandLoadFactorProvider =
//                               context.getBean("commandLoadFactorProvider", CommandLoadFactorProvider.class);
//                       int loadFactor = commandLoadFactorProvider.getFor("anything");
//                       assertEquals(36, loadFactor);
//                   });
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
