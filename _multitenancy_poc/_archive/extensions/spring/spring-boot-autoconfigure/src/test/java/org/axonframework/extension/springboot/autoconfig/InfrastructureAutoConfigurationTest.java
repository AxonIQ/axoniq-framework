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

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.extension.spring.config.MessageHandlerLookup;
import org.axonframework.extension.spring.config.SpringEventSourcedEntityLookup;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Test class validating the behavior of the {@link InfrastructureAutoConfiguration}.
 *
 * @author Simon Zambrovski
 */
class InfrastructureAutoConfigurationTest {

    private ApplicationContextRunner testApplicationContext;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner()
                .withUserConfiguration(DefaultContext.class)
                .withPropertyValues("axon.axonserver.enabled:false", "axon.eventstorage.jpa.polling-interval:0");
    }

    @Test
    public void initializesComponents() {
        testApplicationContext.run(context -> {
                                       SpringEventSourcedEntityLookup springEventSourcedEntityLookup = context.getBean(
                                               SpringEventSourcedEntityLookup.class);
                                       assertThat(springEventSourcedEntityLookup).isNotNull();

                                       MessageHandlerLookup messageHandlerLookup = context.getBean(MessageHandlerLookup.class);
                                       assertThat(messageHandlerLookup).isNotNull();
                                   }
        );
    }


    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class DefaultContext {

    }
}
