/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import jakarta.persistence.EntityManagerFactory;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class JpaDeadLetterQueueAutoConfigurationTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withUserConfiguration(TestContext.class)
                .withPropertyValues(
                        "axon.axonserver.enabled=false",
                        "axon.postgresql.enabled=false",
                        "axon.workflow.enabled=false",
                        "axon.eventstorage.jpa.polling-interval=0"
                );
    }

    @Test
    void jpaDeadLetterQueueFactoryBeanIsAutoConfigured() {
        testContext.run(context -> assertThat(context).hasSingleBean(SequencedDeadLetterQueueFactory.class));
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    private static class TestContext {

        @Bean
        public EntityManagerFactory entityManagerFactory() {
            return mock();
        }

        @Bean
        public EventConverter eventConverter() {
            return mock(EventConverter.class);
        }

        @Bean
        public Converter genericConverter() {
            return mock(Converter.class);
        }
    }
}
