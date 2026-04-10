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

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import org.axonframework.messaging.eventhandling.deadletter.jdbc.DeadLetterSchema;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class JdbcDeadLetterQueueAutoConfigurationTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withUserConfiguration(TestContext.class)
                .withPropertyValues("axon.axonserver.enabled=false");
    }

    @Test
    void jdbcDeadLetterQueueFactoryBeanIsAutoConfigured() {
        testContext.run(context -> assertThat(context).hasSingleBean(SequencedDeadLetterQueueFactory.class));
    }

    @Test
    void defaultDeadLetterSchemaIsPresent() {
        testContext.run(context -> {
            DeadLetterSchema schema = context.getBean(DeadLetterSchema.class);
            assertNotNull(schema);
            assertEquals("DeadLetterEntry", schema.deadLetterTable());
        });
    }

    @Test
    void customDeadLetterSchemaIsPresent() {
        String expectedDeadLetterTable = "custom-table-name";
        DeadLetterSchema customSchema = DeadLetterSchema.builder()
                                                        .deadLetterTable(expectedDeadLetterTable)
                                                        .build();
        testContext.withBean(DeadLetterSchema.class, () -> customSchema)
                   .run(context -> {
                       DeadLetterSchema schema = context.getBean(DeadLetterSchema.class);
                       assertNotNull(schema);
                       assertEquals(expectedDeadLetterTable, schema.deadLetterTable());
                   });
    }

    @Test
    void customSequencedDeadLetterQueueFactoryOverridesDefault() {
        SequencedDeadLetterQueueFactory customFactory = mock(SequencedDeadLetterQueueFactory.class);
        testContext.withBean(SequencedDeadLetterQueueFactory.class, () -> customFactory)
                   .run(context -> {
                       assertThat(context).hasSingleBean(SequencedDeadLetterQueueFactory.class);
                       assertThat(context.getBean(SequencedDeadLetterQueueFactory.class)).isSameAs(customFactory);
                   });
    }

    @ContextConfiguration
    @EnableAutoConfiguration(exclude = {
            JpaRepositoriesAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    private static class TestContext {

        @Bean
        public DataSource dataSource() {
            return mock(DataSource.class);
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
