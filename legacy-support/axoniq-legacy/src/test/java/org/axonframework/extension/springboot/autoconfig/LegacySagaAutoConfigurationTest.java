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

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.extension.spring.config.SpringSagaLookup;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.inmemory.InMemorySagaStore;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link LegacySagaAutoConfiguration}.
 *
 * @author Mateusz Nowak
 */
class LegacySagaAutoConfigurationTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withUserConfiguration(TestContext.class)
                .withPropertyValues("axon.eventstorage.jpa.polling-interval=0");
    }

    @Test
    void backsOffWithoutAxonLegacyOnTheClasspath() {
        testContext
                .withClassLoader(new FilteredClassLoader(SagaStore.class))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(SpringSagaLookup.class);
                    assertThat(context).doesNotHaveBean(SagaStore.class);
                });
    }

    @Test
    void registersTheSagaLookup() {
        testContext.run(context -> assertThat(context).hasSingleBean(SpringSagaLookup.class));
    }

    @Test
    void fallsBackToAnInMemorySagaStore() {
        testContext.run(context -> {
            assertThat(context).hasBean("sagaStore");
            assertThat(context.getBean("sagaStore")).isInstanceOf(InMemorySagaStore.class);
        });
    }

    @Test
    void userDefinedSagaStoreBacksOffTheFallback() {
        testContext.withUserConfiguration(CustomSagaStoreContext.class).run(context -> {
            assertThat(context).doesNotHaveBean("sagaStore");
            assertThat(context.getBeanNamesForType(SagaStore.class)).containsExactly("customSagaStore");
            assertThat(context.getBean("customSagaStore", SagaStore.class))
                    .isSameAs(context.getBean(SagaStore.class));
        });
    }

    /**
     * Excludes Hibernate and the embedded {@code DataSource} autoconfiguration: this module has {@code hsqldb} and
     * {@code spring-boot-starter-data-jpa} on its test classpath, so without excluding them Spring Boot would
     * auto-configure an embedded {@code EntityManagerFactory} and {@link LegacyJpaSagaStoreAutoConfiguration} would
     * activate instead of the in-memory fallback under test here.
     */
    @Configuration
    @EnableAutoConfiguration(exclude = {HibernateJpaAutoConfiguration.class, DataSourceAutoConfiguration.class})
    static class TestContext {

    }

    @Configuration
    static class CustomSagaStoreContext {

        @Bean
        public SagaStore<Object> customSagaStore() {
            return new InMemorySagaStore();
        }
    }
}
