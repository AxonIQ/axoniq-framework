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

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.SimpleDeadlineManager;
import org.axonframework.deadline.annotation.DeadlineHandler;
import org.axonframework.messaging.LegacyScopeAwareProvider;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeAwareProviderSettings;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.spring.stereotype.Saga;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating that a deadline a Spring Boot discovered {@link Saga @Saga} schedules reaches its
 * {@link DeadlineHandler @DeadlineHandler}, with no other wiring than a {@link DeadlineManager} bean autowiring the
 * configuration's {@link ScopeAwareProvider}.
 * <p>
 * Hibernate and the embedded {@code DataSource} auto-configuration are excluded, as in {@link SagaAutoConfigurationIT},
 * to keep the in-memory event store and Saga store.
 *
 * @author Jakob Hatzl
 */
@SpringBootTest(
        classes = {
                SagaDeadlineAutoConfigurationIT.TestContext.class,
                SagaDeadlineAutoConfigurationIT.ReminderSaga.class
        },
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "axon.deadline.scope-aware-provider-readiness-timeout=2m"
)
class SagaDeadlineAutoConfigurationIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Autowired
    private ApplicationContext context;

    @Test
    void aDeadlineScheduledByADiscoveredSagaReachesItsDeadlineHandler() {
        // given
        String id = UUID.randomUUID().toString();

        // when
        context.getBean(EventGateway.class)
               .publish(null, new ReminderRequested(id))
               .orTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS)
               .join();

        // then
        ReminderRecorder recorder = context.getBean(ReminderRecorder.class);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(recorder.reminders).containsExactly(id));
    }

    @Test
    void theAutowiredScopeAwareProviderIsTheConfigurationsLegacyScopeAwareProvider() {
        // when
        ScopeAwareProvider bean = context.getBean(ScopeAwareProvider.class);

        // then
        assertThat(bean).isInstanceOf(LegacyScopeAwareProvider.class)
                        .isSameAs(context.getBean(AxonConfiguration.class).getComponent(ScopeAwareProvider.class));
    }

    @Test
    void theReadinessTimeoutIsBoundFromTheProperty() {
        // when
        ScopeAwareProviderSettings settings =
                context.getBean(AxonConfiguration.class).getComponent(ScopeAwareProviderSettings.class);

        // then
        assertThat(settings.readinessTimeout()).isEqualTo(Duration.ofMinutes(2));
    }

    public record ReminderRequested(String id) {

    }

    public static class ReminderRecorder {

        private final List<String> reminders = new CopyOnWriteArrayList<>();
    }

    @ContextConfiguration
    @EnableAutoConfiguration(exclude = {HibernateJpaAutoConfiguration.class, DataSourceAutoConfiguration.class})
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class TestContext {

        @Bean
        public TokenStore tokenStore() {
            return new InMemoryTokenStore();
        }

        @Bean
        public ReminderRecorder reminderRecorder() {
            return new ReminderRecorder();
        }

        @Bean
        public SimpleDeadlineManager deadlineManager(ScopeAwareProvider scopeAwareProvider,
                                                     Configuration configuration) {
            return SimpleDeadlineManager.builder()
                                        .scopeAwareProvider(scopeAwareProvider)
                                        .unitOfWorkFactory(configuration.getComponent(UnitOfWorkFactory.class))
                                        .build();
        }
    }

    @Saga
    @SuppressWarnings({"unused", "deprecation", "removal"})
    public static class ReminderSaga {

        @StartSaga
        @SagaEventHandler(associationProperty = "id")
        void on(ReminderRequested event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Duration.ofMillis(100), "reminder", event.id());
        }

        @DeadlineHandler(deadlineName = "reminder")
        void onReminder(String id, ReminderRecorder recorder) {
            recorder.reminders.add(id);
        }
    }
}
