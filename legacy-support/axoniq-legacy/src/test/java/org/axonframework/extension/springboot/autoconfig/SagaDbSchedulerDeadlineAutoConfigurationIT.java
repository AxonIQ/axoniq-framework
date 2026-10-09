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

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.task.Task;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.annotation.DeadlineHandler;
import org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineManager;
import org.axonframework.extension.springboot.autoconfig.SagaDeadlineAutoConfigurationIT.ReminderRecorder;
import org.axonframework.extension.springboot.autoconfig.SagaDeadlineAutoConfigurationIT.ReminderRequested;
import org.axonframework.extension.springboot.autoconfig.SagaDeadlineAutoConfigurationIT.ReminderSaga;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.spring.stereotype.Saga;
import org.hsqldb.jdbc.JDBCDataSource;
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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.axonframework.common.util.DbSchedulerTestUtil.reCreateTable;

/**
 * Test class validating that a deadline a Spring Boot discovered {@link Saga @Saga} schedules reaches its
 * {@link DeadlineHandler @DeadlineHandler} through the auto-configured {@link DbSchedulerDeadlineManager}, when the
 * application only provides the db-scheduler {@link Scheduler}: no {@link ScopeAwareProvider} wiring.
 * <p>
 * The scheduler starts when its bean is created, as with db-scheduler's Spring Boot starter. Its
 * {@link javax.sql.DataSource} is not a bean, so that the in-memory event store and Saga store are kept, as in
 * {@link SagaDeadlineAutoConfigurationIT}.
 *
 * @author Jakob Hatzl
 */
@SpringBootTest(
        classes = {SagaDbSchedulerDeadlineAutoConfigurationIT.TestContext.class, ReminderSaga.class},
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class SagaDbSchedulerDeadlineAutoConfigurationIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Autowired
    private ApplicationContext context;

    @Test
    void aDeadlineScheduledByADiscoveredSagaReachesItsDeadlineHandler() {
        // given
        assertThat(context.getBean(DeadlineManager.class)).isInstanceOf(DbSchedulerDeadlineManager.class);
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

        @Bean(destroyMethod = "stop")
        public Scheduler scheduler(List<Task<?>> tasks) {
            JDBCDataSource dataSource = new JDBCDataSource();
            dataSource.setUrl("jdbc:hsqldb:mem:sagaDbSchedulerDeadline");
            dataSource.setUser("sa");
            reCreateTable(dataSource);
            Scheduler scheduler = new SchedulerBuilder(dataSource, tasks).pollingInterval(Duration.ofMillis(50))
                                                                         .build();
            scheduler.start();
            return scheduler;
        }
    }
}
