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
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.SimpleDeadlineManager;
import org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineManager;
import org.axonframework.deadline.jobrunr.JobRunrDeadlineManager;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.storage.InMemoryStorageProvider;
import org.jobrunr.utils.mapper.jackson3.Jackson3JsonMapper;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link LegacyJobRunrDeadlineManagerAutoConfiguration} and the
 * {@link LegacyDbSchedulerDeadlineManagerAutoConfiguration}.
 *
 * @author Jakob Hatzl
 */
class LegacyDeadlineManagerAutoConfigurationTest {

    private final ApplicationContextRunner testContext = new ApplicationContextRunner()
            .withPropertyValues("axon.axonserver.enabled=false")
            .withUserConfiguration(BaseContext.class);

    @Nested
    class JobRunr {

        @Test
        void configuresAJobRunrDeadlineManager() {
            testContext.withUserConfiguration(JobSchedulerContext.class, ScopeAwareProviderContext.class)
                       .run(context -> assertThat(context).hasSingleBean(DeadlineManager.class)
                                                          .hasSingleBean(JobRunrDeadlineManager.class));
        }

        @Test
        void storesDeadlinesWithTheEventConverter() {
            testContext.withUserConfiguration(JobSchedulerContext.class,
                                              ScopeAwareProviderContext.class,
                                              RecordingEventConverterContext.class)
                       .run(context -> {
                           // when
                           context.getBean(DeadlineManager.class).schedule(
                                   Duration.ofMinutes(5), "deadline", "payload", new SagaScopeDescriptor("Saga", "id")
                           );

                           // then
                           assertThat(context.getBean(RecordingEventConverter.class).conversions).isNotEmpty();
                       });
        }

        @Test
        void configuresNoManagerWithoutAScopeAwareProvider() {
            testContext.withUserConfiguration(JobSchedulerContext.class)
                       .run(context -> assertThat(context).doesNotHaveBean(DeadlineManager.class));
        }

        @Test
        void configuresNoManagerWithoutAJobScheduler() {
            testContext.withUserConfiguration(ScopeAwareProviderContext.class)
                       .run(context -> assertThat(context).doesNotHaveBean(DeadlineManager.class));
        }

        @Test
        void aDeadlineManagerOfTheApplicationWins() {
            testContext.withUserConfiguration(JobSchedulerContext.class,
                                              ScopeAwareProviderContext.class,
                                              CustomDeadlineManagerContext.class)
                       .run(context -> assertThat(context).hasSingleBean(DeadlineManager.class)
                                                          .doesNotHaveBean(JobRunrDeadlineManager.class));
        }
    }

    @Nested
    class DbScheduler {

        @Test
        void configuresADbSchedulerDeadlineManagerAndItsTask() {
            testContext.withUserConfiguration(DbSchedulerContext.class, ScopeAwareProviderContext.class)
                       .run(context -> {
                           assertThat(context).hasSingleBean(DeadlineManager.class)
                                              .hasSingleBean(DbSchedulerDeadlineManager.class)
                                              .hasBean("deadlineDetailsTask");
                           assertThat(context.getBean("deadlineDetailsTask", Task.class).getName())
                                   .isEqualTo("AxonDeadline");
                       });
        }

        @Test
        void configuresNoManagerWithoutAScopeAwareProvider() {
            testContext.withUserConfiguration(DbSchedulerContext.class)
                       .run(context -> assertThat(context).doesNotHaveBean(DeadlineManager.class));
        }

        @Test
        void aDeadlineManagerOfTheApplicationWins() {
            testContext.withUserConfiguration(DbSchedulerContext.class,
                                              ScopeAwareProviderContext.class,
                                              CustomDeadlineManagerContext.class)
                       .run(context -> assertThat(context).hasSingleBean(DeadlineManager.class)
                                                          .doesNotHaveBean(DbSchedulerDeadlineManager.class));
        }
    }

    @Configuration
    @EnableAutoConfiguration(exclude = {HibernateJpaAutoConfiguration.class, JpaRepositoriesAutoConfiguration.class})
    static class BaseContext {

        @Bean
        public DataSource dataSource() {
            JDBCDataSource dataSource = new JDBCDataSource();
            dataSource.setUrl("jdbc:hsqldb:mem:deadlineAutoConfiguration");
            dataSource.setUser("sa");
            return dataSource;
        }
    }

    @Configuration
    static class JobSchedulerContext {

        @Bean
        public JobScheduler jobScheduler() {
            InMemoryStorageProvider storageProvider = new InMemoryStorageProvider();
            storageProvider.setJobMapper(new JobMapper(new Jackson3JsonMapper()));
            return new JobScheduler(storageProvider);
        }
    }

    @Configuration
    static class DbSchedulerContext {

        @Bean
        public Scheduler scheduler(DataSource dataSource, List<Task<?>> tasks) {
            return new SchedulerBuilder(dataSource, tasks).build();
        }
    }

    @Configuration
    static class ScopeAwareProviderContext {

        @Bean
        public ScopeAwareProvider scopeAwareProvider() {
            return scope -> Stream.empty();
        }
    }

    @Configuration
    static class CustomDeadlineManagerContext {

        @Bean
        public DeadlineManager customDeadlineManager(ScopeAwareProvider scopeAwareProvider) {
            return SimpleDeadlineManager.builder()
                                        .scopeAwareProvider(scopeAwareProvider)
                                        .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                        .build();
        }
    }

    @Configuration
    static class RecordingEventConverterContext {

        @Bean
        public RecordingEventConverter eventConverter() {
            return new RecordingEventConverter();
        }
    }

    /**
     * An {@link EventConverter} converting through a {@link JacksonConverter}, recording the conversions it is asked
     * for.
     */
    static class RecordingEventConverter extends DelegatingEventConverter {

        private final List<Type> conversions = new CopyOnWriteArrayList<>();

        RecordingEventConverter() {
            super(new JacksonConverter());
        }

        @Override
        public <T> @Nullable T convert(@Nullable Object input, Type targetType) {
            conversions.add(targetType);
            return super.convert(input, targetType);
        }
    }
}
