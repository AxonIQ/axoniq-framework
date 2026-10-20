/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerBuilder;
import com.github.kagkarlsson.scheduler.event.AbstractSchedulerListener;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;

import static org.awaitility.Awaitility.await;
import static org.axonframework.common.util.DbSchedulerTestUtil.reCreateTable;
import static org.assertj.core.api.Assertions.assertThat;

@ContextConfiguration
@ExtendWith(SpringExtension.class)
class DbSchedulerDeadlineManagerTest {

    @Autowired
    protected DataSource dataSource;

    @BeforeEach
    void prepare() {
        reCreateTable(dataSource);
    }

    @Test
    void aBinaryTaskFailsWithDeadlineManagerNotSuppliedExceptionWhenNoManagerWasSupplied() {
        // given
        DbSchedulerDeadlineManagerSupplier supplier = new DbSchedulerDeadlineManagerSupplier();
        Task<DbSchedulerBinaryDeadlineDetails> task = DbSchedulerDeadlineManager.binaryTask(supplier);

        // when
        List<Throwable> failures = runOnce(task, task.instance("id", new DbSchedulerBinaryDeadlineDetails()));

        // then
        assertThat(failures).singleElement().isInstanceOf(DeadlineManagerNotSuppliedException.class);
    }

    @Test
    void aHumanReadableTaskFailsWithDeadlineManagerNotSuppliedExceptionWhenNoManagerWasSupplied() {
        // given
        DbSchedulerDeadlineManagerSupplier supplier = new DbSchedulerDeadlineManagerSupplier();
        Task<DbSchedulerHumanReadableDeadlineDetails> task = DbSchedulerDeadlineManager.humanReadableTask(supplier);

        // when
        List<Throwable> failures =
                runOnce(task, task.instance("id", new DbSchedulerHumanReadableDeadlineDetails()));

        // then
        assertThat(failures).singleElement().isInstanceOf(DeadlineManagerNotSuppliedException.class);
    }

    /**
     * Schedules the given {@code instance} to run now on a scheduler with only the given {@code task}, and returns
     * the causes of its failed executions once one failed.
     */
    private <T> List<Throwable> runOnce(Task<T> task, TaskInstance<T> instance) {
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        Scheduler scheduler = new SchedulerBuilder(dataSource, List.of(task))
                .threads(2)
                .pollingInterval(Duration.ofMillis(50L))
                .addSchedulerListener(new AbstractSchedulerListener() {
                    @Override
                    public void onExecutionComplete(ExecutionComplete executionComplete) {
                        executionComplete.getCause().ifPresent(failures::add);
                    }
                })
                .build();
        scheduler.start();
        try {
            scheduler.schedule(instance, Instant.now());
            await().atMost(Duration.ofSeconds(5L)).until(() -> !failures.isEmpty());
            return failures;
        } finally {
            scheduler.stop();
        }
    }

    @Configuration
    public static class Context {

        @SuppressWarnings("Duplicates")
        @Bean
        public DataSource dataSource() {
            JDBCDataSource dataSource = new JDBCDataSource();
            dataSource.setUrl("jdbc:hsqldb:mem:testdb");
            dataSource.setUser("sa");
            dataSource.setPassword("");
            return dataSource;
        }
    }
}
