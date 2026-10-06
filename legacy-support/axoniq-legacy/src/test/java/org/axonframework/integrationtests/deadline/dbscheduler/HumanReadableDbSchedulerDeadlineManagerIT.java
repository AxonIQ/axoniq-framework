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

package org.axonframework.integrationtests.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.Scheduler;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.AbstractDeadlineManager;
import org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineManager;
import org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineManagerSupplier;
import org.axonframework.integrationtests.deadline.AbstractDeadlineManagerTestSuite;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.axonframework.common.util.DbSchedulerTestUtil.getScheduler;
import static org.axonframework.common.util.DbSchedulerTestUtil.reCreateTable;

/**
 * Runs the {@link AbstractDeadlineManagerTestSuite} against the {@link DbSchedulerDeadlineManager} storing
 * human-readable task data, on an in-memory HSQL database.
 */
class HumanReadableDbSchedulerDeadlineManagerIT extends AbstractDeadlineManagerTestSuite {

    private Scheduler scheduler;

    @Override
    protected AbstractDeadlineManager buildDeadlineManager(ScopeAwareProvider scopeAwareProvider,
                                                           UnitOfWorkFactory unitOfWorkFactory) {
        JDBCDataSource dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:humanReadableDeadlines");
        dataSource.setUser("sa");
        reCreateTable(dataSource);
        DbSchedulerDeadlineManagerSupplier supplier = new DbSchedulerDeadlineManagerSupplier();
        scheduler = getScheduler(dataSource, DbSchedulerDeadlineManager.humanReadableTask(supplier));
        DbSchedulerDeadlineManager manager = DbSchedulerDeadlineManager.builder()
                                                                       .scheduler(scheduler)
                                                                       .scopeAwareProvider(scopeAwareProvider)
                                                                       .unitOfWorkFactory(unitOfWorkFactory)
                                                                       .converter(new JacksonConverter())
                                                                       .useBinaryPojo(false)
                                                                       .build();
        supplier.set(manager);
        manager.start();
        return manager;
    }

    @Test
    void aFailingDeliveryLeavesTheTaskForALaterRetry() {
        // given
        scopeAware.failWith(new IllegalStateException("delivery failure"));

        // when
        deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

        // then
        await().atMost(FIRING_TIMEOUT).untilAsserted(
                () -> assertThat(scheduler.getFailingExecutions(Duration.ofHours(1))).hasSize(1)
        );
        assertThat(scopeAware.deliveries()).isEmpty();
    }
}
