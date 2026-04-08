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

package org.axonframework.messaging.eventhandling.scheduling.dbscheduler;

import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;
import static org.axonframework.common.util.DbSchedulerTestUtil.getScheduler;
import static org.junit.jupiter.api.Assertions.*;

class HumanReadableDbSchedulerEventSchedulerTest extends AbstractDbSchedulerEventSchedulerTest {

    @Override
    Task<?> getTask(Supplier<DbSchedulerEventScheduler> eventSchedulerSupplier) {
        return DbSchedulerEventScheduler.humanReadableTask(eventSchedulerSupplier);
    }

    @Override
    boolean useBinaryPojo() {
        return false;
    }

    @Test
    void whenNotInitializedThrow() {
        eventScheduler.shutdown();
        DbSchedulerEventSchedulerSupplier supplier = new DbSchedulerEventSchedulerSupplier();
        scheduler = getScheduler(dataSource, getTask(supplier));
        scheduler.start();
        try {
            TaskInstance<DbSchedulerHumanReadableEventData> instance =
                    DbSchedulerEventScheduler.humanReadableTask(supplier)
                                             .instance("id", new DbSchedulerHumanReadableEventData());
            scheduler.schedule(instance, Instant.now());
            await().atMost(Duration.ofSeconds(1L)).untilAsserted(
                    () -> {
                        List<Execution> failures = scheduler.getFailingExecutions(Duration.ofHours(1L));
                        assertEquals(1, failures.size());
                        assertNotNull(failures.get(0).lastFailure);
                    }
            );
        } finally {
            scheduler.stop();
        }
    }
}
