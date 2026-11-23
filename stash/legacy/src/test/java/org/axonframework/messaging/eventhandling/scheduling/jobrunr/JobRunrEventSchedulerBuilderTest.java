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

package org.axonframework.messaging.eventhandling.scheduling.jobrunr;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.eventhandling.EventBus;
import org.axonframework.conversion.json.JacksonSerializer;
import org.jobrunr.scheduling.JobScheduler;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobRunrEventSchedulerBuilderTest {

    private JobRunrEventScheduler.Builder builder;
    private final JobScheduler jobScheduler = mock(JobScheduler.class);
    private final TransactionManager transactionManager = mock(TransactionManager.class);
    private final EventBus eventBus = mock(EventBus.class);

    @BeforeEach
    void newBuilder() {
        builder = JobRunrEventScheduler.builder();
    }

    @Test
    void whenAllPropertiesAreSetCreatesManager() {
        JobRunrEventScheduler scheduler = builder.transactionManager(transactionManager)
                                                 .jobScheduler(jobScheduler)
                                                 .serializer(JacksonSerializer.defaultSerializer())
                                                 .eventBus(eventBus)
                                                 .build();

        assertNotNull(scheduler);
    }

    @Test
    void validateNeedsAllPropertiesSet() {
        builder.jobScheduler(jobScheduler)
               .transactionManager(transactionManager);
        assertThrows(AxonConfigurationException.class, () -> builder.build());
    }

    @Test
    void whenSettingSchedulerWithNullThrowError() {
        assertThrows(AxonConfigurationException.class, () -> builder.jobScheduler(null));
    }

    @Test
    void whenSettingNameWithNullThrowError() {
        assertThrows(AxonConfigurationException.class, () -> builder.jobName(null));
    }

    @Test
    void whenSettingTransactionManagerWithNullThrowError() {
        assertThrows(AxonConfigurationException.class, () -> builder.transactionManager(null));
    }

    @Test
    void whenSettingSerializerWithNullThrowError() {
        assertThrows(AxonConfigurationException.class, () -> builder.serializer(null));
    }

    @Test
    void whenSettingEventBusWithNullThrowError() {
        assertThrows(AxonConfigurationException.class, () -> builder.eventBus(null));
    }
}
