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

package org.axonframework.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.Scheduler;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.ScopeAwareProvider;
import org.axonframework.conversion.json.JacksonSerializer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DbSchedulerDeadlineManagerBuilderTest {

    private DbSchedulerDeadlineManager.Builder builder;
    private final Scheduler scheduler = mock(Scheduler.class);
    private final TransactionManager transactionManager = mock(TransactionManager.class);
    private final ScopeAwareProvider scopeAwareProvider = mock(ScopeAwareProvider.class);

    @BeforeEach
    void newBuilder() {
        builder = DbSchedulerDeadlineManager.builder();
    }

    @Test
    void whenAllPropertiesAreSetCreatesManager() {
        DbSchedulerDeadlineManager manager = builder.scopeAwareProvider(scopeAwareProvider)
                                                    .transactionManager(transactionManager)
                                                    .scheduler(scheduler)
                                                    .serializer(JacksonSerializer.defaultSerializer())
                                                    .build();

        assertNotNull(manager);
    }

    @Test
    void validateNeedsAllPropertiesSet() {
        builder.scheduler(scheduler)
               .transactionManager(transactionManager);
        assertThrows(AxonConfigurationException.class, () -> builder.build());
    }

    @Test
    void whenSettingSchedulerWithNullThrowError() {
        assertThrows(AxonConfigurationException.class, () -> builder.scheduler(null));
    }

    @Test
    void whenSettingTransactionManagerWithNullThrowError() {
        assertThrows(AxonConfigurationException.class, () -> builder.transactionManager(null));
    }

    @Test
    void whenSettingScopeAwareProviderWithNullThrowError() {
        assertThrows(AxonConfigurationException.class, () -> builder.scopeAwareProvider(null));
    }
}
