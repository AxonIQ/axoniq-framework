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

package org.axonframework.integrationtests.deadline;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.DeadlineManagerSpanFactory;
import org.axonframework.deadline.SimpleDeadlineManager;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.ScopeAwareProvider;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

@Disabled("TODO #3065 - Revisit Deadline support")
@ExtendWith(MockitoExtension.class)
class SimpleDeadlineManagerTest extends AbstractDeadlineManagerTestSuite {

    @Override
    public DeadlineManager buildDeadlineManager(Configuration configuration) {
        return SimpleDeadlineManager.builder()
//                                    .scopeAwareProvider(new ConfigurationScopeAwareProvider(configuration))
                                    .spanFactory(configuration.getComponent(DeadlineManagerSpanFactory.class))
                                    .messageNameResolver(new ClassBasedMessageTypeResolver())
                                    .build();
    }

    @Test
    void shutdownInvokesExecutorServiceShutdown(@Mock ScopeAwareProvider scopeAwareProvider,
                                                @Mock ScheduledExecutorService scheduledExecutorService) {
        SimpleDeadlineManager testSubject = SimpleDeadlineManager.builder()
                                                                 .scopeAwareProvider(scopeAwareProvider)
                                                                 .scheduledExecutorService(scheduledExecutorService)
                                                                 .build();

        testSubject.shutdown();

        verify(scheduledExecutorService).shutdown();
    }
}
