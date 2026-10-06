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

package org.axonframework.integrationtests.deadline;

import org.axonframework.deadline.AbstractDeadlineManager;
import org.axonframework.deadline.SimpleDeadlineManager;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs the {@link AbstractDeadlineManagerTestSuite} against the {@link SimpleDeadlineManager}.
 */
class SimpleDeadlineManagerIT extends AbstractDeadlineManagerTestSuite {

    @Override
    protected AbstractDeadlineManager buildDeadlineManager(ScopeAwareProvider scopeAwareProvider,
                                                           UnitOfWorkFactory unitOfWorkFactory) {
        return SimpleDeadlineManager.builder()
                                    .scopeAwareProvider(scopeAwareProvider)
                                    .unitOfWorkFactory(unitOfWorkFactory)
                                    .build();
    }

    @Test
    void aFailingDeliveryIsNotRetried() {
        // given
        scopeAware.failWith(new IllegalStateException("delivery failure"));

        // when
        deadlineManager.schedule(TRIGGER_DURATION, DEADLINE_NAME, "payload", SAGA_SCOPE);

        // then
        await().atMost(FIRING_TIMEOUT).until(() -> !scopeAware.attempts().isEmpty());
        await().during(NOT_FIRING_PERIOD).atMost(NOT_FIRING_PERIOD.plusSeconds(1))
               .until(() -> scopeAware.attempts().size() == 1);
        assertThat(scopeAware.deliveries()).isEmpty();
    }
}
