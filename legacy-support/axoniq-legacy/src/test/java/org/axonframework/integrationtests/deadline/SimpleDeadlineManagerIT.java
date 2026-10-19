/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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

    @Override
    protected boolean keepsTheTimestampOfTheScheduledMessage() {
        return false;
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
