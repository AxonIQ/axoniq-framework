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

package org.axonframework.messaging.core.unitofwork;

import org.axonframework.messaging.core.EmptyApplicationContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link SimpleUnitOfWorkFactory}.
 *
 * @author John Hendrikx
 */
class SimpleUnitOfWorkFactoryTest {

    @Test
    void shouldEnhanceUnitOfWorkProcessingLifecycle() {
        AtomicBoolean enhancerCalled = new AtomicBoolean();
        AtomicBoolean preInvocationCalled = new AtomicBoolean();

        SimpleUnitOfWorkFactory factory = new SimpleUnitOfWorkFactory(
            EmptyApplicationContext.INSTANCE,
            c -> c.registerProcessingLifecycleEnhancer(pl -> {
                enhancerCalled.set(true);
                pl.runOnPreInvocation(pc -> preInvocationCalled.set(true));
            })
        );

        UnitOfWork unitOfWork = factory.create();

        assertThat(enhancerCalled).isTrue();
        assertThat(preInvocationCalled).isFalse();

        unitOfWork.execute();

        assertThat(preInvocationCalled).isTrue();
    }

}
