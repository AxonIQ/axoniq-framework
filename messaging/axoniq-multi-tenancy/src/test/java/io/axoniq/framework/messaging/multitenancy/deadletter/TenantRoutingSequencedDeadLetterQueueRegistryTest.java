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

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.common.Registration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TenantRoutingSequencedDeadLetterQueueRegistryTest {

    private final Configuration configuration = mock(Configuration.class);
    private final TenantRoutingSequencedDeadLetterQueueRegistry testSubject =
            new TenantRoutingSequencedDeadLetterQueueRegistry();

    @SuppressWarnings("unchecked")
    private static SequencedDeadLetterQueue<EventMessage> queue() {
        return mock(SequencedDeadLetterQueue.class);
    }

    @Nested
    class QueueFor {

        @Test
        void createsAQueueForATenantRegisteredBeforeTheQueueCacheExists() {
            // given
            TenantAwareSequencedDeadLetterQueueFactory factory = mock(TenantAwareSequencedDeadLetterQueueFactory.class);
            SequencedDeadLetterQueue<EventMessage> tenantQueue = queue();
            testSubject.registerTenant(TENANT_A);
            when(factory.create(TENANT_A, "first", configuration)).thenReturn(tenantQueue);

            // when
            SequencedDeadLetterQueue<EventMessage> firstLookup = testSubject.queueFor(
                    "first", configuration, factory, TENANT_A
            );
            SequencedDeadLetterQueue<EventMessage> secondLookup = testSubject.queueFor(
                    "first", configuration, factory, TENANT_A
            );

            // then
            assertThat(firstLookup).isSameAs(tenantQueue);
            assertThat(secondLookup).isSameAs(tenantQueue);
            verify(factory).create(TENANT_A, "first", configuration);
        }

        @Test
        void createsFreshQueuesForEveryProcessingGroupAfterTenantRemoval() {
            // given
            TenantAwareSequencedDeadLetterQueueFactory firstFactory = mock(TenantAwareSequencedDeadLetterQueueFactory.class);
            TenantAwareSequencedDeadLetterQueueFactory secondFactory = mock(TenantAwareSequencedDeadLetterQueueFactory.class);
            SequencedDeadLetterQueue<EventMessage> firstQueue = queue();
            SequencedDeadLetterQueue<EventMessage> firstQueueAfterReAdding = queue();
            SequencedDeadLetterQueue<EventMessage> secondQueue = queue();
            SequencedDeadLetterQueue<EventMessage> secondQueueAfterReAdding = queue();
            when(firstFactory.create(TENANT_A, "first", configuration)).thenReturn(firstQueue, firstQueueAfterReAdding);
            when(secondFactory.create(TENANT_A, "second", configuration)).thenReturn(secondQueue, secondQueueAfterReAdding);
            Registration registration = testSubject.registerTenant(TENANT_A);
            testSubject.queueFor("first", configuration, firstFactory, TENANT_A);
            testSubject.queueFor("second", configuration, secondFactory, TENANT_A);

            // when
            registration.cancel();
            testSubject.registerTenant(TENANT_A);
            SequencedDeadLetterQueue<EventMessage> firstLookup = testSubject.queueFor(
                    "first", configuration, firstFactory, TENANT_A
            );
            SequencedDeadLetterQueue<EventMessage> secondLookup = testSubject.queueFor(
                    "second", configuration, secondFactory, TENANT_A
            );

            // then
            assertThat(firstLookup).isSameAs(firstQueueAfterReAdding);
            assertThat(secondLookup).isSameAs(secondQueueAfterReAdding);
        }
    }
}
