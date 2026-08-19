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
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TenantRoutingSequencedDeadLetterQueueFactoryTest {

    @SuppressWarnings("unchecked")
    private static SequencedDeadLetterQueue<EventMessage> queue() {
        return mock(SequencedDeadLetterQueue.class);
    }

    @Test
    void createsAQueueThatRoutesOperationsToTheTenantQueue() {
        // given
        StubTenantProvider tenantProvider = new StubTenantProvider();
        tenantProvider.addTenant(TENANT_A);
        TenantRoutingSequencedDeadLetterQueueRegistry registry = new TenantRoutingSequencedDeadLetterQueueRegistry();
        tenantProvider.subscribe(registry);
        Configuration configuration = mock(Configuration.class);
        ProcessingContext context = mock(ProcessingContext.class);
        SequencedDeadLetterQueueFactory delegate = mock(SequencedDeadLetterQueueFactory.class);
        SequencedDeadLetterQueue<EventMessage> tenantQueue = queue();
        when(context.getResource(TenantDescriptor.RESOURCE_KEY)).thenReturn(TENANT_A);
        when(delegate.create("projection", configuration)).thenReturn(tenantQueue);
        when(tenantQueue.size(context)).thenReturn(CompletableFuture.completedFuture(1L));
        TenantRoutingSequencedDeadLetterQueueFactory testSubject =
                new TenantRoutingSequencedDeadLetterQueueFactory(delegate, registry);

        // when
        CompletableFuture<Long> result = testSubject.create("projection", configuration).size(context);

        // then
        assertThat(result).isCompletedWithValue(1L);
        verify(delegate).create("projection", configuration);
        verify(tenantQueue).size(context);
    }
}
