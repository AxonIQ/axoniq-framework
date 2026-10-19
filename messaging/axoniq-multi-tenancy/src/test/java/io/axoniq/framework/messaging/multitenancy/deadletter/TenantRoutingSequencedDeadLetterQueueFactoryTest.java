/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TenantRoutingSequencedDeadLetterQueueFactoryTest {

    @SuppressWarnings("unchecked")
    private static SequencedDeadLetterQueue<EventMessage> queue() {
        return mock(SequencedDeadLetterQueue.class);
    }

    @Test
    void createsARoutingQueueWithoutEagerlyCreatingATenantQueue() {
        // given
        TenantAwareSequencedDeadLetterQueueFactory delegate = mock(
                TenantAwareSequencedDeadLetterQueueFactory.class
        );
        TenantRoutingSequencedDeadLetterQueueFactory testSubject = new TenantRoutingSequencedDeadLetterQueueFactory(
                delegate, new TenantRoutingSequencedDeadLetterQueueRegistry()
        );

        // when
        SequencedDeadLetterQueue<EventMessage> result = testSubject.create("projection", mock(Configuration.class));

        // then
        assertThat(result).isInstanceOf(TenantRoutingSequencedDeadLetterQueue.class);
        verifyNoInteractions(delegate);
    }

    @Test
    void createsAQueueThatRoutesOperationsToTheTenantQueue() {
        // given
        StubTenantProvider tenantProvider = new StubTenantProvider();
        tenantProvider.addTenant(TENANT_A);
        TenantRoutingSequencedDeadLetterQueueRegistry registry =
                new TenantRoutingSequencedDeadLetterQueueRegistry();
        tenantProvider.subscribe(registry);
        Configuration configuration = mock(Configuration.class);
        ProcessingContext context = mock(ProcessingContext.class);
        TenantAwareSequencedDeadLetterQueueFactory delegate = mock(
                TenantAwareSequencedDeadLetterQueueFactory.class
        );
        SequencedDeadLetterQueue<EventMessage> tenantQueue = queue();
        when(context.getResource(TenantDescriptor.RESOURCE_KEY)).thenReturn(TENANT_A);
        when(delegate.create(TENANT_A, "projection", configuration)).thenReturn(tenantQueue);
        when(tenantQueue.size(context)).thenReturn(CompletableFuture.completedFuture(1L));
        TenantRoutingSequencedDeadLetterQueueFactory testSubject = new TenantRoutingSequencedDeadLetterQueueFactory(
                delegate, registry
        );

        // when
        CompletableFuture<Long> result = testSubject.create("projection", configuration).size(context);

        // then
        assertThat(result).isCompletedWithValue(1L);
        verify(delegate).create(TENANT_A, "projection", configuration);
        verify(tenantQueue).size(context);
    }
}
