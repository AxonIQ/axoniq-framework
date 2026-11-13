/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TenantRoutingSequencedDeadLetterQueueTest {

    @Test
    void enqueuesIntoTheQueueCreatedForTheTenantOnTheProcessingContext() {
        StubTenantProvider tenantProvider = new StubTenantProvider();
        tenantProvider.addTenant(TENANT_A);
        Configuration configuration = mock(Configuration.class);
        ProcessingContext context = mock(ProcessingContext.class);
        @SuppressWarnings("unchecked")
        SequencedDeadLetterQueue<EventMessage> tenantQueue = mock(SequencedDeadLetterQueue.class);
        @SuppressWarnings("unchecked")
        DeadLetter<EventMessage> letter = mock(DeadLetter.class);
        TenantAwareSequencedDeadLetterQueueFactory factory = (tenant, processorName, ignored) -> tenantQueue;
        when(context.getResource(TenantDescriptor.RESOURCE_KEY)).thenReturn(TENANT_A);
        when(tenantQueue.enqueue(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        TenantRoutingSequencedDeadLetterQueueRegistry registry = new TenantRoutingSequencedDeadLetterQueueRegistry();
        tenantProvider.subscribe(registry);

        TenantRoutingSequencedDeadLetterQueue testSubject = new TenantRoutingSequencedDeadLetterQueue(
                "DeadLetterQueue[projection]", configuration, factory, registry
        );

        testSubject.enqueue("account-1", letter, context).join();

        verify(tenantQueue).enqueue("account-1", letter, context);
    }

    @Test
    void rejectsProcessingWithoutATenantCarryingContext() {
        TenantRoutingSequencedDeadLetterQueue testSubject = new TenantRoutingSequencedDeadLetterQueue(
                "DeadLetterQueue[projection]", mock(Configuration.class), (tenant, processorName, configuration) -> null,
                new TenantRoutingSequencedDeadLetterQueueRegistry()
        );

        CompletableFuture<Boolean> result = testSubject.process(
                letter -> true, letter -> CompletableFuture.completedFuture(null), null
        );

        assertThat(result).isCompletedExceptionally();
        assertThat(result.handle((ignored, exception) -> exception.getCause()))
                .isCompletedWithValueMatching(TenantNotResolvedException.class::isInstance);
    }

    @Test
    void createsAFreshQueueWhenATenantIsReAdded() {
        StubTenantProvider tenantProvider = new StubTenantProvider();
        tenantProvider.addTenant(TENANT_A);
        Configuration configuration = mock(Configuration.class);
        ProcessingContext context = mock(ProcessingContext.class);
        @SuppressWarnings("unchecked")
        SequencedDeadLetterQueue<EventMessage> firstQueue = mock(SequencedDeadLetterQueue.class);
        @SuppressWarnings("unchecked")
        SequencedDeadLetterQueue<EventMessage> reAddedTenantQueue = mock(SequencedDeadLetterQueue.class);
        AtomicInteger creations = new AtomicInteger();
        TenantAwareSequencedDeadLetterQueueFactory factory = (tenant, processorName, ignored) ->
                creations.getAndIncrement() == 0 ? firstQueue : reAddedTenantQueue;
        when(context.getResource(TenantDescriptor.RESOURCE_KEY)).thenReturn(TENANT_A);
        when(firstQueue.size(context)).thenReturn(CompletableFuture.completedFuture(1L));
        when(reAddedTenantQueue.size(context)).thenReturn(CompletableFuture.completedFuture(2L));
        TenantRoutingSequencedDeadLetterQueueRegistry registry = new TenantRoutingSequencedDeadLetterQueueRegistry();
        tenantProvider.subscribe(registry);

        TenantRoutingSequencedDeadLetterQueue testSubject = new TenantRoutingSequencedDeadLetterQueue(
                "DeadLetterQueue[projection]", configuration, factory, registry
        );

        testSubject.size(context).join();
        tenantProvider.removeTenant(TENANT_A);
        tenantProvider.addTenant(TENANT_A);
        testSubject.size(context).join();

        verify(firstQueue).size(context);
        verify(reAddedTenantQueue).size(context);
    }
}
