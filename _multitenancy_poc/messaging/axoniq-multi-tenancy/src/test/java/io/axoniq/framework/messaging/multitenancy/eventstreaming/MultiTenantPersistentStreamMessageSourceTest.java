/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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
package io.axoniq.framework.messaging.multitenancy.eventstreaming;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamMessageSource;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantPersistentStreamMessageSourceFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiTenantPersistentStreamMessageSourceTest {

    @Mock
    private Configuration configuration;

    @Mock
    private ScheduledExecutorService scheduler;

    @Mock
    private PersistentStreamMessageSource tenantSource;

    @Mock
    private Registration tenantRegistration;

    private final List<RecordingCall> calls = new ArrayList<>();

    private MultiTenantPersistentStreamMessageSource source;

    @BeforeEach
    void setUp() {
        TenantPersistentStreamMessageSourceFactory factory = (name,
                                                             persistentStreamProperties,
                                                             scheduler,
                                                             batchSize,
                                                             context,
                                                             configuration,
                                                             tenantDescriptor) -> {
            calls.add(new RecordingCall(name, batchSize, context, tenantDescriptor.tenantId(), persistentStreamProperties));
            return tenantSource;
        };

        source = new MultiTenantPersistentStreamMessageSource(
                "stream",
                new PersistentStreamProperties("stream", 1, "example", Collections.emptyList(), "HEAD", null),
                scheduler,
                32,
                null,
                configuration,
                factory
        );
    }

    @Test
    void registerAndStartTenantSubscribesWhenConsumerIsAlreadyPresent() {
        // given
        BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer = mock(BiFunction.class);
        when(tenantSource.subscribe(eq(consumer))).thenReturn(tenantRegistration);
        source.subscribe(consumer);

        // when
        Registration registration = source.registerAndStartTenant(TenantDescriptor.tenantWithId("foo-a"));

        // then
        assertThat(calls).hasSize(1);
        assertThat(calls.getFirst().name()).isEqualTo("stream@foo-a");
        assertThat(calls.getFirst().context()).isEqualTo("foo-a");
        assertThat(source.tenantSegments()).containsKey(TenantDescriptor.tenantWithId("foo-a"));
        verify(tenantSource, times(1)).subscribe(consumer);

        // cleanup
        registration.cancel();
    }

    @Test
    void cancelSubscriptionReleasesTenantSubscriptionsAndAllowsResubscribe() {
        // given
        BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> consumer = mock(BiFunction.class);
        when(tenantSource.subscribe(eq(consumer))).thenReturn(tenantRegistration);
        source.subscribe(consumer);
        Registration tenantRegistrationHandle = source.registerAndStartTenant(TenantDescriptor.tenantWithId("foo-a"));

        // when
        boolean canceled = tenantRegistrationHandle.cancel();

        // then
        assertThat(canceled).isTrue();
        verify(tenantRegistration).cancel();
        assertThat(source.tenantSegments()).isEmpty();

        // when
        source.subscribe(consumer);
        source.registerAndStartTenant(TenantDescriptor.tenantWithId("foo-a"));

        // then
        verify(tenantSource, times(2)).subscribe(consumer);
    }

    private record RecordingCall(String name,
                                 int batchSize,
                                 String context,
                                 String tenantId,
                                 PersistentStreamProperties properties) {
    }
}
