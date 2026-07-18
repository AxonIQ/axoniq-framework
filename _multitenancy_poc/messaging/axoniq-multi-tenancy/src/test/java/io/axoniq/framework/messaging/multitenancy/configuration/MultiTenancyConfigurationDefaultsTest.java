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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantRoutingEventStore;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.pooled.MultiTenantPooledStreamingEventProcessorModule;
import org.axonframework.common.Registration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.DefaultComponentRegistry;
import org.axonframework.common.configuration.StubLifecycleRegistry;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

class MultiTenancyConfigurationDefaultsTest {

    @Nested
    class ParameterResolverRegistration {

        @Test
        void resolvesTenantScopedComponentsFromMessageMetadata() throws Exception {
            DefaultComponentRegistry registry = new DefaultComponentRegistry();
            registry.disableEnhancerScanning();
            registry.registerComponent(TenantComponentRegistry.class, cfg ->
                    new DefaultTenantComponentRegistry<>(
                            StringBuilder.class,
                            tenant -> new StringBuilder("repo-" + tenant.tenantId())
                    )
            );
            registry.registerEnhancer(new MultiTenancyConfigurationDefaults());

            Configuration configuration = registry.build(new StubLifecycleRegistry());
            ParameterResolverFactory parameterResolverFactory = configuration.getComponent(ParameterResolverFactory.class);

            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            ParameterResolver<?> resolver = parameterResolverFactory.createInstance(method, method.getParameters(), 1);

            ProcessingContext context = StubProcessingContext.forMessage(
                    new GenericMessage(
                            new MessageType("TestCommand"),
                            "payload",
                            Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY, "foo-a")
                    )
            );

            Object value = resolver.resolveParameterValue(context).orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();

            assertThat(value).isInstanceOf(StringBuilder.class);
            assertThat(value.toString()).isEqualTo("repo-foo-a");
        }
    }

    @Test
    void resolvesTenantRoutingEventStoreBehindDelegatingWrappers() throws Exception {
        TenantDescriptor tenant = TenantDescriptor.tenantWithId("foo-a");
        RecordingEventStore tenantStore = new RecordingEventStore();
        TenantRoutingEventStore routingEventStore = new TenantRoutingEventStore(t -> tenantStore, (message, tenants) -> tenant);
        routingEventStore.registerTenant(tenant);

        EventStore wrapped = new DelegatingEventStore(new DelegatingEventStore(routingEventStore));

        Method method = MultiTenantPooledStreamingEventProcessorModule.class
                .getDeclaredMethod("resolveTenantRoutingEventStore", Object.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Optional<TenantRoutingEventStore> resolved = (Optional<TenantRoutingEventStore>) method.invoke(null, wrapped);

        assertThat(resolved).isPresent();
        assertThat(resolved).get().isSameAs(routingEventStore);
    }

    private static class SampleHandler {

        @SuppressWarnings("unused")
        public void handle(String payload, StringBuilder repository) {
            // no-op
        }
    }

    private static final class DelegatingEventStore implements EventStore {

        private final EventStore delegate;

        private DelegatingEventStore(EventStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Void> publish(ProcessingContext context, List<? extends EventMessage> events) {
            return delegate.publish(context, events);
        }

        @Override
        public Registration subscribe(
                BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer) {
            return delegate.subscribe(eventsBatchConsumer);
        }

        @Override
        public MessageStream<EventMessage> open(StreamingCondition condition, ProcessingContext context) {
            return delegate.open(condition, context);
        }

        @Override
        public EventStoreTransaction transaction(ProcessingContext processingContext) {
            return delegate.transaction(processingContext);
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> firstToken(
                ProcessingContext context
        ) {
            return delegate.firstToken(context);
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> latestToken(
                ProcessingContext context
        ) {
            return delegate.latestToken(context);
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> tokenAt(
                Instant at,
                ProcessingContext context
        ) {
            return delegate.tokenAt(at, context);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeWrapperOf(delegate);
        }
    }

    private static final class RecordingEventStore implements EventStore {

        @Override
        public CompletableFuture<Void> publish(ProcessingContext context, List<? extends EventMessage> events) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Registration subscribe(
                BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer) {
            return () -> true;
        }

        @Override
        public MessageStream<EventMessage> open(StreamingCondition condition, ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EventStoreTransaction transaction(ProcessingContext processingContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> firstToken(
                ProcessingContext context
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> latestToken(
                ProcessingContext context
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken> tokenAt(
                Instant at,
                ProcessingContext context
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // no-op
        }
    }
}
