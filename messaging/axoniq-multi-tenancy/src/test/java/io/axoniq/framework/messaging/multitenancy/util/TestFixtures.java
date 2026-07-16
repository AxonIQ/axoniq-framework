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

package io.axoniq.framework.messaging.multitenancy.util;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.test.fixture.RecordingEventStore;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_REPLICATION_GROUP;
import static io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor.tenantWithId;

/**
 * Utility class providing test fixtures for multi-tenancy related tests.
 */
public enum TestFixtures {
    ;

    public static final TenantDescriptor TENANT_A = new TenantDescriptor(
            "foo-a",
            Map.of("replicationGroup", DEFAULT_REPLICATION_GROUP)
    );

    public static final TenantDescriptor TENANT_B = new TenantDescriptor(
            "foo-b",
            Map.of("replicationGroup", DEFAULT_REPLICATION_GROUP)
    );

    /**
     * Creates a {@link TenantResolver} that always resolves to the given tenant ID.
     *
     * @param tenantId the tenant ID to resolve to
     * @return a {@link TenantResolver} that always resolves to the given tenant ID
     * @see #alwaysTenant(TenantDescriptor)
     */
    public static TenantResolver alwaysTenant(String tenantId) {
        return alwaysTenant(tenantWithId(tenantId));
    }

    /**
     * Creates a {@link TenantResolver} that always resolves to the given {@link TenantDescriptor}.
     *
     * @param tenantDescriptor the {@link TenantDescriptor} to resolve to
     * @return a {@link TenantResolver} that always resolves to the given {@link TenantDescriptor}
     */
    public static TenantResolver alwaysTenant(TenantDescriptor tenantDescriptor) {
        return (message, tenants) -> tenantDescriptor;
    }

    /**
     * Creates a {@link RecordingEventStore} that uses a no-op {@link EventStore} implementation for all operations.
     *
     * @return a {@link RecordingEventStore} that uses a no-op {@link EventStore} implementation for all operations
     */
    public static RecordingEventStore recordingEventStore() {
        return new RecordingEventStore(NOOP_EVENT_STORE);
    }

    /**
     * A no-op {@link EventStore} implementation that throws {@link UnsupportedOperationException} for all operations
     * except for {@code #subscribe(BiFunction)} and
     * {@code #publish(ProcessingContext<ProcessingContext>, List<EventMessage>)} which are no-ops.
     */
    @Internal
    static final EventStore NOOP_EVENT_STORE = new EventStore() {

        @Override
        public EventStoreTransaction transaction(ProcessingContext processingContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // no-op
        }

        @Override
        public Registration subscribe(BiFunction<List<? extends EventMessage>,
                @Nullable ProcessingContext,
                CompletableFuture<?>> eventsBatchConsumer) {
            return () -> true;
        }

        @Override
        public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                               List<? extends EventMessage> events) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public MessageStream<EventMessage> open(StreamingCondition condition, @Nullable ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<TrackingToken> tokenAt(Instant at, @Nullable ProcessingContext context) {
            throw new UnsupportedOperationException();
        }
    };
}
