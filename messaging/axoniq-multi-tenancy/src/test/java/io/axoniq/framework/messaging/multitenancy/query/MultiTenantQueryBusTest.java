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

package io.axoniq.framework.messaging.multitenancy.query;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.query.TenantQuerySegmentFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class MultiTenantQueryBusTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant-1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant-2");
    private static final QualifiedName QUERY_NAME = new QualifiedName("FindOrder");

    private RecordingQueryBus tenant1Bus;
    private RecordingQueryBus tenant2Bus;
    private MultiTenantQueryBus testSubject;

    @BeforeEach
    void setUp() {
        tenant1Bus = new RecordingQueryBus(TENANT_1.tenantId());
        tenant2Bus = new RecordingQueryBus(TENANT_2.tenantId());

        TenantQuerySegmentFactory tenantQuerySegmentFactory = tenant -> {
            if (tenant.equals(TENANT_1)) {
                return tenant1Bus;
            }
            if (tenant.equals(TENANT_2)) {
                return tenant2Bus;
            }
            throw new IllegalArgumentException("Unexpected tenant: " + tenant.tenantId());
        };
        TenantResolver<Message> tenantResolver = (message, tenants) ->
                message.payloadAs(String.class).equals(TENANT_1.tenantId()) ? TENANT_1 : TENANT_2;

        testSubject = new MultiTenantQueryBus(tenantQuerySegmentFactory, tenantResolver);
    }

    @Nested
    class QueryDispatching {

        @Test
        void queryRoutesToResolvedTenantSegment() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            QueryMessage query = queryFor(QUERY_NAME, TENANT_2.tenantId());

            // when
            MessageStream<QueryResponseMessage> result = testSubject.query(query, null);

            // then
            assertThat(firstResponsePayload(result)).isEqualTo(TENANT_2.tenantId());
            assertThat(tenant1Bus.queryCount.get()).isZero();
            assertThat(tenant2Bus.queryCount.get()).isEqualTo(1);
        }

        @Test
        void subscriptionQueryRoutesToResolvedTenantSegment() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            QueryMessage query = queryFor(QUERY_NAME, TENANT_1.tenantId());

            // when
            MessageStream<QueryResponseMessage> result = testSubject.subscriptionQuery(query, null, 10);

            // then
            assertThat(firstResponsePayload(result)).isEqualTo(TENANT_1.tenantId());
            assertThat(tenant1Bus.subscriptionQueryCount.get()).isEqualTo(1);
            assertThat(tenant2Bus.subscriptionQueryCount.get()).isZero();
        }
    }

    @Nested
    class HandlerReplay {

        @Test
        void subscribePropagatesHandlersToExistingTenantSegments() {
            // given
            testSubject.registerTenant(TENANT_1);
            testSubject.registerTenant(TENANT_2);

            QueryHandler handler = (query, context) -> MessageStream.fromIterable(
                    () -> List.of(response("shared-response")).iterator()
            );

            // when
            testSubject.subscribe(QUERY_NAME, handler);

            // then
            assertThat(firstResponsePayload(testSubject.query(queryFor(QUERY_NAME, TENANT_1.tenantId()), null)))
                    .isEqualTo("shared-response");
            assertThat(firstResponsePayload(testSubject.query(queryFor(QUERY_NAME, TENANT_2.tenantId()), null)))
                    .isEqualTo("shared-response");
            assertThat(tenant1Bus.subscribeCount.get()).isEqualTo(1);
            assertThat(tenant2Bus.subscribeCount.get()).isEqualTo(1);
        }

        @Test
        void registerAndStartTenantReplaysHandlersWhenSegmentIsCreatedLater() {
            // given
            QueryHandler handler = (query, context) -> MessageStream.fromIterable(
                    () -> List.of(response("late-tenant-response")).iterator()
            );
            testSubject.subscribe(QUERY_NAME, handler);

            // when
            testSubject.registerAndStartTenant(TENANT_2);

            // then
            assertThat(firstResponsePayload(testSubject.query(queryFor(QUERY_NAME, TENANT_2.tenantId()), null)))
                    .isEqualTo("late-tenant-response");
            assertThat(tenant2Bus.subscribeCount.get()).isEqualTo(1);
        }
    }

    private static QueryMessage queryFor(QualifiedName queryName, String payload) {
        return new GenericQueryMessage(
                new GenericMessage(
                        "query-" + payload,
                        new MessageType(queryName),
                        payload,
                        Map.of()
                )
        );
    }

    private static String firstResponsePayload(MessageStream<QueryResponseMessage> stream) {
        try {
            Optional<MessageStream.Entry<QueryResponseMessage>> entry = stream.next();
            assertThat(entry).isPresent();
            return entry.get().message().payloadAs(String.class);
        } finally {
            stream.close();
        }
    }

    private static org.axonframework.messaging.queryhandling.QueryResponseMessage response(String payload) {
        return new GenericQueryResponseMessage(
                new GenericMessage(
                        "response-" + payload,
                        new MessageType(String.class),
                        payload,
                        Map.of()
                )
        );
    }

    private static final class RecordingQueryBus implements QueryBus {

        private final String tenantId;
        private final Map<QualifiedName, QueryHandler> handlers = new ConcurrentHashMap<>();
        private final AtomicInteger queryCount = new AtomicInteger();
        private final AtomicInteger subscriptionQueryCount = new AtomicInteger();
        private final AtomicInteger subscribeCount = new AtomicInteger();

        private RecordingQueryBus(String tenantId) {
            this.tenantId = tenantId;
        }

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query, ProcessingContext processingContext) {
            queryCount.incrementAndGet();
            return execute(query, processingContext);
        }

        @Override
        public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                     ProcessingContext processingContext,
                                                                     int updateBufferSize) {
            subscriptionQueryCount.incrementAndGet();
            return execute(query, processingContext);
        }

        @Override
        public MessageStream<SubscriptionQueryUpdateMessage> subscribeToUpdates(QueryMessage query, int updateBufferSize) {
            return MessageStream.fromIterable(() -> List.<SubscriptionQueryUpdateMessage>of().iterator());
        }

        @Override
        public CompletableFuture<Void> emitUpdate(Predicate<QueryMessage> filter,
                                                  Supplier<SubscriptionQueryUpdateMessage> updateSupplier,
                                                  ProcessingContext processingContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> completeSubscriptions(Predicate<QueryMessage> filter,
                                                             ProcessingContext processingContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> completeSubscriptionsExceptionally(Predicate<QueryMessage> filter,
                                                                           Throwable cause,
                                                                           ProcessingContext processingContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public QueryBus subscribe(QualifiedName queryName, QueryHandler queryHandler) {
            subscribeCount.incrementAndGet();
            handlers.put(queryName, queryHandler);
            return this;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("tenantId", tenantId);
        }

        private MessageStream<QueryResponseMessage> execute(QueryMessage query, ProcessingContext processingContext) {
            QueryHandler queryHandler = handlers.get(query.type().qualifiedName());
            if (queryHandler != null) {
                return queryHandler.handle(query, processingContext);
            }
            return MessageStream.<QueryResponseMessage>fromIterable(() -> List.of(response(tenantId)).iterator());
        }
    }
}
