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
package io.axoniq.framework.messaging.multitenancy.api;

import io.axoniq.framework.messaging.multitenancy.queryhandling.TenantAwareQueryBus;
import io.axoniq.framework.messaging.multitenancy.util.TestFixtures;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantAwareQueryBusTest {

    private static final TenantDescriptor TENANT_A = TestFixtures.TENANT_A;

    private static QueryMessage queryMessageForTenant(String tenantId) {
        return new GenericQueryMessage(new MessageType(new QualifiedName("TestQuery")), "payload")
                .andMetadata(Map.of(TenantDescriptor.TENANT_ID_KEY, tenantId));
    }

    private static ProcessingContext contextForTenant(TenantDescriptor tenant) {
        QueryMessage message = queryMessageForTenant(tenant.tenantId());
        return StubProcessingContext.forMessage(message).withResource(TenantDescriptor.RESOURCE_KEY, tenant);
    }

    private static Supplier<SubscriptionQueryUpdateMessage> updateSupplier() {
        return () -> new GenericSubscriptionQueryUpdateMessage(new MessageType(String.class), "update");
    }

    @Nested
    class TenantScopedFiltering {

        private final RecordingQueryBus delegate = new RecordingQueryBus();
        private final TenantAwareQueryBus testSubject = new TenantAwareQueryBus(delegate,
                                                                                new MetadataBasedTenantResolver());

        @Test
        void emitUpdateAndsTenantClauseOntoTheGivenFilter() {
            // given
            ProcessingContext context = contextForTenant(TENANT_A);
            Predicate<QueryMessage> callerFilter = message -> true;

            // when
            testSubject.emitUpdate(callerFilter, updateSupplier(), context);

            // then
            assertThat(delegate.emitUpdateFilter.test(queryMessageForTenant(TENANT_A.tenantId()))).isTrue();
            assertThat(delegate.emitUpdateFilter.test(queryMessageForTenant("tenant-B"))).isFalse();
            assertThat(delegate.emitUpdateContext).isSameAs(context);
        }

        @Test
        void emitUpdateStillHonoursTheCallersFilter() {
            // given a caller filter that itself rejects everything
            ProcessingContext context = contextForTenant(TENANT_A);
            Predicate<QueryMessage> callerFilter = message -> false;

            // when
            testSubject.emitUpdate(callerFilter, updateSupplier(), context);

            // then even a matching tenant is rejected, since the caller's filter is ANDed in, not replaced
            assertThat(delegate.emitUpdateFilter.test(queryMessageForTenant(TENANT_A.tenantId()))).isFalse();
        }

        @Test
        void completeSubscriptionsAndsTenantClauseOntoTheGivenFilter() {
            // given
            ProcessingContext context = contextForTenant(TENANT_A);

            // when
            testSubject.completeSubscriptions(message -> true, context);

            // then
            assertThat(delegate.completeSubscriptionsFilter.test(queryMessageForTenant(TENANT_A.tenantId()))).isTrue();
            assertThat(delegate.completeSubscriptionsFilter.test(queryMessageForTenant("tenant-B"))).isFalse();
            assertThat(delegate.completeSubscriptionsContext).isSameAs(context);
        }

        @Test
        void completeSubscriptionsExceptionallyAndsTenantClauseOntoTheGivenFilter() {
            // given
            ProcessingContext context = contextForTenant(TENANT_A);
            RuntimeException cause = new RuntimeException("boom");

            // when
            testSubject.completeSubscriptionsExceptionally(message -> true, cause, context);

            // then
            assertThat(delegate.completeSubscriptionsExceptionallyFilter.test(queryMessageForTenant(TENANT_A.tenantId())))
                    .isTrue();
            assertThat(delegate.completeSubscriptionsExceptionallyFilter.test(queryMessageForTenant("tenant-B")))
                    .isFalse();
            assertThat(delegate.completeSubscriptionsExceptionallyCause).isSameAs(cause);
            assertThat(delegate.completeSubscriptionsExceptionallyContext).isSameAs(context);
        }
    }

    @Nested
    class NoTenantResolvable {

        private final RecordingQueryBus delegate = new RecordingQueryBus();
        private final TenantAwareQueryBus testSubject = new TenantAwareQueryBus(delegate,
                                                                                new MetadataBasedTenantResolver());

        @Test
        void emitUpdateThrowsWhenContextCarriesNoTenant() {
            // given a context without a TenantDescriptor resource
            ProcessingContext context = StubProcessingContext.forMessage(queryMessageForTenant(TENANT_A.tenantId()));

            // when / then
            assertThatThrownBy(() -> testSubject.emitUpdate(message -> true, updateSupplier(), context))
                    .isInstanceOf(TenantNotResolvedException.class);
            assertThat(delegate.emitUpdateFilter).isNull();
        }

        @Test
        void emitUpdateThrowsWhenContextIsNull() {
            assertThatThrownBy(() -> testSubject.emitUpdate(message -> true, updateSupplier(), null))
                    .isInstanceOf(TenantNotResolvedException.class);
            assertThat(delegate.emitUpdateFilter).isNull();
        }

        @Test
        void completeSubscriptionsThrowsWhenContextCarriesNoTenant() {
            ProcessingContext context = StubProcessingContext.forMessage(queryMessageForTenant(TENANT_A.tenantId()));

            assertThatThrownBy(() -> testSubject.completeSubscriptions(message -> true, context))
                    .isInstanceOf(TenantNotResolvedException.class);
            assertThat(delegate.completeSubscriptionsFilter).isNull();
        }

        @Test
        void completeSubscriptionsExceptionallyThrowsWhenContextCarriesNoTenant() {
            ProcessingContext context = StubProcessingContext.forMessage(queryMessageForTenant(TENANT_A.tenantId()));

            assertThatThrownBy(() -> testSubject.completeSubscriptionsExceptionally(message -> true,
                                                                                    new RuntimeException(), context))
                    .isInstanceOf(TenantNotResolvedException.class);
            assertThat(delegate.completeSubscriptionsExceptionallyFilter).isNull();
        }
    }

    @Nested
    class InjectedTenantResolverIsConsulted {

        @Test
        void emitUpdateDelegatesResolutionToTheInjectedResolverRatherThanReadingMetadataDirectly() {
            // given a resolver that ignores message metadata entirely and always resolves to TENANT_A
            TenantResolver alwaysTenantA = (message, tenants) -> TENANT_A;
            RecordingQueryBus delegate = new RecordingQueryBus();
            TenantAwareQueryBus testSubject = new TenantAwareQueryBus(delegate, alwaysTenantA);
            ProcessingContext context = contextForTenant(TENANT_A);

            // when
            testSubject.emitUpdate(message -> true, updateSupplier(), context);

            // then a message tagged for a DIFFERENT tenant still matches, since the injected resolver - not the
            // tenantId metadata - decides the tenant: this fails if resolution ever falls back to a metadata read
            assertThat(delegate.emitUpdateFilter.test(queryMessageForTenant("some-other-tenant"))).isTrue();
        }

        @Test
        void completeSubscriptionsDelegatesResolutionToTheInjectedResolver() {
            // given
            TenantResolver alwaysTenantA = (message, tenants) -> TENANT_A;
            RecordingQueryBus delegate = new RecordingQueryBus();
            TenantAwareQueryBus testSubject = new TenantAwareQueryBus(delegate, alwaysTenantA);
            ProcessingContext context = contextForTenant(TENANT_A);

            // when
            testSubject.completeSubscriptions(message -> true, context);

            // then
            assertThat(delegate.completeSubscriptionsFilter.test(queryMessageForTenant("some-other-tenant"))).isTrue();
        }
    }

    @Nested
    class RegistryMessageTenantResolutionFailure {

        private final RecordingQueryBus delegate = new RecordingQueryBus();
        private final TenantAwareQueryBus testSubject = new TenantAwareQueryBus(delegate,
                                                                                new MetadataBasedTenantResolver());

        @Test
        void composedEmitUpdateFilterTreatsAnUnresolvableRegistryMessageAsANonMatchRatherThanThrowing() {
            // given a composed filter for a valid emitting tenant
            ProcessingContext context = contextForTenant(TENANT_A);
            testSubject.emitUpdate(message -> true, updateSupplier(), context);

            // and a registry entry whose stored QueryMessage was never tenant-tagged
            QueryMessage untaggedMessage = new GenericQueryMessage(new MessageType(new QualifiedName("TestQuery")),
                                                                   "payload");

            // when / then: the filter treats it as a non-match instead of propagating the resolver's
            // TenantNotResolvedException, so one unrelated unresolvable entry can't abort matching for every other
            // entry DistributedQueryBus.emitUpdate tests against the same filter in its registry forEach
            assertThat(delegate.emitUpdateFilter.test(untaggedMessage)).isFalse();
        }
    }

    @Nested
    class PassThroughDelegation {

        private final RecordingQueryBus delegate = new RecordingQueryBus();
        private final TenantAwareQueryBus testSubject = new TenantAwareQueryBus(delegate,
                                                                                new MetadataBasedTenantResolver());

        @Test
        void subscribeDelegatesAndReturnsItself() {
            QualifiedName queryName = new QualifiedName("TestQuery");
            QueryHandler handler = (query, context) -> MessageStream.empty();

            TenantAwareQueryBus result = testSubject.subscribe(queryName, handler);

            assertThat(result).isSameAs(testSubject);
            assertThat(delegate.subscribedName).isSameAs(queryName);
            assertThat(delegate.subscribedHandler).isSameAs(handler);
        }

        @Test
        void queryDelegatesWithoutModifyingArguments() {
            QueryMessage query = queryMessageForTenant(TENANT_A.tenantId());
            ProcessingContext context = contextForTenant(TENANT_A);

            testSubject.query(query, context);

            assertThat(delegate.queriedMessage).isSameAs(query);
            assertThat(delegate.queriedContext).isSameAs(context);
        }

        @Test
        void subscriptionQueryDelegatesWithoutModifyingArguments() {
            QueryMessage query = queryMessageForTenant(TENANT_A.tenantId());
            ProcessingContext context = contextForTenant(TENANT_A);

            testSubject.subscriptionQuery(query, context, 50);

            assertThat(delegate.subscriptionQueriedMessage).isSameAs(query);
            assertThat(delegate.subscriptionUpdateBufferSize).isEqualTo(50);
        }

        @Test
        void subscribeToUpdatesDelegatesWithoutModifyingArguments() {
            QueryMessage query = queryMessageForTenant(TENANT_A.tenantId());

            testSubject.subscribeToUpdates(query, 25);

            assertThat(delegate.subscribeToUpdatesMessage).isSameAs(query);
            assertThat(delegate.subscribeToUpdatesBufferSize).isEqualTo(25);
        }

        @Test
        void describeToDelegatesAsAWrapper() {
            MockComponentDescriptor descriptor = new MockComponentDescriptor();

            testSubject.describeTo(descriptor);
            Map<String, ?> describedProperties = descriptor.getDescribedProperties();
            assertThat(describedProperties)
                    .hasSize(1)
                    .containsKey("delegate");
        }
    }

    /**
     * Recording {@link QueryBus} stub capturing the arguments it was invoked with, for state-based assertions on what
     * {@link TenantAwareQueryBus} passes through.
     */
    private static class RecordingQueryBus implements QueryBus {

        private QualifiedName subscribedName;
        private QueryHandler subscribedHandler;
        private QueryMessage queriedMessage;
        private ProcessingContext queriedContext;
        private QueryMessage subscriptionQueriedMessage;
        private int subscriptionUpdateBufferSize;
        private QueryMessage subscribeToUpdatesMessage;
        private int subscribeToUpdatesBufferSize;
        private Predicate<QueryMessage> emitUpdateFilter;
        private ProcessingContext emitUpdateContext;
        private Predicate<QueryMessage> completeSubscriptionsFilter;
        private ProcessingContext completeSubscriptionsContext;
        private Predicate<QueryMessage> completeSubscriptionsExceptionallyFilter;
        private Throwable completeSubscriptionsExceptionallyCause;
        private ProcessingContext completeSubscriptionsExceptionallyContext;

        @Override
        public QueryBus subscribe(QualifiedName queryName, QueryHandler queryHandler) {
            this.subscribedName = queryName;
            this.subscribedHandler = queryHandler;
            return this;
        }

        @Override
        public MessageStream<QueryResponseMessage> query(QueryMessage query, @Nullable ProcessingContext context) {
            this.queriedMessage = query;
            this.queriedContext = context;
            return MessageStream.empty();
        }

        @Override
        public MessageStream<QueryResponseMessage> subscriptionQuery(QueryMessage query,
                                                                     @Nullable ProcessingContext context,
                                                                     int updateBufferSize) {
            this.subscriptionQueriedMessage = query;
            this.subscriptionUpdateBufferSize = updateBufferSize;
            return MessageStream.empty();
        }

        @Override
        public MessageStream<SubscriptionQueryUpdateMessage> subscribeToUpdates(QueryMessage query,
                                                                                int updateBufferSize) {
            this.subscribeToUpdatesMessage = query;
            this.subscribeToUpdatesBufferSize = updateBufferSize;
            return MessageStream.empty();
        }

        @Override
        public CompletableFuture<Void> emitUpdate(Predicate<QueryMessage> filter,
                                                  Supplier<SubscriptionQueryUpdateMessage> updateSupplier,
                                                  @Nullable ProcessingContext context) {
            this.emitUpdateFilter = filter;
            this.emitUpdateContext = context;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> completeSubscriptions(Predicate<QueryMessage> filter,
                                                             @Nullable ProcessingContext context) {
            this.completeSubscriptionsFilter = filter;
            this.completeSubscriptionsContext = context;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> completeSubscriptionsExceptionally(Predicate<QueryMessage> filter,
                                                                          Throwable cause,
                                                                          @Nullable ProcessingContext context) {
            this.completeSubscriptionsExceptionallyFilter = filter;
            this.completeSubscriptionsExceptionallyCause = cause;
            this.completeSubscriptionsExceptionallyContext = context;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("name", "RecordingQueryBus");
        }
    }
}
