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

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBusConfiguration;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DefaultAxonApplication;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Integration test for the multi-tenancy feature exercising queries through the {@link QueryGateway} and
 * subscription queries of the {@link QueryBus} against a multi-context Axon Server, mirroring
 * {@link MultiTenantCommandHandlingIT}.
 * <p>
 * Every test runs twice: once with {@link DistributedQueryBusConfiguration#preferLocalQueryHandler()} enabled, so
 * queries with a locally registered handler are served from the local segment, and once with it disabled, so every
 * query is dispatched through the multi-tenant connector. Tenant routing and isolation must hold in both setups.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 */
@ParameterizedClass(name = "preferLocalQueryHandler = {0}")
@ValueSource(booleans = {false, true})
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantQueryHandlingIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();
    private static final String TENANT_A = "tenant-A";
    private static final String TENANT_B = "tenant-B";

    private final boolean preferLocalQueryHandler;

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;
    private final Map<String, QueryUpdateEmitter> capturedEmitters = new ConcurrentHashMap<>();
    private TenantProvider tenantDescriptors;

    MultiTenantQueryHandlingIT(boolean preferLocalQueryHandler) {
        this.preferLocalQueryHandler = preferLocalQueryHandler;
    }

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);
        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder(ADMIN_CONTEXT, DEFAULT_CONTEXT, TENANT_A, TENANT_B);

        QueryHandlingModule.QueryHandlerPhase queryHandlingModule =
                QueryHandlingModule.named("multi-tenancy-query-it-module")
                                   .queryHandlers()
                                   .queryHandler(new QualifiedName(RecordTenantQuery.class),
                                                 this::resolveTenant)
                                   .queryHandler(new QualifiedName(SubscriptionTenantQuery.class),
                                                 this::resolveTenantAndCaptureEmitter)
                                   .queryHandler(new QualifiedName(DispatchFollowUpQuery.class),
                                                 this::dispatchFollowUpQuery)
                                   .autodetectedQueryHandlingComponent(cfg -> this);

        application = new DefaultAxonApplication()
                .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                // Parameterized per class invocation: with the local handler preferred, queries with a local handler
                // are served from the local segment; without it, every query goes through the multi-tenant connector.
                .componentRegistry(cr -> cr.registerComponent(
                        DistributedQueryBusConfiguration.class,
                        cfg -> DistributedQueryBusConfiguration.DEFAULT
                                .preferLocalQueryHandler(preferLocalQueryHandler)))
                .componentRegistry(cr -> cr.registerComponent(
                        TenantConnectPredicate.class,
                        c -> d -> !Set.of(ADMIN_CONTEXT, DEFAULT_CONTEXT).contains(d.tenantId())))
                // Identity factory: the tenant-scoped component IS the resolved TenantDescriptor, so injecting it
                // into the annotated handler below proves parameter resolution picks the dispatched tenant's instance.
                .componentRegistry(registry -> registry.registerComponent(TenantComponentProvider.class,
                                                                          cfg -> TenantComponentProvider.withFactory(
                                                                                  TenantDescriptor.class,
                                                                                  tenant -> tenant)))
                .componentRegistry(cr -> cr.registerModule(queryHandlingModule.build()))
                .start();

        tenantDescriptors = application.getComponent(TenantProvider.class);
    }

    @AfterEach
    void tearDown() {
        application.shutdown();
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        capturedEmitters.clear();
        INFRASTRUCTURE.stop();
    }

    @Test
    void querySentViaTenantContextWithTenantMeta() {
        QueryGateway queryGateway = application.getComponent(QueryGateway.class);

        assertThat(queryTenant(queryGateway, new RecordTenantQuery("for-tenant-a"), TENANT_A))
                .isEqualTo(TENANT_A);
        assertThat(queryTenant(queryGateway, new RecordTenantQuery("for-tenant-b"), TENANT_B))
                .isEqualTo(TENANT_B);
    }

    @Test
    void querySentViaDynamicallyAddedTenant() {
        String dynamicTenant = "tenant-D";
        QueryGateway queryGateway = application.getComponent(QueryGateway.class);

        assertThat(queryTenant(queryGateway, new RecordTenantQuery("for-tenant-a"), TENANT_A))
                .isEqualTo(TENANT_A);

        contextManager.createContext(dynamicTenant);

        await().untilAsserted(() -> assertThat(tenantDescriptors.tenants()).anyMatch(d -> dynamicTenant.equals(d.tenantId())));

        assertThat(queryTenant(queryGateway, new RecordTenantQuery("for-tenant-d"), dynamicTenant))
                .isEqualTo(dynamicTenant);
    }

    @Test
    void sendingQueryToDeletedTenantFails() {
        QueryGateway queryGateway = application.getComponent(QueryGateway.class);
        contextManager.deleteContext(TENANT_A);

        // Awaiting TenantProvider#tenants() is not a reliable signal here: the provider removes the descriptor (so
        // tenants() no longer lists it) before it cancels the connector's tenant registration that actually drops the
        // per-tenant connector. Until then a query still resolves the tenant's connector and round-trips successfully.
        // Await the connector-observable failure directly, re-dispatching until the tenant is gone.
        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> assertThat(dispatchFailure(queryGateway,
                                                                 new RecordTenantQuery("for-tenant-a"),
                                                                 TENANT_A))
                       .isInstanceOf(TenantNotResolvedException.class));
    }

    /**
     * Dispatches the given {@code payload} for the given {@code tenant} and returns the (unwrapped) failure it
     * produces, whether raised synchronously by tenant resolution or asynchronously through the result stream.
     * Returns {@code null} when the query succeeds.
     */
    private static @Nullable Throwable dispatchFailure(QueryGateway queryGateway, Object payload, String tenant) {
        try {
            queryGateway.query(payload,
                              String.class,
                              Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, tenant),
                              null)
                       .orTimeout(10, TimeUnit.SECONDS)
                       .join();
            return null;
        } catch (CompletionException e) {
            return e.getCause();
        } catch (Throwable e) {
            return e;
        }
    }

    @Test
    void queryHandlerReceivesTenantScopedComponentMatchingTheDispatchedTenant() {
        QueryGateway queryGateway = application.getComponent(QueryGateway.class);

        assertThat(queryTenant(queryGateway, new ResolveTenantScopedComponentQuery("for-tenant-a"), TENANT_A))
                .isEqualTo(TENANT_A);
        assertThat(queryTenant(queryGateway, new ResolveTenantScopedComponentQuery("for-tenant-b"), TENANT_B))
                .isEqualTo(TENANT_B);
    }

    @Test
    void subscriptionQueryRoutesInitialResultAndUpdatesToResolvedTenantInIsolation() {
        QueryBus queryBus = application.getComponent(QueryBus.class);

        // given a subscription query per tenant, each carrying its tenant id as the query payload
        MessageStream<QueryResponseMessage> streamA = subscriptionQuery(queryBus, TENANT_A);
        MessageStream<QueryResponseMessage> streamB = subscriptionQuery(queryBus, TENANT_B);

        // then the initial result is routed to the resolved tenant's context
        assertThat(nextPayload(streamA)).isEqualTo(TENANT_A);
        assertThat(nextPayload(streamB)).isEqualTo(TENANT_B);
        // and both handlers captured the emitter scoped to their tenant's context
        await().untilAsserted(() -> assertThat(capturedEmitters).containsKeys(TENANT_A, TENANT_B));

        // when emitting an update with a tenant-BLIND predicate (matches both tenants' payloads) from each tenant's
        // own captured emitter - isolation must now come from the bus itself, not from a tenant-distinguishing filter
        capturedEmitters.get(TENANT_A).emit(SubscriptionTenantQuery.class, q -> true, updatePayload(TENANT_A));
        capturedEmitters.get(TENANT_B).emit(SubscriptionTenantQuery.class, q -> true, updatePayload(TENANT_B));

        // then each subscription stream receives only the update emitted from its own tenant's context
        assertThat(nextPayload(streamA)).isEqualTo(updatePayload(TENANT_A));
        assertThat(nextPayload(streamB)).isEqualTo(updatePayload(TENANT_B));

        // when completing tenant-A's subscription with a tenant-BLIND predicate
        capturedEmitters.get(TENANT_A).complete(SubscriptionTenantQuery.class, q -> true);

        // then only tenant-A's stream completes; tenant-B's subscription is unaffected by a same-process,
        // tenant-blind completion issued from tenant-A's context
        await().untilAsserted(() -> {
            assertThat(streamA.hasNextAvailable()).isFalse();
            assertThat(streamA.isCompleted()).isTrue();
        });
        assertThat(streamB.hasNextAvailable()).isFalse();
        assertThat(streamB.isCompleted()).isFalse();

        // and completing tenant-B's own subscription completes it too, independently
        capturedEmitters.get(TENANT_B).complete(SubscriptionTenantQuery.class, q -> true);
        await().untilAsserted(() -> {
            assertThat(streamB.hasNextAvailable()).isFalse();
            assertThat(streamB.isCompleted()).isTrue();
        });
    }

    /**
     * The subscription query stream must terminate when its tenant's connector is removed, rather than waiting.
     */
    @Test
    void activeSubscriptionQueryTerminatesAfterItsTenantConnectorIsRemoved() {
        assumeFalse(preferLocalQueryHandler,
                    "The tenant connector is only used when local query handling is not preferred.");
        QueryBus queryBus = application.getComponent(QueryBus.class);
        QueryGateway queryGateway = application.getComponent(QueryGateway.class);

        // given an active subscription query which has delivered an initial result and an update
        MessageStream<QueryResponseMessage> stream = subscriptionQuery(queryBus, TENANT_A);
        assertThat(nextPayload(stream)).isEqualTo(TENANT_A);
        await().untilAsserted(() -> assertThat(capturedEmitters).containsKey(TENANT_A));
        capturedEmitters.get(TENANT_A).emit(SubscriptionTenantQuery.class, query -> true, updatePayload(TENANT_A));
        assertThat(nextPayload(stream)).isEqualTo(updatePayload(TENANT_A));

        // when the tenant context is deleted
        contextManager.deleteContext(TENANT_A);

        // and its connector has actually been removed, rather than merely its descriptor from the provider
        await().untilAsserted(() -> assertThat(dispatchFailure(queryGateway,
                                                                 new RecordTenantQuery("after-deletion"),
                                                                 TENANT_A))
                       .isInstanceOf(TenantNotResolvedException.class));

        // then the already-active stream terminates instead of waiting indefinitely
        await().untilAsserted(() -> assertThat(stream.isCompleted() || stream.error().isPresent()).isTrue());
    }

    @Test
    void followUpQueryDispatchedFromWithinAHandlerStaysWithTheTenantOfTheHandledMessage() {
        QueryGateway queryGateway = application.getComponent(QueryGateway.class);

        assertThat(queryTenant(queryGateway, new DispatchFollowUpQuery("chained"), TENANT_A))
                .isEqualTo(TENANT_A);
    }

    @QueryHandler
    public String resolveTenantScopedComponent(ResolveTenantScopedComponentQuery query,
                                               @TenantScoped TenantDescriptor tenantScopedComponent) {
        return tenantScopedComponent.tenantId();
    }

    private MessageStream<QueryResponseMessage> resolveTenant(QueryMessage query, ProcessingContext context) {
        String tenantId = TenantDescriptor.fromContext(context).get().tenantId();
        return MessageStream.just(new GenericQueryResponseMessage(new MessageType(String.class), tenantId));
    }

    // The follow-up query is dispatched without naming a tenant of its own: this handler's own result IS the
    // follow-up's result, so the tenant it resolves to is exactly what the caller observes.
    private MessageStream<QueryResponseMessage> dispatchFollowUpQuery(QueryMessage query, ProcessingContext context) {
        String id = query.payloadAs(DispatchFollowUpQuery.class).id();
        QueryMessage followUp = new GenericQueryMessage(new MessageType(RecordTenantQuery.class),
                                                        new RecordTenantQuery(id));
        return context.component(QueryBus.class).query(followUp, context);
    }

    private MessageStream<QueryResponseMessage> resolveTenantAndCaptureEmitter(QueryMessage query,
                                                                              ProcessingContext context) {
        String tenantId = TenantDescriptor.fromContext(context).get().tenantId();
        capturedEmitters.put(tenantId, QueryUpdateEmitter.forContext(context));
        return MessageStream.just(new GenericQueryResponseMessage(new MessageType(String.class), tenantId));
    }

    private static String queryTenant(QueryGateway queryGateway, Object payload, String tenant) {
        return queryGateway.query(payload,
                                  String.class,
                                  Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, tenant),
                                  null)
                           .orTimeout(10, TimeUnit.SECONDS)
                           .join();
    }

    private MessageStream<QueryResponseMessage> subscriptionQuery(QueryBus queryBus, String tenant) {
        return queryBus.subscriptionQuery(
                tenantQuery(SubscriptionTenantQuery.class, new SubscriptionTenantQuery(tenant), tenant),
                null,
                50);
    }

    private static QueryMessage tenantQuery(Class<?> queryType, Object payload, String tenant) {
        return new GenericQueryMessage(new MessageType(queryType), payload)
                .andMetadata(Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, tenant));
    }

    private static String nextPayload(MessageStream<QueryResponseMessage> stream) {
        await().until(stream::hasNextAvailable);
        return stream.next().orElseThrow().message().payloadAs(String.class);
    }

    private static String updatePayload(String tenant) {
        return tenant + "-update";
    }

    public record RecordTenantQuery(String id) {

    }

    public record ResolveTenantScopedComponentQuery(String id) {

    }

    public record SubscriptionTenantQuery(String id) {

    }

    public record DispatchFollowUpQuery(String id) {

    }
}
