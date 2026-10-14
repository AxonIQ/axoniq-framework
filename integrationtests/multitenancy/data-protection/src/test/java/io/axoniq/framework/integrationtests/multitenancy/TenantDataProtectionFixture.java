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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsRequest;
import io.axoniq.axonserver.grpc.event.dcb.StreamEventsResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.dataprotection.api.DataSubjectId;
import io.axoniq.framework.dataprotection.api.FieldEncryptingConverter;
import io.axoniq.framework.dataprotection.api.PersonalData;
import io.axoniq.framework.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.jspecify.annotations.NonNull;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.crypto.SecretKey;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Shared two-tenant application fixture for data-protection integration tests.
 *
 * @author Jan Galinski
 */
final class TenantDataProtectionFixture {

    static final String TENANT_A = "dataprotection-tenant-a";
    static final String TENANT_B = "dataprotection-tenant-b";
    static final String CUSTOMER_ID = "customer-1";

    private static final QualifiedName CUSTOMER_SNAPSHOT = new QualifiedName("test", "CustomerSnapshot");
    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    private final Map<String, RecordingInMemoryCryptoEngine> cryptoEngines = new ConcurrentHashMap<>();
    private final Map<String, FieldEncryptingConverter> tenantConverters = new ConcurrentHashMap<>();

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;

    void start() {
        start(configurer -> {
        });
    }

    void start(Consumer<EventSourcingConfigurer> customize) {
        INFRASTRUCTURE.start();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);

        EventSourcingConfigurer configurer = EventSourcingConfigurer.create()
                                                                  .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                                                  .componentRegistry(registry -> TenantFixture.connectOnlyCustomTenantsPredicate(registry, "dataprotection-tenant-"))
                                                                  .componentRegistry(registry -> registry.registerComponent(
                                                                          TenantComponentProvider.class,
                                                                          configuration -> TenantComponentProvider.withFactory(
                                                                                  Converter.class,
                                                                                  tenant -> tenantConverters.computeIfAbsent(
                                                                                          tenant.tenantId(),
                                                                                          ignored -> new FieldEncryptingConverter(
                                                                                                  cryptoEngines.computeIfAbsent(
                                                                                                          tenant.tenantId(),
                                                                                                          unused -> new RecordingInMemoryCryptoEngine()
                                                                                                  ),
                                                                                                  new JacksonConverter()
                                                                                          )
                                                                                  )
                                                                          )
                                                                  ));
        customize.accept(configurer);
        application = configurer.start();
        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(application.getComponent(TenantProvider.class).tenants())
                       .extracting(TenantDescriptor::tenantId)
                       .contains(TENANT_A, TENANT_B));
    }

    void stop() {
        if (application != null) {
            application.shutdown();
        }
        contextManager.deleteContexts(TENANT_A, TENANT_B);
        INFRASTRUCTURE.stop();
    }

    void publishEvent(String tenantId, String email) {
        var unitOfWork = application.getComponent(org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory.class)
                                    .create();
        unitOfWork.runOnInvocation(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            EventAppender.forContext(context).append(new CustomerDataRecorded(CUSTOMER_ID, email));
        });
        unitOfWork.execute().orTimeout(15, TimeUnit.SECONDS).join();
    }

    Snapshot storeAndLoadSnapshot(String tenantId, CustomerDataSnapshot payload) {
        SnapshotStore snapshotStore = application.getComponent(SnapshotStore.class);
        var context = new StubProcessingContext().withResource(TenantDescriptor.RESOURCE_KEY,
                                                                TenantDescriptor.tenantWithId(tenantId));
        Snapshot snapshot = new Snapshot(new GlobalIndexPosition(0), "1.0.0", payload, Instant.now(), Map.of());

        snapshotStore.store(CUSTOMER_SNAPSHOT, CUSTOMER_ID, snapshot, context).join();
        return snapshotStore.load(CUSTOMER_SNAPSHOT, CUSTOMER_ID, context).join();
    }

    String rawStoredSnapshotPayload(Snapshot snapshot) {
        return new String((byte[]) snapshot.payload(), StandardCharsets.UTF_8);
    }

    String rawStoredPayload(String tenantId) {
        AtomicReference<String> payload = new AtomicReference<>();
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            StreamEventsResponse event = firstStoredEvent(tenantId);
            assertThat(event)
                    .as("an event stored in tenant [%s]", tenantId)
                    .isNotNull();
            payload.set(event.getEvent().getEvent().getPayload().toStringUtf8());
        });
        return payload.get();
    }

    private StreamEventsResponse firstStoredEvent(String tenantId) {
        AxonServerConnectionManager connectionManager = application.getComponent(AxonServerConnectionManager.class);
        try (ResultStream<StreamEventsResponse> stream = connectionManager.getConnection(tenantId)
                                                                          .dcbEventChannel()
                                                                          .stream(StreamEventsRequest.newBuilder()
                                                                                                      .setFromSequence(0)
                                                                                                      .build())) {
            return stream.nextIfAvailable(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrieving the event stored for tenant [" + tenantId + "]", e);
        }
    }

    Map<String, RecordingInMemoryCryptoEngine> cryptoEngines() {
        return cryptoEngines;
    }

    Map<String, FieldEncryptingConverter> tenantConverters() {
        return tenantConverters;
    }

    @Event(namespace = "test", name = "CustomerDataRecorded", version = "1.0.0")
    record CustomerDataRecorded(@DataSubjectId String customerId, @PersonalData String email) {

    }

    record CustomerDataSnapshot(@DataSubjectId String customerId, @PersonalData String email) {

    }

    /**
     * The extension's {@link InMemoryCryptoEngine} shares its backing map between instances. Recording calls on each
     * instance therefore proves converter routing without representing stronger key-store isolation than this test
     * engine provides.
     */
    static class RecordingInMemoryCryptoEngine extends InMemoryCryptoEngine {

        private final Set<String> createdKeyIds = ConcurrentHashMap.newKeySet();

        @Override
        @NonNull
        public SecretKey getOrCreateKey(@NonNull String id) {
            createdKeyIds.add(id);
            return super.getOrCreateKey(id);
        }

        Set<String> createdKeyIds() {
            return createdKeyIds;
        }
    }
}
