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
 * https://www.axoniq.io/pricing
 */

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import io.axoniq.framework.dataprotection.api.DataSubjectId;
import io.axoniq.framework.dataprotection.api.FieldEncryptingConverter;
import io.axoniq.framework.dataprotection.api.PersonalData;
import io.axoniq.framework.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamEventSourceFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving that data-protection converters are resolved per tenant for both event storage and
 * persistent-stream consumption.
 *
 * @author Jan Galinski
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantDataProtectionIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();
    private static final String TENANT_A = "tenant-A";
    private static final String TENANT_B = "tenant-B";
    private static final String CUSTOMER_ID = "customer-1";

    private final Map<String, RecordingInMemoryCryptoEngine> cryptoEngines = new ConcurrentHashMap<>();
    private final List<HandledCustomerData> handled = new CopyOnWriteArrayList<>();

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;
    private String streamName;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);
        streamName = "multi-tenant-data-protection-" + UUID.randomUUID();
        application = buildApplication();
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void usesEachTenantsConverterToStoreAndReadProtectedEvents() {
        publishEvent(TENANT_A, "alice@tenant-a.example");
        publishEvent(TENANT_B, "bob@tenant-b.example");

        assertThat(rawStoredPayload(TENANT_A))
                .contains(CUSTOMER_ID)
                .doesNotContain("alice@tenant-a.example");
        assertThat(rawStoredPayload(TENANT_B))
                .contains(CUSTOMER_ID)
                .doesNotContain("bob@tenant-b.example");

        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(handled).containsExactlyInAnyOrder(
                       new HandledCustomerData(TENANT_A, CUSTOMER_ID, "alice@tenant-a.example"),
                       new HandledCustomerData(TENANT_B, CUSTOMER_ID, "bob@tenant-b.example")
               ));

        assertThat(cryptoEngines).containsOnlyKeys(TENANT_A, TENANT_B);
        assertThat(cryptoEngines.get(TENANT_A)).isNotSameAs(cryptoEngines.get(TENANT_B));
        assertThat(cryptoEngines.get(TENANT_A).createdKeyIds()).contains(CUSTOMER_ID);
        assertThat(cryptoEngines.get(TENANT_B).createdKeyIds()).contains(CUSTOMER_ID);
    }

    private AxonConfiguration buildApplication() {
        return EventSourcingConfigurer.create()
                                      .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                      .componentRegistry(registry -> registry.registerComponent(
                                              TenantResolver.class,
                                              configuration -> new MetadataBasedTenantResolver()
                                      ))
                                      .componentRegistry(registry -> registry.registerComponent(
                                              TenantConnectPredicate.class,
                                              configuration -> descriptor -> !Set.of(ADMIN_CONTEXT, DEFAULT_CONTEXT)
                                                                              .contains(descriptor.tenantId())
                                      ))
                                      .componentRegistry(registry -> registry.registerComponent(
                                              TenantComponentProvider.class,
                                              configuration -> TenantComponentProvider.withFactory(
                                                      Converter.class,
                                                      tenant -> new FieldEncryptingConverter(
                                                              cryptoEngines.computeIfAbsent(
                                                                      tenant.tenantId(),
                                                                      ignored -> new RecordingInMemoryCryptoEngine()
                                                              ),
                                                              new JacksonConverter()
                                                      )
                                              )
                                      ))
                                      .messaging(messaging -> messaging.eventProcessing(
                                              processing -> processing.subscribing(
                                                      subscribing -> subscribing.processor(buildProcessorModule())
                                              )
                                      ))
                                      .start();
    }

    private SubscribingEventProcessorModule buildProcessorModule() {
        return EventProcessorModule.subscribing("multi-tenant-data-protection")
                                   .eventHandlingComponents(components -> components.declarative(
                                           "multi-tenant-data-protection-handler",
                                           configuration -> buildHandlingComponent()
                                   ))
                                   .customized((configuration, subscribing) -> subscribing.eventSource(
                                           buildMultiTenantStreamSource(configuration)
                                   ));
    }

    private EventHandlingComponent buildHandlingComponent() {
        SimpleEventHandlingComponent handlingComponent = SimpleEventHandlingComponent.create(
                "multi-tenant-data-protection-handler",
                SequentialPolicy.INSTANCE
        );
        handlingComponent.subscribe(
                new QualifiedName("test", "CustomerDataRecorded"),
                (event, context) -> {
                    TenantDescriptor tenant = TenantDescriptor.fromContext(context).orElseThrow();
                    CustomerDataRecorded customerData = event.payloadAs(CustomerDataRecorded.class);
                    handled.add(new HandledCustomerData(tenant.tenantId(),
                                                       customerData.customerId(),
                                                       customerData.email()));
                    return MessageStream.empty();
                }
        );
        return handlingComponent;
    }

    private SubscribableEventSource buildMultiTenantStreamSource(Configuration configuration) {
        PersistentStreamProperties properties = new PersistentStreamProperties(
                streamName,
                1,
                PersistentStreamSequencingPolicy.SEQUENTIAL_POLICY,
                Collections.emptyList(),
                "TAIL",
                null
        );
        return new MultiTenantPersistentStreamEventSourceFactory().build(
                streamName,
                properties,
                poolName -> PersistentStreamScheduledExecutorBuilder.defaultFactory().build(1, poolName),
                10,
                configuration
        );
    }

    private void publishEvent(String tenantId, String email) {
        UnitOfWorkFactory unitOfWorkFactory = application.getComponent(UnitOfWorkFactory.class);
        var unitOfWork = unitOfWorkFactory.create();
        unitOfWork.runOnInvocation(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            EventAppender.forContext(context).append(new CustomerDataRecorded(CUSTOMER_ID, email));
        });
        unitOfWork.execute().orTimeout(15, TimeUnit.SECONDS).join();
    }

    private String rawStoredPayload(String tenantId) {
        AxonServerConnectionManager connectionManager = application.getComponent(AxonServerConnectionManager.class);
        try (EventStream stream = connectionManager.getConnection(tenantId).eventChannel().openStream(0, 1)) {
            EventWithToken event = stream.nextIfAvailable(10, TimeUnit.SECONDS);
            return event.getEvent().getPayload().getData().toStringUtf8();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrieving the event stored for tenant [" + tenantId + "]", e);
        }
    }

    @Event(namespace = "test", name = "CustomerDataRecorded", version = "1.0.0")
    record CustomerDataRecorded(@DataSubjectId String customerId, @PersonalData String email) {

    }

    private record HandledCustomerData(String tenantId, String customerId, String email) {

    }

    /**
     * The extension's {@link InMemoryCryptoEngine} shares its backing map between instances. Recording calls on each
     * instance therefore proves converter routing without representing stronger key-store isolation than this test
     * engine provides.
     */
    private static class RecordingInMemoryCryptoEngine extends InMemoryCryptoEngine {

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
