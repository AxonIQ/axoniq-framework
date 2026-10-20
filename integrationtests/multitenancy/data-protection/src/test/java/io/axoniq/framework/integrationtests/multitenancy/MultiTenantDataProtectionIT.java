/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamEventSourceFactory;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.SimpleEventHandlingComponent;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving that data-protection converters are resolved per tenant for event storage and
 * persistent-stream consumption.
 *
 * @author Jan Galinski
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantDataProtectionIT {

    private final TenantDataProtectionFixture fixture = new TenantDataProtectionFixture();
    private final List<HandledCustomerData> handled = new CopyOnWriteArrayList<>();
    private String streamName;

    @BeforeEach
    void setUp() {
        streamName = "multi-tenant-data-protection-" + UUID.randomUUID();
        fixture.start(configurer -> configurer.messaging(messaging -> messaging.eventProcessing(
                processing -> processing.subscribing(
                        subscribing -> subscribing.processor(buildProcessorModule())
                )
        )));
    }

    @AfterEach
    void tearDown() {
        fixture.stop();
    }

    @Test
    void usesEachTenantsConverterToStoreAndReadProtectedEvents() {
        fixture.publishEvent(TenantDataProtectionFixture.TENANT_A, "alice@tenant-a.example");
        fixture.publishEvent(TenantDataProtectionFixture.TENANT_B, "bob@tenant-b.example");

        assertThat(fixture.rawStoredPayload(TenantDataProtectionFixture.TENANT_A))
                .contains(TenantDataProtectionFixture.CUSTOMER_ID)
                .doesNotContain("alice@tenant-a.example");
        assertThat(fixture.rawStoredPayload(TenantDataProtectionFixture.TENANT_B))
                .contains(TenantDataProtectionFixture.CUSTOMER_ID)
                .doesNotContain("bob@tenant-b.example");

        await().atMost(30, TimeUnit.SECONDS)
               .untilAsserted(() -> assertThat(handled).containsExactlyInAnyOrder(
                       new HandledCustomerData(TenantDataProtectionFixture.TENANT_A,
                                               TenantDataProtectionFixture.CUSTOMER_ID,
                                               "alice@tenant-a.example"),
                       new HandledCustomerData(TenantDataProtectionFixture.TENANT_B,
                                               TenantDataProtectionFixture.CUSTOMER_ID,
                                               "bob@tenant-b.example")
               ));

        assertThat(fixture.cryptoEngines()).containsOnlyKeys(TenantDataProtectionFixture.TENANT_A,
                                                              TenantDataProtectionFixture.TENANT_B);
        assertThat(fixture.cryptoEngines().get(TenantDataProtectionFixture.TENANT_A))
                .isNotSameAs(fixture.cryptoEngines().get(TenantDataProtectionFixture.TENANT_B));
        assertThat(fixture.cryptoEngines().get(TenantDataProtectionFixture.TENANT_A).createdKeyIds())
                .contains(TenantDataProtectionFixture.CUSTOMER_ID);
        assertThat(fixture.cryptoEngines().get(TenantDataProtectionFixture.TENANT_B).createdKeyIds())
                .contains(TenantDataProtectionFixture.CUSTOMER_ID);
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
                    TenantDataProtectionFixture.CustomerDataRecorded customerData =
                            event.payloadAs(TenantDataProtectionFixture.CustomerDataRecorded.class);
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

    private record HandledCustomerData(String tenantId, String customerId, String email) {

    }
}
