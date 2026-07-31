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

package io.axoniq.framework.integrationtests.multitenancy.hunt;

import io.axoniq.framework.integrationtests.multitenancy.DisableMultiTenancyTestsWithoutLicense;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.BalanceStore;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.EmitBalanceUpdate;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.StartWatch;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.WatchAccount;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Hunt scenarios S1 and S4 on subscription-query update delivery.
 * <p>
 * S1 (claim MT-C2): a subscription query routed to its tenant by the ProcessingContext, without naming the tenant in
 * its metadata, must receive the updates its tenant emits, exactly like the metadata-carrying subscription does.
 * <p>
 * S4 (claim MT-C6): an update emitted by a handler whose unit of work then fails must not reach the subscriber.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class SubscriptionQueryUpdateDeliveryIT {

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    private final Map<String, BalanceStore> stores = new ConcurrentHashMap<>();
    private AxonConfiguration application;
    private AxonServerTestInfrastructure.ContextManager contextManager;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.deleteAllCustomContexts();
        contextManager.createContext(tenantA);
        contextManager.createContext(tenantB);
        TenantBankFixture.contextRoutedSubscriptions.clear();
        application = TenantBankFixture.startApp(INFRASTRUCTURE, stores, runId, registry -> {
        });
    }

    @AfterEach
    void tearDown() {
        TenantBankFixture.contextRoutedSubscriptions.values().forEach(MessageStream::close);
        TenantBankFixture.contextRoutedSubscriptions.clear();
        if (application != null) {
            application.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Nested
    class ContextRoutedSubscription {

        @Test
        void metadataCarryingSubscriptionReceivesItsTenantsUpdate() {
            // control arm: the exact flow the existing ITs prove, expressed with this fixture
            MessageStream<QueryResponseMessage> stream = subscribeWithMetadata("acct-meta", tenantA);
            assertInitialResult(stream);

            sendAndAwait(new EmitBalanceUpdate("acct-meta", 7L, false), tenantA);

            assertThat(nextValue(stream, Duration.ofSeconds(10)))
                    .as("update for the metadata-carrying subscription of tenant A")
                    .isEqualTo(7L);
        }

        @Test
        void contextRoutedSubscriptionReceivesItsTenantsUpdate() {
            // given a subscription query dispatched from inside a tenant-A handler, tenant taken from the context
            sendAndAwait(new StartWatch("acct-ctx"), tenantA);
            MessageStream<QueryResponseMessage> stream =
                    await().atMost(Duration.ofSeconds(10))
                           .until(() -> TenantBankFixture.contextRoutedSubscriptions.get("acct-ctx"),
                                  s -> s != null);
            // evidence: the subscription reached the tenant's context and produced its initial result
            assertInitialResult(stream);

            // when tenant A emits a matching update
            sendAndAwait(new EmitBalanceUpdate("acct-ctx", 9L, false), tenantA);

            // then the update reaches the context-routed subscription of the SAME tenant
            assertThat(nextValue(stream, Duration.ofSeconds(10)))
                    .as("update for the context-routed subscription of tenant A")
                    .isEqualTo(9L);
        }
    }

    @Nested
    class RolledBackEmission {

        @Test
        void updateEmittedByFailingUnitOfWorkIsNotDelivered() {
            MessageStream<QueryResponseMessage> stream = subscribeWithMetadata("acct-rb", tenantA);
            assertInitialResult(stream);

            // when a handler emits an update and then fails its unit of work
            Throwable failure = null;
            try {
                sendAndAwait(new EmitBalanceUpdate("acct-rb", 666L, true), tenantA);
            } catch (Exception e) {
                failure = e;
            }
            assertThat(failure).as("the emitting command must fail (its handler throws)").isNotNull();

            // and a subsequent successful emission happens
            sendAndAwait(new EmitBalanceUpdate("acct-rb", 1L, false), tenantA);

            // then the first update observed is the committed one; the rolled-back 666 never surfaces
            Long first = nextValue(stream, Duration.ofSeconds(10));
            assertThat(first)
                    .as("first update after a rolled-back emission followed by a committed emission")
                    .isEqualTo(1L);
        }
    }

    private MessageStream<QueryResponseMessage> subscribeWithMetadata(String accountId, String tenant) {
        QueryBus queryBus = application.getComponent(QueryBus.class);
        return queryBus.subscriptionQuery(
                new GenericQueryMessage(new MessageType(WatchAccount.class), new WatchAccount(accountId))
                        .andMetadata(tenantMetadata(tenant)),
                null,
                50);
    }

    private void assertInitialResult(MessageStream<QueryResponseMessage> stream) {
        assertThat(nextValue(stream, Duration.ofSeconds(10)))
                .as("initial result of the WatchAccount subscription")
                .isEqualTo(0L);
    }

    private static Long nextValue(MessageStream<QueryResponseMessage> stream, Duration timeout) {
        await().atMost(timeout).until(stream::hasNextAvailable);
        return stream.next().orElseThrow().message().payloadAs(Long.class);
    }

    private void sendAndAwait(Object command, String tenant) {
        application.getComponent(CommandGateway.class)
                   .send(command, tenantMetadata(tenant), null)
                   .getResultMessage()
                   .orTimeout(30, TimeUnit.SECONDS)
                   .join();
    }
}
