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
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.OpenAccount;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Hunt scenarios S5 and S6 on the tenant lifecycle.
 * <p>
 * S5 (gap MT-M1, claim MT-C3): an ACTIVE subscription-query stream must reach a terminal state (completed or error)
 * within 30s of its tenant's context being deleted; silence is the silent-wedge failure shape.
 * <p>
 * S6 (claim MT-C7): contexts created back-to-back all become served tenants; a context deleted and immediately
 * recreated serves commands again.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class TenantLifecycleIT {

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
        application = TenantBankFixture.startApp(INFRASTRUCTURE, stores, runId, registry -> {
        });
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

    @Nested
    class RemovalWithActiveSubscription {

        @Test
        void activeSubscriptionStreamReachesTerminalStateWhenItsTenantIsDeleted() {
            QueryBus queryBus = application.getComponent(QueryBus.class);
            MessageStream<QueryResponseMessage> stream = queryBus.subscriptionQuery(
                    new GenericQueryMessage(new MessageType(WatchAccount.class), new WatchAccount("acct-del"))
                            .andMetadata(tenantMetadata(tenantA)),
                    null,
                    50);

            // evidence the stream is live: initial result plus one delivered update
            await().atMost(Duration.ofSeconds(10)).until(stream::hasNextAvailable);
            stream.next();
            sendAndAwait(new EmitBalanceUpdate("acct-del", 1L, false), tenantA);
            await().atMost(Duration.ofSeconds(10)).until(stream::hasNextAvailable);
            stream.next();

            // when the tenant's context is deleted
            contextManager.deleteContext(tenantA);

            // then the stream reaches a terminal state within 30s: completed, or failed with an error
            await().atMost(Duration.ofSeconds(30))
                   .untilAsserted(() -> assertThat(stream.isCompleted() || stream.error().isPresent())
                           .as("active subscription stream of a deleted tenant must terminate, not hang silently"
                                       + " (completed=" + stream.isCompleted()
                                       + ", error=" + stream.error() + ")")
                           .isTrue());
        }
    }

    @Nested
    class RapidContextLifecycle {

        @Test
        void burstOfCreatedContextsAllBecomeServedTenants() {
            List<String> burst = List.of(runId + "-burst-1", runId + "-burst-2", runId + "-burst-3", runId + "-burst-4", runId + "-burst-5");
            burst.forEach(contextManager::createContext);

            for (String tenant : burst) {
                await().atMost(Duration.ofSeconds(60))
                       .pollInterval(Duration.ofMillis(500))
                       .untilAsserted(() -> assertThat(commandRoundTrips(tenant))
                               .as("tenant [" + tenant + "] created in a burst must become a served tenant")
                               .isTrue());
            }
        }

        @Test
        void deletedAndImmediatelyRecreatedContextServesCommandsAgain() {
            // given a served tenant
            await().atMost(Duration.ofSeconds(60)).until(() -> commandRoundTrips(tenantB));

            // when its context is deleted and immediately recreated
            contextManager.deleteContext(tenantB);
            contextManager.createContext(tenantB);

            // then the recreated tenant serves commands again
            await().atMost(Duration.ofSeconds(60))
                   .pollInterval(Duration.ofMillis(500))
                   .untilAsserted(() -> assertThat(commandRoundTrips(tenantB))
                           .as("recreated tenant [" + tenantB + "] must serve commands again")
                           .isTrue());
        }
    }

    private boolean commandRoundTrips(String tenant) {
        try {
            sendAndAwait(new OpenAccount("probe-" + tenant + "-" + System.nanoTime()), tenant);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void sendAndAwait(Object command, String tenant) {
        application.getComponent(CommandGateway.class)
                   .send(command, tenantMetadata(tenant), null)
                   .getResultMessage()
                   .orTimeout(15, TimeUnit.SECONDS)
                   .join();
    }
}
