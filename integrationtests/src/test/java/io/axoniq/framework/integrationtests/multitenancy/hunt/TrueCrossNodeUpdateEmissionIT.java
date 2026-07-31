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
 * Hunt scenario for gap MT-M2, in its decisive form: the emitting command handler exists on EXACTLY ONE instance, so
 * the emission provably happens on a different node than the one holding the subscription registration.
 * <p>
 * app-1 has NO command handlers -- it only opens the subscription query, so its {@code DistributedQueryBus} holds the
 * registration. app-2 has the command handlers, so Axon Server routes {@link EmitBalanceUpdate} there, and the emit
 * runs against app-2's own (separate) update registry.
 * <p>
 * ORACLE: the subscriber on app-1 receives the update emitted on app-2 within 15s. A silent non-delivery (with the
 * emitting command reporting success) is the cross-node silent-loss shape.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class TrueCrossNodeUpdateEmissionIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "cross-node-account";

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";

    private final Map<String, BalanceStore> storesSubscriber = new ConcurrentHashMap<>();
    private final Map<String, BalanceStore> storesEmitter = new ConcurrentHashMap<>();
    private AxonConfiguration subscriberApp;
    private AxonConfiguration emitterApp;
    private AxonServerTestInfrastructure.ContextManager contextManager;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.deleteAllCustomContexts();
        contextManager.createContext(tenantA);
        // app-1: query side only -- no command handlers, so it can never handle the emitting command
        subscriberApp = TenantBankFixture.startApp(INFRASTRUCTURE, storesSubscriber, runId, false, registry -> {
        });
        // app-2: the only instance with command handlers
        emitterApp = TenantBankFixture.startApp(INFRASTRUCTURE, storesEmitter, runId, true, registry -> {
        });
    }

    @AfterEach
    void tearDown() {
        if (subscriberApp != null) {
            subscriberApp.shutdown();
        }
        if (emitterApp != null) {
            emitterApp.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void subscriptionOnOneNodeReceivesUpdateEmittedOnAnotherNode() {
        // given a subscription registered through the node that has NO command handlers
        MessageStream<QueryResponseMessage> stream = subscriberApp.getComponent(QueryBus.class).subscriptionQuery(
                new GenericQueryMessage(new MessageType(WatchAccount.class), new WatchAccount(ACCOUNT_ID))
                        .andMetadata(tenantMetadata(tenantA)),
                null,
                50);
        await().atMost(Duration.ofSeconds(15)).until(stream::hasNextAvailable);
        assertThat(stream.next().orElseThrow().message().payloadAs(Long.class))
                .as("initial result proves the subscription is live on the subscriber node")
                .isEqualTo(0L);

        // when the emitting command is dispatched -- only the other node can handle it
        emitterApp.getComponent(CommandGateway.class)
                  .send(new EmitBalanceUpdate(ACCOUNT_ID, 77L, false), tenantMetadata(tenantA), null)
                  .getResultMessage()
                  .orTimeout(30, TimeUnit.SECONDS)
                  .join();

        // then the update reaches the subscription held on the other node
        Long received = null;
        try {
            await().atMost(Duration.ofSeconds(15)).until(stream::hasNextAvailable);
            received = stream.next().orElseThrow().message().payloadAs(Long.class);
        } catch (Exception timeout) {
            // fall through: null means the emit reported success while nothing was delivered
        }
        assertThat(received)
                .as("update emitted on the node WITHOUT the subscription registration must still reach the "
                            + "subscriber; null means silent cross-node loss (the emitting command succeeded)")
                .isEqualTo(77L);
    }
}
