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
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.OpenAccount;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.RecordBalance;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.SlowDeposit;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Hunt scenario S3 (claim MT-C8, gap MT-M4): a command whose handler already entered before shutdown was initiated
 * must complete successfully, and its event must be durably appended to the tenant's store -- the per-tenant Axon
 * Server connection it needs for the append is shared with the query connector, whose disconnect this branch extends
 * with a full {@code connection.disconnect()}.
 * <p>
 * Verified end to end: the dispatch future completes normally, and a FRESH application sourcing the account sees the
 * deposited amount.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class GracefulShutdownInFlightWorkIT {

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "shutdown-account";

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

    @Test
    void commandInFlightWhenShutdownStartsCompletesAndItsEventIsDurable() {
        CommandGateway gateway = application.getComponent(CommandGateway.class);
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID));

        // given a slow deposit whose handler has observably entered
        CompletableFuture<? extends Message> inFlight =
                gateway.send(new SlowDeposit(ACCOUNT_ID, 42L, 1_500L), tenantMetadata(tenantA), null)
                       .getResultMessage();
        await().atMost(Duration.ofSeconds(10))
               .until(() -> stores.get(tenantA) != null
                       && "started".equals(stores.get(tenantA).note("slow-" + ACCOUNT_ID)));

        // when the application shuts down while the handler is still sleeping
        application.shutdown();
        AxonConfiguration stopped = application;
        application = null;

        // then the accepted command completes successfully
        Throwable inFlightFailure = inFlight.orTimeout(30, TimeUnit.SECONDS)
                                            .handle((result, failure) -> failure)
                                            .join();
        assertThat(inFlightFailure)
                .as("a command whose handler entered before shutdown must complete, not be severed")
                .isNull();

        // and its event is durable: a fresh application sources the deposited amount
        Map<String, BalanceStore> freshStores = new ConcurrentHashMap<>();
        application = TenantBankFixture.startApp(INFRASTRUCTURE, freshStores, runId, registry -> {
        });
        CommandGateway freshGateway = application.getComponent(CommandGateway.class);
        sendAndAwait(freshGateway, new RecordBalance(ACCOUNT_ID));
        assertThat(freshStores.get(tenantA).observedBalance(ACCOUNT_ID))
                .as("the in-flight deposit must be in tenant A's store after the restart")
                .isEqualTo(42L);
    }

    private void sendAndAwait(CommandGateway gateway, Object command) {
        gateway.send(command, tenantMetadata(tenantA), null)
               .getResultMessage()
               .orTimeout(30, TimeUnit.SECONDS)
               .join();
    }
}
