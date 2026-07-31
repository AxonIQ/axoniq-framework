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
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Minimal reproducer isolating the sourced-state anomaly the larger scenarios tripped over (claim MT-C4): an
 * acknowledged append must be observable by the next command sourcing the same entity in the same tenant.
 * <p>
 * Two arms: the same application that appended (read-your-writes), and a second application on the same Axon Server
 * (append durability). A red first arm with a green second arm indicts the appending application's read path; both
 * red indicts the append itself.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantReadYourWritesIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "ryw-account";

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";

    private final Map<String, BalanceStore> stores = new ConcurrentHashMap<>();
    private final Map<String, BalanceStore> stores2 = new ConcurrentHashMap<>();
    private AxonConfiguration application;
    private AxonConfiguration application2;
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
        if (application2 != null) {
            application2.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void acknowledgedAppendIsSourcedByTheNextCommandInTheSameTenant() {
        CommandGateway gateway = application.getComponent(CommandGateway.class);

        // given an acknowledged AccountOpened append
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID));

        // when the next command sources the same entity in the same tenant, in the same application
        sendAndAwait(gateway, new RecordBalance(ACCOUNT_ID));

        // then the sourced state contains the acknowledged event
        assertThat(stores.get(tenantA).note("open-" + ACCOUNT_ID))
                .as("read-your-writes: the appending application must source its own acknowledged append")
                .isEqualTo("true");
    }

    @Test
    void acknowledgedAppendIsSourcedByASecondApplication() {
        CommandGateway gateway = application.getComponent(CommandGateway.class);
        sendAndAwait(gateway, new OpenAccount(ACCOUNT_ID));
        // The first application stops before the second starts, so Axon Server cannot route the probe back to it.
        application.shutdown();
        application = null;

        // when a fresh application sources the same entity in the same tenant
        application2 = TenantBankFixture.startApp(INFRASTRUCTURE, stores2, runId, registry -> {
        });
        sendAndAwait(application2.getComponent(CommandGateway.class), new RecordBalance(ACCOUNT_ID));

        // then the acknowledged event is durable and observable
        assertThat(stores2.get(tenantA).note("open-" + ACCOUNT_ID))
                .as("durability: a second application must source the acknowledged append")
                .isEqualTo("true");
    }

    private void sendAndAwait(CommandGateway gateway, Object command) {
        gateway.send(command, tenantMetadata(tenantA), null)
               .getResultMessage()
               .orTimeout(30, TimeUnit.SECONDS)
               .join();
    }
}
