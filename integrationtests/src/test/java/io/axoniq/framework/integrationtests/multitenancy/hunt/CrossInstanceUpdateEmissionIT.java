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
 * Hunt scenario S7 (gap MT-M2): two application instances of the SAME multi-tenant application, connected to the same
 * Axon Server, serving the same tenant. A subscription registered through instance 1; the matching update is emitted
 * by a handler running on instance 2 (its command is dispatched through instance 2's gateway).
 * <p>
 * Characterisation of a documented silence: nothing in the module states whether the update crosses instances.
 * The control emit (from instance 1's own handler) proves the subscription is live.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class CrossInstanceUpdateEmissionIT {

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "cross-instance-account";

    private final Map<String, BalanceStore> stores1 = new ConcurrentHashMap<>();
    private final Map<String, BalanceStore> stores2 = new ConcurrentHashMap<>();
    private AxonConfiguration app1;
    private AxonConfiguration app2;
    private AxonServerTestInfrastructure.ContextManager contextManager;

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.deleteAllCustomContexts();
        contextManager.createContext(tenantA);
        app1 = TenantBankFixture.startApp(INFRASTRUCTURE, stores1, runId, registry -> {
        });
        app2 = TenantBankFixture.startApp(INFRASTRUCTURE, stores2, runId, registry -> {
        });
    }

    @AfterEach
    void tearDown() {
        if (app1 != null) {
            app1.shutdown();
        }
        if (app2 != null) {
            app2.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void updateEmittedOnAnotherInstanceReachesTheSubscription() {
        // given a subscription opened through instance 1
        QueryBus queryBus1 = app1.getComponent(QueryBus.class);
        MessageStream<QueryResponseMessage> stream = queryBus1.subscriptionQuery(
                new GenericQueryMessage(new MessageType(WatchAccount.class), new WatchAccount(ACCOUNT_ID))
                        .andMetadata(tenantMetadata(tenantA)),
                null,
                50);
        await().atMost(Duration.ofSeconds(10)).until(stream::hasNextAvailable);
        stream.next(); // initial result

        // control: an update emitted through instance 1 must arrive (proves the subscription is live).
        // The emitting command is dispatched via Axon Server, so either instance may HANDLE it; dispatch through
        // app2's gateway is the realistic multi-node path, dispatch through app1's is the control when it lands there.
        sendAndAwait(app1, new EmitBalanceUpdate(ACCOUNT_ID, 11L, false));
        Long controlValue = nextValueOrNull(stream, Duration.ofSeconds(10));

        // when instance 2 emits the matching update
        sendAndAwait(app2, new EmitBalanceUpdate(ACCOUNT_ID, 22L, false));
        Long crossValue = nextValueOrNull(stream, Duration.ofSeconds(10));

        assertThat(controlValue)
                .as("an update emitted while the subscription is live must arrive at least once "
                            + "(control=" + controlValue + ", cross=" + crossValue + ")")
                .isNotNull();
        assertThat(crossValue)
                .as("update emitted by a handler on the OTHER instance (silent loss when null)")
                .isNotNull();
    }

    private static Long nextValueOrNull(MessageStream<QueryResponseMessage> stream, Duration timeout) {
        try {
            await().atMost(timeout).until(stream::hasNextAvailable);
        } catch (Exception e) {
            return null;
        }
        return stream.next().orElseThrow().message().payloadAs(Long.class);
    }

    private void sendAndAwait(AxonConfiguration app, Object command) {
        app.getComponent(CommandGateway.class)
           .send(command, tenantMetadata(tenantA), null)
           .getResultMessage()
           .orTimeout(30, TimeUnit.SECONDS)
           .join();
    }
}
