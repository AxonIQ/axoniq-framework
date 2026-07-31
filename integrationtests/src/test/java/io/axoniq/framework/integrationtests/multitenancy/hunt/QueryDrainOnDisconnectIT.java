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
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.SlowQuery;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
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
 * Hunt scenario probing the query-connector drain on disconnect (claim MT-C8 territory, adjacent to the shutdown
 * finding): a query that is still being handled when the application shuts down must either complete or fail
 * definitively -- and the shutdown must not silently abandon it.
 * <p>
 * ORACLE: with a query in flight (its handler entered, sleeping), {@code application.shutdown()} returns and the
 * in-flight query's stream reaches a terminal state (a value, or an error) within 30s. A stream that neither completes
 * nor errors is an abandoned in-flight query -- the shape the connector's drain is supposed to prevent.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class QueryDrainOnDisconnectIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";

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
        TenantBankFixture.slowQueryEntered.set(false);
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
    void queryInFlightAtShutdownReachesATerminalState() {
        // Weak oracle, kept as the baseline: whatever the drain does, the caller must not be left hanging.
        Object outcome = dispatchSlowQueryAndShutdown(2_000L);
        assertThat(outcome)
                .as("an in-flight query at shutdown must complete or fail definitively, not be abandoned")
                .isNotNull()
                .isNotEqualTo("future-failed: TimeoutException");
    }

    /**
     * The strict oracle for the connector's drain, independent of the shutdown-latency finding: the connector's
     * {@code queryInProgressAwait} budget is 5s, so a query that needs 2s of handler time and was already in flight
     * when the disconnect started MUST be allowed to finish. Receiving its value proves the drain waited; an error or
     * a cancellation proves it did not -- which is what an in-progress map that is empty by construction
     * ({@code result.onClose(queriesInProgress.remove(id))} evaluates the remove immediately) would produce.
     */
    @Test
    void queryWellWithinTheDrainBudgetStillProducesItsResult() {
        Object outcome = dispatchSlowQueryAndShutdown(2_000L);
        assertThat(outcome)
                .as("a 2s query in flight when disconnect starts is inside the connector's 5s drain budget, so the "
                            + "drain must let it finish rather than cancelling it")
                .isEqualTo(2_000L);
    }

    private Object dispatchSlowQueryAndShutdown(long handlerMillis) {
        QueryBus queryBus = application.getComponent(QueryBus.class);

        // given a query whose handler has observably entered and is still running
        CompletableFuture<Object> inFlight = CompletableFuture.supplyAsync(() -> {
            var stream = queryBus.query(
                    new GenericQueryMessage(new MessageType(SlowQuery.class), new SlowQuery(handlerMillis))
                            .andMetadata(tenantMetadata(tenantA)),
                    null);
            await().atMost(Duration.ofSeconds(30)).until(() -> stream.hasNextAvailable() || stream.isCompleted()
                    || stream.error().isPresent());
            return stream.error().isPresent()
                    ? "error: " + stream.error().get().getClass().getSimpleName()
                    : (stream.hasNextAvailable() ? stream.next().orElseThrow().message().payloadAs(Long.class)
                            : "completed-empty");
        });
        await().atMost(Duration.ofSeconds(15)).until(TenantBankFixture.slowQueryEntered::get);

        // when the application shuts down while the query is in flight
        application.shutdown();
        application = null;

        return inFlight.orTimeout(45, TimeUnit.SECONDS)
                       .handle((result, failure) -> failure != null
                               ? "future-failed: " + failure.getClass().getSimpleName()
                               : result)
                       .join();
    }
}
