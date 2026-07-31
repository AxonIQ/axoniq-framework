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

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.integrationtests.multitenancy.DisableMultiTenancyTestsWithoutLicense;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.BalanceStore;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.DepositMoney;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.OpenAccount;
import io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.RecordBalance;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Chaos arm with REPEATED faults and injected latency, closing the "single fault only" residual: three successive
 * connection-reset rounds against a two-tenant workload, with per-byte forwarding latency switched on between rounds so
 * reconnects race a degraded link rather than a clean one. Axon Server itself stays up throughout, so a resume is
 * expected after every round (a full server restart is Finding 5's territory).
 * <p>
 * ORACLE: dispatching resumes after EACH of the three rounds within 90s, and after the last round per-tenant
 * conservation holds: {@code accepted <= balance <= accepted + unknown} in each tenant's own value space (tenant A
 * deposits 1, tenant B deposits 1000), with tenant A's balance staying below one tenant-B amount.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class RepeatedFaultChaosIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "repeated-fault-account";
    private static final long AMOUNT_A = 1L;
    private static final long AMOUNT_B = 1_000L;
    private static final int FAULT_ROUNDS = 3;

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private final Map<String, BalanceStore> stores = new ConcurrentHashMap<>();
    private final AtomicReference<String> lastError = new AtomicReference<>("none");
    private AxonConfiguration application;
    private AxonServerTestInfrastructure.ContextManager contextManager;
    private LatencyProxy proxy;

    @BeforeEach
    void setUp() throws IOException {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.deleteAllCustomContexts();
        contextManager.createContext(tenantA);
        contextManager.createContext(tenantB);

        AxonConfiguration probe = null;
        String realTarget;
        try {
            probe = TenantBankFixture.startApp(INFRASTRUCTURE, new ConcurrentHashMap<>(), runId, false, r -> {
            });
            realTarget = probe.getComponent(AxonServerConfiguration.class).getServers();
        } finally {
            if (probe != null) {
                probe.shutdown();
            }
        }
        String host = realTarget.split(":")[0];
        int port = Integer.parseInt(realTarget.split(":")[1].split(",")[0]);
        proxy = new LatencyProxy(host, port);
        proxy.start();

        String proxyTarget = "localhost:" + proxy.localPort();
        application = TenantBankFixture.startApp(INFRASTRUCTURE, stores, runId, true, registry ->
                registry.registerComponent(AxonServerConfiguration.class,
                                           c -> AxonServerConfiguration.builder().servers(proxyTarget).build()));
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
        if (proxy != null) {
            proxy.close();
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void applicationSurvivesRepeatedConnectionFaultsWithInjectedLatency() throws Exception {
        CommandGateway gateway = application.getComponent(CommandGateway.class);
        sendWithRetry(gateway, new OpenAccount(ACCOUNT_ID), tenantA);
        sendWithRetry(gateway, new OpenAccount(ACCOUNT_ID), tenantB);

        AtomicLong acceptedA = new AtomicLong();
        AtomicLong acceptedB = new AtomicLong();
        AtomicLong unknownA = new AtomicLong();
        AtomicLong unknownB = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean(false);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            pool.submit(() -> workload(gateway, tenantA, AMOUNT_A, acceptedA, unknownA, stop));
            pool.submit(() -> workload(gateway, tenantB, AMOUNT_B, acceptedB, unknownB, stop));
            await().atMost(Duration.ofSeconds(30)).until(() -> acceptedA.get() > 0 && acceptedB.get() > 0);

            for (int round = 1; round <= FAULT_ROUNDS; round++) {
                long unknownBefore = unknownA.get() + unknownB.get();
                long acceptedBefore = acceptedA.get() + acceptedB.get();

                // fault: sever every established connection; from round 2 on, the link is also slowed down
                if (round > 1) {
                    proxy.injectLatency(Duration.ofMillis(25));
                }
                proxy.severEstablishedConnections();

                // fault landing evidence for THIS round
                int currentRound = round;
                await().atMost(Duration.ofSeconds(60))
                       .until(() -> unknownA.get() + unknownB.get() > unknownBefore);

                // resume evidence for THIS round
                try {
                    await().atMost(Duration.ofSeconds(90))
                           .pollInterval(Duration.ofSeconds(1))
                           .until(() -> acceptedA.get() + acceptedB.get() > acceptedBefore + 5);
                } catch (Exception recoveryTimeout) {
                    throw new AssertionError("No resume after fault round " + currentRound + " of " + FAULT_ROUNDS
                                                     + " (server up throughout, latency="
                                                     + proxy.currentLatency().toMillis() + "ms): accepted="
                                                     + (acceptedA.get() + acceptedB.get())
                                                     + " unknown=" + (unknownA.get() + unknownB.get())
                                                     + " lastWorkloadFailure=" + lastError.get(), recoveryTimeout);
                }
            }
        } finally {
            stop.set(true);
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }

        // then per-tenant conservation still holds after three fault rounds
        proxy.injectLatency(Duration.ZERO);
        sendWithRetry(gateway, new RecordBalance(ACCOUNT_ID), tenantA);
        sendWithRetry(gateway, new RecordBalance(ACCOUNT_ID), tenantB);
        long balanceA = stores.get(tenantA).observedBalance(ACCOUNT_ID);
        long balanceB = stores.get(tenantB).observedBalance(ACCOUNT_ID);
        assertThat(balanceB % AMOUNT_B)
                .as("tenant B's balance must consist of tenant-B amounts only (bleed check)")
                .isZero();
        assertThat(balanceA)
                .as("tenant A after %s fault rounds: accepted=%s unknown=%s", FAULT_ROUNDS, acceptedA.get(),
                    unknownA.get())
                .isBetween(acceptedA.get(), acceptedA.get() + unknownA.get() * AMOUNT_A)
                .isLessThan(AMOUNT_B);
        assertThat(balanceB)
                .as("tenant B after %s fault rounds: accepted=%s unknown=%s", FAULT_ROUNDS, acceptedB.get(),
                    unknownB.get())
                .isBetween(acceptedB.get(), acceptedB.get() + unknownB.get() * AMOUNT_B);
    }

    private void workload(CommandGateway gateway, String tenant, long amount, AtomicLong accepted, AtomicLong unknown,
                          AtomicBoolean stop) {
        while (!stop.get()) {
            try {
                gateway.send(new DepositMoney(ACCOUNT_ID, amount), tenantMetadata(tenant), null)
                       .getResultMessage()
                       .orTimeout(10, TimeUnit.SECONDS)
                       .join();
                accepted.addAndGet(amount);
            } catch (Exception e) {
                unknown.incrementAndGet();
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                lastError.set(cause.getClass().getSimpleName() + ": " + cause.getMessage());
                LockSupport.parkNanos(50_000_000L);
            }
        }
    }

    private static void sendWithRetry(CommandGateway gateway, Object command, String tenant) {
        await().atMost(Duration.ofMinutes(2))
               .pollInterval(Duration.ofSeconds(1))
               .until(() -> {
                   try {
                       gateway.send(command, tenantMetadata(tenant), null)
                              .getResultMessage()
                              .orTimeout(15, TimeUnit.SECONDS)
                              .join();
                       return true;
                   } catch (Exception e) {
                       return false;
                   }
               });
    }

    /**
     * TCP forwarder that can both sever its established connections and delay every forwarded chunk, so repeated
     * faults can be injected on a degraded link without a Toxiproxy dependency.
     */
    private static final class LatencyProxy implements AutoCloseable {

        private final String targetHost;
        private final int targetPort;
        private final ServerSocket serverSocket;
        private final List<Socket> live = new CopyOnWriteArrayList<>();
        private final ExecutorService pumps = Executors.newCachedThreadPool();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicReference<Duration> latency = new AtomicReference<>(Duration.ZERO);

        LatencyProxy(String targetHost, int targetPort) throws IOException {
            this.targetHost = targetHost;
            this.targetPort = targetPort;
            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(new InetSocketAddress("localhost", 0));
        }

        int localPort() {
            return serverSocket.getLocalPort();
        }

        Duration currentLatency() {
            return latency.get();
        }

        void injectLatency(Duration perChunk) {
            latency.set(perChunk);
        }

        void start() {
            pumps.submit(() -> {
                while (!closed.get()) {
                    try {
                        Socket downstream = serverSocket.accept();
                        Socket upstream = new Socket(targetHost, targetPort);
                        downstream.setSoLinger(true, 0);
                        upstream.setSoLinger(true, 0);
                        live.add(downstream);
                        live.add(upstream);
                        pumps.submit(() -> pump(downstream, upstream));
                        pumps.submit(() -> pump(upstream, downstream));
                    } catch (IOException e) {
                        if (!closed.get()) {
                            LockSupport.parkNanos(10_000_000L);
                        }
                    }
                }
            });
        }

        void severEstablishedConnections() {
            List<Socket> victims = List.copyOf(live);
            live.clear();
            victims.forEach(socket -> {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // already gone
                }
            });
        }

        private void pump(Socket from, Socket to) {
            byte[] buffer = new byte[16 * 1024];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int read;
                while ((read = in.read(buffer)) != -1) {
                    Duration delay = latency.get();
                    if (!delay.isZero()) {
                        LockSupport.parkNanos(delay.toNanos());
                    }
                    out.write(buffer, 0, read);
                    out.flush();
                }
            } catch (IOException ignored) {
                // connection severed or closed; the pump ends with it
            }
        }

        @Override
        public void close() {
            closed.set(true);
            severEstablishedConnections();
            try {
                serverSocket.close();
            } catch (IOException ignored) {
                // nothing further to do
            }
            pumps.shutdownNow();
        }
    }
}
