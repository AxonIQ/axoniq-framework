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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
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
 * Chaos arm with a PARTIAL network fault instead of a full restart: the application connects to Axon Server through an
 * in-process TCP proxy, whose established connections are then severed abruptly (half-open from the server's point of
 * view) while Axon Server itself keeps running. New connections are allowed immediately afterwards, so the only
 * question is whether the client stack reconnects and resumes.
 * <p>
 * ORACLE: after the sever, command dispatching resumes within 2 minutes (the fault is transient and the server never
 * went away), and per-tenant conservation holds -- each tenant's sourced balance lies between its accepted deposits and
 * accepted+unknown, in its own value space (tenant A deposits 1, tenant B deposits 1000).
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class PartialNetworkFaultIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String ACCOUNT_ID = "partial-fault-account";
    private static final long AMOUNT_A = 1L;
    private static final long AMOUNT_B = 1_000L;

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private final Map<String, BalanceStore> stores = new ConcurrentHashMap<>();
    private final AtomicReference<String> lastError = new AtomicReference<>("none");
    private AxonConfiguration application;
    private AxonServerTestInfrastructure.ContextManager contextManager;
    private TcpProxy proxy;

    @BeforeEach
    void setUp() throws IOException {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.deleteAllCustomContexts();
        contextManager.createContext(tenantA);
        contextManager.createContext(tenantB);

        // The infrastructure's own AxonServerConfiguration names the real container; re-point the app at the proxy.
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
        proxy = new TcpProxy(host, port);
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
    void applicationResumesAndConservesPerTenantStateAfterConnectionsAreSevered() throws Exception {
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

            // when every established connection is severed, while Axon Server stays up and reachable
            proxy.severEstablishedConnections();

            // fault landing evidence
            await().atMost(Duration.ofSeconds(60)).until(() -> unknownA.get() + unknownB.get() > 0);

            long acceptedBefore = acceptedA.get() + acceptedB.get();
            try {
                await().atMost(Duration.ofMinutes(2))
                       .pollInterval(Duration.ofSeconds(1))
                       .until(() -> acceptedA.get() + acceptedB.get() > acceptedBefore + 10);
            } catch (Exception recoveryTimeout) {
                throw new AssertionError("No resume within 2 minutes of a transient connection break (server never "
                                                 + "went down): accepted=" + (acceptedA.get() + acceptedB.get())
                                                 + " unknown=" + (unknownA.get() + unknownB.get())
                                                 + " lastWorkloadFailure=" + lastError.get(), recoveryTimeout);
            }
        } finally {
            stop.set(true);
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }

        // then per-tenant conservation holds, in each tenant's own value space
        sendWithRetry(gateway, new RecordBalance(ACCOUNT_ID), tenantA);
        sendWithRetry(gateway, new RecordBalance(ACCOUNT_ID), tenantB);
        long balanceA = stores.get(tenantA).observedBalance(ACCOUNT_ID);
        long balanceB = stores.get(tenantB).observedBalance(ACCOUNT_ID);
        assertThat(balanceB % AMOUNT_B)
                .as("tenant B's balance must consist of tenant-B amounts only (bleed check)")
                .isZero();
        assertThat(balanceA)
                .as("tenant A: accepted=%s unknown=%s", acceptedA.get(), unknownA.get())
                .isBetween(acceptedA.get(), acceptedA.get() + unknownA.get() * AMOUNT_A)
                .isLessThan(AMOUNT_B);
        assertThat(balanceB)
                .as("tenant B: accepted=%s unknown=%s", acceptedB.get(), unknownB.get())
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
     * Minimal TCP forwarder whose established connections can be severed on demand, so a transient connection break
     * can be injected without stopping Axon Server and without a Toxiproxy dependency.
     */
    private static final class TcpProxy implements AutoCloseable {

        private final String targetHost;
        private final int targetPort;
        private final ServerSocket serverSocket;
        private final List<Socket> live = new CopyOnWriteArrayList<>();
        private final ExecutorService pumps = Executors.newCachedThreadPool();
        private final AtomicBoolean closed = new AtomicBoolean(false);

        TcpProxy(String targetHost, int targetPort) throws IOException {
            this.targetHost = targetHost;
            this.targetPort = targetPort;
            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(new InetSocketAddress("localhost", 0));
        }

        int localPort() {
            return serverSocket.getLocalPort();
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

        /** Closes every currently established socket with SO_LINGER 0, so both ends see an abrupt reset. */
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

        private static void pump(Socket from, Socket to) {
            byte[] buffer = new byte[16 * 1024];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int read;
                while ((read = in.read(buffer)) != -1) {
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
