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
import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DefaultAxonApplication;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.builder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@link ChaosSingleTenantControlIT} is the test that shows the apparent bug from AxoniQ/axoniq-framework#320:
 * command dispatch looks like it never recovers from an Axon Server restart. This test is the proof that it is not
 * a real bug. It runs the exact same restart-under-workload chaos, but against an Axon Server container published
 * on a FIXED host port instead of Testcontainers' default dynamic one. {@link DockerRestartPortStabilityIT} shows
 * why the dynamic port matters: {@code docker restart} can reassign it on some Docker setups, observed on macOS
 * with Docker Desktop. With a fixed port, recovery here is fast, well inside 30 seconds, not "eventually within 3
 * minutes" like the dynamic-port version.
 * <p>
 * Deliberately self-contained. It has no dependency on {@link TenantBankFixture} or event-sourced entities, only a
 * stateless echo command handler, so this proof stays independent of anything else in this package or module. It
 * uses its own dedicated container on hardcoded fixed ports rather than the shared
 * {@link io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure} instance, since
 * a fixed port cannot be shared safely with whatever else that infrastructure's dynamically-ported container is
 * doing. Do not run this test concurrently with another instance of itself.
 */
class ChaosFixedPortRecoveryIT {

    private static final int FIXED_HTTP_PORT = 19024;
    private static final int FIXED_GRPC_PORT = 19124;
    private static final QualifiedName PING_COMMAND = new QualifiedName(PingCommand.class);

    private final AtomicReference<String> lastError = new AtomicReference<>("none");
    private FixedPortAxonServerContainer container;
    private AxonConfiguration application;

    private record PingCommand() {

    }

    /** Exposes {@code addFixedExposedPort}, which {@link AxonServerContainer} does not itself publish. */
    private static final class FixedPortAxonServerContainer extends AxonServerContainer {

        FixedPortAxonServerContainer() {
            super("docker.axoniq.io/axoniq/axonserver:latest");
            addFixedExposedPort(FIXED_HTTP_PORT, 8024);
            addFixedExposedPort(FIXED_GRPC_PORT, 8124);
        }
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
        if (container != null) {
            container.stop();
        }
    }

    @Test
    void singleTenantApplicationRecoversQuicklyAfterRestartWhenAxonServerUsesAFixedPort() throws Exception {
        container = new FixedPortAxonServerContainer();
        container.withAxonServerHostname("localhost");
        container.withDevMode(true);
        container.withDcbContext(true);
        container.start();
        String containerId = container.getContainerId();

        AxonServerConfiguration.Builder configBuilder = builder().servers("localhost:" + FIXED_GRPC_PORT);
        CommandHandlingModule.CommandHandlerPhase commandHandlingModule =
                CommandHandlingModule.named("fixed-port-ping-module")
                                     .commandHandlers()
                                     .commandHandler(PING_COMMAND, this::handlePing);

        application = new DefaultAxonApplication()
                .componentRegistry(cr -> cr.registerComponent(AxonServerConfiguration.class,
                                                              c -> configBuilder.build()))
                .componentRegistry(cr -> cr.registerModule(commandHandlingModule.build()))
                .start();

        CommandGateway gateway = application.getComponent(CommandGateway.class);
        sendAndAwait(gateway);

        AtomicLong accepted = new AtomicLong();
        AtomicLong unknown = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean(false);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int t = 0; t < 2; t++) {
                pool.submit(() -> {
                    while (!stop.get()) {
                        try {
                            sendAndAwait(gateway);
                            accepted.incrementAndGet();
                        } catch (Exception e) {
                            unknown.incrementAndGet();
                            Throwable cause = e.getCause() != null ? e.getCause() : e;
                            lastError.set(cause.getClass().getSimpleName() + ": " + cause.getMessage());
                            LockSupport.parkNanos(50_000_000L);
                        }
                    }
                });
            }
            await().atMost(Duration.ofSeconds(30)).until(() -> accepted.get() > 10);

            restartAxonServerContainer(containerId);
            await().atMost(Duration.ofSeconds(60)).until(() -> unknown.get() > 0);

            long acceptedBeforeRecovery = accepted.get();
            // A tight 30s bound, not the 3 minutes ChaosSingleTenantControlIT allows: with a stable port, recovery
            // is expected to be fast, not merely eventual.
            try {
                await().atMost(Duration.ofSeconds(30))
                       .pollInterval(Duration.ofSeconds(1))
                       .until(() -> accepted.get() > acceptedBeforeRecovery + 10);
            } catch (Exception recoveryTimeout) {
                throw new AssertionError("Fixed-port single-tenant app did not recover within 30 seconds of the "
                                                 + "restart: accepted=" + accepted.get() + " unknown=" + unknown.get()
                                                 + " lastWorkloadFailure=" + lastError.get(), recoveryTimeout);
            }
        } finally {
            stop.set(true);
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }
    }

    private MessageStream.Single<CommandResultMessage> handlePing(CommandMessage command, ProcessingContext context) {
        return MessageStream.just(new GenericCommandResultMessage(new MessageType(String.class), "pong"));
    }

    private static void sendAndAwait(CommandGateway gateway) {
        gateway.send(new PingCommand(), Metadata.emptyInstance(), null)
               .getResultMessage()
               .orTimeout(10, TimeUnit.SECONDS)
               .join();
    }

    private static void restartAxonServerContainer(String containerId) throws IOException, InterruptedException {
        Process restart = new ProcessBuilder("docker", "restart", containerId).inheritIO().start();
        if (!restart.waitFor(90, TimeUnit.SECONDS) || restart.exitValue() != 0) {
            throw new IllegalStateException("docker restart failed for container " + containerId);
        }
    }
}
