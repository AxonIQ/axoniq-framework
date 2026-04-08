/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.extension.springboot.util;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

/**
 * A stub implementation of the {@link Server}. The only service of the server is the {@link ControlChannelStub} to
 * allow basic connections.
 * <p>
 * Use this to spoof the existence of an Axon Server instance for tests where spinning up a full instance is too much
 * effort.
 *
 * @author Marc Gathier
 * @author Steven van Beelen
 */
public class GrpcServerStub {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Default host of an Axon Server connection.
     */
    public static final String DEFAULT_HOST = "localhost";
    /**
     * Default gRPC port of an Axon Server connection.
     */
    public static final int DEFAULT_GRPC_PORT = 8124;

    private final Server server;

    /**
     * Construct a stub {@link Server} to spoof existence of an Axon Server instance.
     * <p>
     * Uses the given {@code port} to connect this server with and the constructed {@link ControlChannelStub}.
     *
     * @param grpcPort The port to connect with.
     */
    public GrpcServerStub(int grpcPort) {
        server = NettyServerBuilder.forPort(grpcPort)
                                   .addService(new ControlChannelStub(grpcPort))
                                   .build();
    }

    /**
     * Start this stub its {@link Server}.
     *
     * @throws IOException If {@link Server#start()} fails.
     */
    public void start() throws IOException {
        logger.debug("Starting stub gRPC Server...");
        server.start();
    }

    /**
     * Shutdown this stub its {@link Server}.
     *
     * @throws InterruptedException If {@link Server#awaitTermination(long, TimeUnit)} exceeds the timeout of 1 second.
     */
    public void shutdown() throws InterruptedException {
        logger.debug("Shutting down stub gRPC Server...");
        server.shutdown();
        server.awaitTermination(1, TimeUnit.SECONDS);
    }
}
