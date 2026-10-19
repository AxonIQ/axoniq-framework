/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.event;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.axoniq.framework.axonserver.connector.util.ContextInterceptor;
import io.axoniq.framework.axonserver.connector.util.PlatformService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class StubServer {

    private final Server server;
    private final PlatformService platformService;
    private final int port;

    public StubServer(int port) {
        this(port, port);
    }

    public StubServer(int port, int redirectPort) {
        this(port, new PlatformService(redirectPort));
    }

    public StubServer(int port, PlatformService platformService) {
        this(port, platformService, new EventStoreImpl());
    }

    public StubServer(int port, PlatformService platformService, EventStoreImpl eventStore) {
        this.port = port;
        server = NettyServerBuilder.forPort(port)
                                   .addService(eventStore)
                                   .addService(platformService)
                                   .intercept(new ContextInterceptor())
                                   .build();
        this.platformService = platformService;
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() throws InterruptedException {
        server.shutdown();
        server.awaitTermination(1, TimeUnit.SECONDS);
    }

    public int getPort() {
        return port;
    }

    public PlatformService getPlatformService() {
        return platformService;
    }
}
