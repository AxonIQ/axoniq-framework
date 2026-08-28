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

package io.axoniq.framework.axonserver.connector.util;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandServiceGrpc;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryProviderOutbound;
import io.axoniq.axonserver.grpc.query.QueryServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Stub Axon Server that acknowledges every instruction it receives and tracks which commands and queries it considers
 * subscribed at any moment.
 * <p>
 * Where a mocked {@link io.axoniq.axonserver.connector.Registration} can only show which registration object a
 * connector dropped, this stub shows what Axon Server ends up routing: a subscription is present from the moment it is
 * subscribed until it is unsubscribed. That makes it the means to verify what repeatedly subscribing the same name
 * leaves behind, which is invisible to an assertion on registration objects.
 *
 * @author Allard Buijze
 */
public class SubscriptionRecordingServer {

    private final int port;
    private final Map<String, CommandSubscription> commandSubscriptions = new ConcurrentHashMap<>();
    private final Map<String, Integer> commandSubscribeCounts = new ConcurrentHashMap<>();
    private final Map<String, Integer> querySubscribeCounts = new ConcurrentHashMap<>();
    private final Map<String, Boolean> querySubscriptions = new ConcurrentHashMap<>();

    private Server server;

    /**
     * Constructs a stub Axon Server listening on a free port.
     */
    public SubscriptionRecordingServer() {
        this.port = TcpUtils.findFreePort();
    }

    /**
     * Starts this stub Axon Server, allowing connections to be made to it.
     *
     * @throws IOException when the port this stub server was assigned cannot be bound
     */
    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                              .addService(new RecordingCommandService())
                              .addService(new RecordingQueryService())
                              .addService(new PlatformService(port))
                              .intercept(new ContextInterceptor())
                              .build();
        server.start();
    }

    /**
     * Stops this stub Axon Server, dropping any connection made to it.
     */
    public void stop() throws InterruptedException {
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * The address to configure a connection to this stub Axon Server with.
     *
     * @return the {@code host:port} address of this stub server
     */
    public String address() {
        return "localhost:" + port;
    }

    /**
     * Indicates whether this stub server currently routes the command with the given {@code commandName} to the client
     * that subscribed it.
     *
     * @param commandName the name of the command to check the subscription of
     * @return {@code true} if the command is subscribed and not unsubscribed since, {@code false} otherwise
     */
    public boolean isCommandSubscribed(String commandName) {
        return commandSubscriptions.containsKey(commandName);
    }

    /**
     * The load factor of the subscription this stub server currently holds for the given {@code commandName}.
     *
     * @param commandName the name of the command to retrieve the load factor of
     * @return the load factor of the current subscription, or an empty {@code Optional} if the command is not
     * subscribed
     */
    public Optional<Integer> loadFactorOf(String commandName) {
        return Optional.ofNullable(commandSubscriptions.get(commandName))
                       .map(CommandSubscription::getLoadFactor);
    }

    /**
     * The number of subscribe instructions this stub server received for the given {@code commandName}, counting
     * repeated subscriptions of the same name separately.
     *
     * @param commandName the name of the command to count the subscribe instructions of
     * @return the number of subscribe instructions received for the given {@code commandName}
     */
    public int commandSubscribeCount(String commandName) {
        return commandSubscribeCounts.getOrDefault(commandName, 0);
    }

    /**
     * Indicates whether this stub server currently routes the query with the given {@code queryName} to the client that
     * subscribed it.
     *
     * @param queryName the name of the query to check the subscription of
     * @return {@code true} if the query is subscribed and not unsubscribed since, {@code false} otherwise
     */
    public boolean isQuerySubscribed(String queryName) {
        return querySubscriptions.getOrDefault(queryName, false);
    }

    /**
     * The number of subscribe instructions this stub server received for the given {@code queryName}, counting repeated
     * subscriptions of the same name separately.
     *
     * @param queryName the name of the query to count the subscribe instructions of
     * @return the number of subscribe instructions received for the given {@code queryName}
     */
    public int querySubscribeCount(String queryName) {
        return querySubscribeCounts.getOrDefault(queryName, 0);
    }

    private class RecordingCommandService extends CommandServiceGrpc.CommandServiceImplBase {

        @Override
        public StreamObserver<CommandProviderOutbound> openStream(
                StreamObserver<CommandProviderInbound> responseObserver
        ) {
            return new StreamObserver<>() {
                @Override
                public void onNext(CommandProviderOutbound instruction) {
                    switch (instruction.getRequestCase()) {
                        case SUBSCRIBE -> {
                            CommandSubscription subscription = instruction.getSubscribe();
                            commandSubscriptions.put(subscription.getCommand(), subscription);
                            commandSubscribeCounts.merge(subscription.getCommand(), 1, Integer::sum);
                        }
                        case UNSUBSCRIBE -> commandSubscriptions.remove(instruction.getUnsubscribe().getCommand());
                        default -> {
                            // Nothing to record for other instructions.
                        }
                    }
                    acknowledge(instruction.getInstructionId());
                }

                @Override
                public void onError(Throwable cause) {
                    responseObserver.onError(cause);
                }

                @Override
                public void onCompleted() {
                    // Completing the response observer ends the call, as Axon Server does. Leaving it open keeps the
                    // call in flight, making a client disconnect await its full grace period before forcing the
                    // connection shut.
                    responseObserver.onCompleted();
                }

                private void acknowledge(String instructionId) {
                    if (instructionId.isEmpty()) {
                        return;
                    }
                    responseObserver.onNext(
                            CommandProviderInbound.newBuilder()
                                                  .setAck(InstructionAck.newBuilder()
                                                                        .setInstructionId(instructionId)
                                                                        .setSuccess(true))
                                                  .build()
                    );
                }
            };
        }
    }

    private class RecordingQueryService extends QueryServiceGrpc.QueryServiceImplBase {

        @Override
        public StreamObserver<QueryProviderOutbound> openStream(
                StreamObserver<QueryProviderInbound> responseObserver
        ) {
            return new StreamObserver<>() {
                @Override
                public void onNext(QueryProviderOutbound instruction) {
                    switch (instruction.getRequestCase()) {
                        case SUBSCRIBE -> {
                            String queryName = instruction.getSubscribe().getQuery();
                            querySubscriptions.put(queryName, true);
                            querySubscribeCounts.merge(queryName, 1, Integer::sum);
                        }
                        case UNSUBSCRIBE -> querySubscriptions.put(instruction.getUnsubscribe().getQuery(), false);
                        default -> {
                            // Nothing to record for other instructions.
                        }
                    }
                    acknowledge(instruction.getInstructionId());
                }

                @Override
                public void onError(Throwable cause) {
                    responseObserver.onError(cause);
                }

                @Override
                public void onCompleted() {
                    // Completing the response observer ends the call, as Axon Server does. Leaving it open keeps the
                    // call in flight, making a client disconnect await its full grace period before forcing the
                    // connection shut.
                    responseObserver.onCompleted();
                }

                private void acknowledge(String instructionId) {
                    if (instructionId.isEmpty()) {
                        return;
                    }
                    responseObserver.onNext(
                            QueryProviderInbound.newBuilder()
                                                .setAck(InstructionAck.newBuilder()
                                                                      .setInstructionId(instructionId)
                                                                      .setSuccess(true))
                                                .build()
                    );
                }
            };
        }
    }
}
