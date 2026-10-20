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

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.grpc.event.dcb.AppendEventsResponse;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.FluxUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manual, long-running demo for capability negotiation: connects once, then repeatedly sources an entity using the
 * snapshot strategy, printing whether the connected node currently reports {@code dcb:snapshot-aware-sourcing} and
 * how many events came back. Meant to be run against a live Axon Server node while that node (or the cluster) is
 * being killed/restarted by hand, to observe the engine gaining/losing the capability across a reconnect. Not a
 * JUnit test - run its {@code main} method directly.
 * <p>
 * Accepts multiple {@code host:port} routing servers so the connector has somewhere to fail over to when the node
 * it is currently connected to is killed - a single seed address would just keep retrying that same, now-dead node.
 *
 * @param routingServers the {@code host:port} addresses of the Axon Server nodes to route through
 */
public record CapabilityDemoRunner(List<ServerAddress> routingServers) {

    private static final String CONTEXT = "default";
    private static final String CAPABILITY = "dcb:snapshot-aware-sourcing";

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 0) {
            System.err.println("Usage: CapabilityDemoRunner <host:port> [<host:port> ...]");
            System.exit(1);
        }
        List<ServerAddress> addresses = List.of(args).stream().map(CapabilityDemoRunner::parseAddress).toList();
        new CapabilityDemoRunner(addresses).run();
    }

    private static ServerAddress parseAddress(String hostAndPort) {
        String[] parts = hostAndPort.split(":");
        return new ServerAddress(parts[0], Integer.parseInt(parts[1]));
    }

    private void run() throws InterruptedException {
        AxonServerConnection connection = AxonServerConnectionFactory.forClient("capability-demo")
                                                                     .routingServers(routingServers.toArray(new ServerAddress[0]))
                                                                     .build()
                                                                     .connect(CONTEXT);
        EventConverter eventConverter = new DelegatingEventConverter(new ChainingContentTypeConverter());
        AxonServerEventStorageEngine engine = new AxonServerEventStorageEngine(connection, eventConverter);

        QualifiedName snapshotType = new QualifiedName("capability-demo-entity");
        String identifier = UUID.randomUUID().toString();
        Tag tag = new Tag("DEMO", identifier);
        Set<Tag> tags = Set.of(tag);
        EventCriteria criteria = EventCriteria.havingTags(tag);

        System.out.println("Connected via " + routingServers + ", entity identifier [" + identifier + "]");

        ConsistencyMarker markerAfterFirstTwoEvents = append(engine, tags, "event-0", "event-1");
        Snapshot snapshot = new Snapshot(
                new GlobalIndexPosition(GlobalIndexConsistencyMarker.position(markerAfterFirstTwoEvents)),
                "0.0.1", "snapshot-payload", Instant.now(), Map.of()
        );
        engine.store(snapshotType, identifier, snapshot, null).orTimeout(5, TimeUnit.SECONDS).join();
        append(engine, tags, "event-2");
        System.out.println("Appended 3 events and stored a snapshot right after the first 2.");

        SourcingCondition condition = SourcingCondition.conditionFor(
                new SourcingStrategy.Snapshot(snapshotType, identifier, null), criteria
        );

        while (true) {
            boolean supportsCapability = connection.supports(CAPABILITY);
            long eventCount = FluxUtils.of(engine.source(condition, null))
                                       .filter(entry -> !entry.message().getClass().getSimpleName()
                                                               .equals("TerminalEventMessage"))
                                       .count()
                                       .block();
            System.out.printf("capability=[%s] sourced %d entries (expect 3: snapshot + event-2's position is "
                                      + "already in the snapshot, so 1 snapshot + 1 trailing event)%n",
                              supportsCapability, eventCount);
            Thread.sleep(2000);
        }
    }

    @SuppressWarnings("unchecked")
    private static ConsistencyMarker append(AxonServerEventStorageEngine engine, Set<Tag> tags, String... names) {
        List<TaggedEventMessage<?>> events = List.of(names).stream()
                                                  .<TaggedEventMessage<?>>map(name -> taggedEventMessage(name, tags))
                                                  .toList();
        EventStorageEngine.AppendTransaction<AppendEventsResponse> transaction =
                (EventStorageEngine.AppendTransaction<AppendEventsResponse>)
                        engine.appendEvents(AppendCondition.none(), null, events).join();
        AppendEventsResponse response = transaction.commit().orTimeout(5, TimeUnit.SECONDS).join();
        return transaction.afterCommit(response).orTimeout(5, TimeUnit.SECONDS).join();
    }

    private static TaggedEventMessage<?> taggedEventMessage(String payload, Set<Tag> tags) {
        EventMessage event = new GenericEventMessage(new MessageType(payload, "0.0.1"), payload);
        return new GenericTaggedEventMessage<>(event, tags);
    }
}
