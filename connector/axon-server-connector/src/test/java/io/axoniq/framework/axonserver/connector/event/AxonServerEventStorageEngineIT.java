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
import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.GlobalIndexPosition;
import org.axonframework.eventsourcing.eventstore.SnapshotEventMessage;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.StorageEngineTestSuite;
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.messaging.core.FluxUtils;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test suite implementation validating the {@link AxonServerEventStorageEngine}.
 *
 * @author Steven van Beelen
 */
@Testcontainers
class AxonServerEventStorageEngineIT extends StorageEngineTestSuite<AxonServerEventStorageEngine> {

    private static final String CONTEXT = "default";

    /*
     * Pinned to 2026.1.0 (not the shared canonical container) because this suite exercises the
     * AxonServerEventStorageEngine's SnapshotStore support, which requires a newer server than the
     * docker.axoniq.io/axoniq/axonserver:latest tag currently provides.
     */
    @SuppressWarnings("resource")
    @Container
    private static final AxonServerContainer container =
            new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:2026.1.0")
                    .withDevMode(true)
                    .withDcbContext(true)
                    .withReuse(true);

    private static AxonServerConnection connection;

    @AfterAll
    static void afterAll() {
        connection.disconnect();
        container.stop();
    }

    @Override
    protected AxonServerEventStorageEngine createStorageEngine() {
        container.start();
        ServerAddress address = new ServerAddress(container.getHost(), container.getGrpcPort());
        connection = AxonServerConnectionFactory.forClient("AxonServerEventStorageEngineTest")
                                                .routingServers(address)
                                                .build()
                                                .connect(CONTEXT);

        EventConverter eventConverter = new DelegatingEventConverter(new ChainingContentTypeConverter());
        return new AxonServerEventStorageEngine(connection, eventConverter);
    }

    @Override
    protected ProcessingContext processingContext() {
        return null;
    }

    @Test
    void describeTo() {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        Map<String, Object> describedProperties = descriptor.getDescribedProperties();
        assertThat(describedProperties)
                .hasSize(3)
                .containsKey("connection")
                .containsKey("converter")
                .containsKey("snapshotStore");
    }

    @Test
    void sourceWithSnapshotStrategyPrependsAStoredSnapshotBeforeSubsequentEvents() {
        // given a couple of events, then a stored snapshot, then one more event appended afterward
        QualifiedName snapshotType = new QualifiedName("test-entity");
        String identifier = UUID.randomUUID().toString();
        Tag tag = new Tag("TEST", identifier);
        Set<Tag> tags = Set.of(tag);
        EventCriteria criteria = EventCriteria.havingTags(tag);

        ConsistencyMarker markerAfterFirstTwoEvents = appendEvents(
                AppendCondition.none(),
                taggedEventMessage("event-0", tags),
                taggedEventMessage("event-1", tags)
        );

        Snapshot snapshot = new Snapshot(
                new GlobalIndexPosition(GlobalIndexConsistencyMarker.position(markerAfterFirstTwoEvents) - 1),
                "0.0.1", "snapshot-payload", Instant.now(), Map.of()
        );
        testSubject.store(snapshotType, identifier, snapshot, processingContext())
                   .orTimeout(5, TimeUnit.SECONDS)
                   .join();

        appendEvents(AppendCondition.none(), taggedEventMessage("event-2", tags));

        SourcingCondition condition = SourcingCondition.conditionFor(
                new SourcingStrategy.Snapshot(snapshotType, identifier, null), criteria
        );

        // when / then the snapshot comes back as the first entry, followed by the event appended after it
        StepVerifier.create(FluxUtils.of(testSubject.source(condition, processingContext())))
                    .assertNext(entry -> assertThat(entry.message()).isInstanceOf(SnapshotEventMessage.class))
                    .assertNext(entry -> assertThat(entry.message()).isNotInstanceOf(TerminalEventMessage.class))
                    .expectNextMatches(entry -> entry.message() instanceof TerminalEventMessage)
                    .verifyComplete();
    }
}