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

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import org.axonframework.eventsourcing.eventstore.AggregateBasedStorageEngineTestSuite;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.test.server.AxonServerContainer;
import org.axonframework.test.server.AxonServerContainerUtils;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@Tags({
        @Tag("slow"),
})
class AggregateBasedAxonServerEventStorageEngineIT extends
        AggregateBasedStorageEngineTestSuite<AggregateBasedAxonServerEventStorageEngine> {

    @SuppressWarnings("resource")
    private static final AxonServerContainer axonServerContainer = new AxonServerContainer()
            .withAxonServerHostname("localhost")
            .withDevMode(true);
    private static AxonServerConnection connection;

    @BeforeAll
    static void beforeAll() {
        axonServerContainer.start();
        connection = AxonServerConnectionFactory.forClient("Test")
                                                .routingServers(new ServerAddress(axonServerContainer.getHost(),
                                                                                  axonServerContainer.getGrpcPort()))
                                                .build()
                                                .connect("default");
    }

    @AfterAll
    static void afterAll() {
        connection.disconnect();
        axonServerContainer.stop();
    }

    @Test
    void sourcingFromNonGlobalSequenceTrackingTokenShouldThrowException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> testSubject.stream(StreamingCondition.startingFrom(
                        new GapAwareTrackingToken(5, Collections.emptySet())
                ))
        );
    }

    @Override
    protected AggregateBasedAxonServerEventStorageEngine buildStorageEngine() throws IOException {
        AxonServerContainerUtils.purgeEventsFromAxonServer(axonServerContainer.getHost(),
                                                           axonServerContainer.getHttpPort(),
                                                           "default",
                                                           AxonServerContainerUtils.NO_DCB_CONTEXT);
        return new AggregateBasedAxonServerEventStorageEngine(connection, converter);
    }

    @Override
    protected ProcessingContext processingContext() {
        return null;
    }

    @Override
    protected long globalSequenceOfEvent(long position) {
        return position - 1;
    }

    @Override
    protected TrackingToken trackingTokenAt(long position) {
        return new GlobalSequenceTrackingToken(globalSequenceOfEvent(position));
    }

    @Test
    void transactionCanBeCommitedOnlyOnce() {
        var tx =
                testSubject.appendEvents(AppendCondition.withCriteria(TEST_AGGREGATE_CRITERIA),
                                         processingContext(),
                                         taggedEventMessage("event-0", TEST_AGGREGATE_TAGS)).join();

        assertDoesNotThrow(() -> tx.commit().get(1, TimeUnit.SECONDS));
        assertThrows(Exception.class, () -> tx.commit().get(1, TimeUnit.SECONDS));
    }
}