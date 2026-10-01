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
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import org.axonframework.eventsourcing.eventstore.AggregateBasedStorageEngineTestSuite;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
@Tag("slow")
class AggregateBasedAxonServerEventStorageEngineIT extends
        AggregateBasedStorageEngineTestSuite<AggregateBasedAxonServerEventStorageEngine> {

    /*
     * A context of its own, rather than the shared container's DCB "default" context: this suite asserts
     * GapAwareTrackingToken/non-DCB aggregate-based semantics, which needs a non-DCB context -- a different
     * configuration than "default" carries. Named after the property it needs, not this suite, so any other
     * non-DCB-dependent suite could use it too.
     */
    private static final String CONTEXT = "non-dcb";

    private static final AxonServerContainer axonServerContainer = SharedAxonServerContainer.INSTANCE;

    private AxonServerConnection connection;

    @AfterEach
    void tearDown() {
        connection.disconnect();
    }

    @Test
    void sourcingFromNonGlobalSequenceTrackingTokenShouldThrowException() {
        assertThatThrownBy(() -> testSubject.stream(StreamingCondition.startingFrom(
                new GapAwareTrackingToken(5, Collections.emptySet())
        ))).isInstanceOf(IllegalArgumentException.class);
    }

    @Override
    protected AggregateBasedAxonServerEventStorageEngine buildStorageEngine() throws IOException {
        SharedAxonServerContainer.ensureStarted();

        try {
            AxonServerContainerUtils.deleteContext(axonServerContainer.getHost(), axonServerContainer.getHttpPort(), CONTEXT);
        } catch (IOException ignored) {
            // Context didn't exist yet.
        }
        AxonServerContainerUtils.createContext(axonServerContainer.getHost(),
                                               axonServerContainer.getHttpPort(),
                                               CONTEXT,
                                               AxonServerContainerUtils.NO_DCB_CONTEXT);

        connection = AxonServerConnectionFactory.forClient("Test")
                                                .routingServers(new ServerAddress(axonServerContainer.getHost(),
                                                                                  axonServerContainer.getGrpcPort()))
                                                .build()
                                                .connect(CONTEXT);
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

        assertThatNoException().isThrownBy(() -> tx.commit().get(1, TimeUnit.SECONDS));
        assertThatThrownBy(() -> tx.commit().get(1, TimeUnit.SECONDS))
                .isInstanceOf(Exception.class);
    }
}