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

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.event.AppendEventsTransaction;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import org.axonframework.eventsourcing.eventstore.AggregateBasedStorageEngineTestSuite;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.messaging.core.FluxUtils;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@Testcontainers
@Tag("slow")
class AggregateBasedAxonServerEventStorageEngineIT extends
        AggregateBasedStorageEngineTestSuite<AggregateBasedAxonServerEventStorageEngine> {

    private static final AxonServerContainer axonServerContainer = new AxonServerContainer()
            .withAxonServerHostname("localhost")
            .withDevMode(true)
            .withReuse(false);

    private AxonServerConnection connection;

    @BeforeAll
    static void beforeAll() {
        axonServerContainer.start();
    }

    @AfterAll
    static void afterAll() {
        axonServerContainer.stop();
    }

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
        AxonServerContainerUtils.purgeEventsFromAxonServer(axonServerContainer.getHost(),
                                                           axonServerContainer.getHttpPort(),
                                                           "default",
                                                           AxonServerContainerUtils.NO_DCB_CONTEXT);
        connection = AxonServerConnectionFactory.forClient("Test")
                                                .routingServers(new ServerAddress(axonServerContainer.getHost(),
                                                                                  axonServerContainer.getGrpcPort()))
                                                .build()
                                                .connect("default");
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

    /**
     * Reproduction of <a href="https://github.com/AxonIQ/AxonFramework/issues/4625">#4625</a>.
     * <p>
     * Events stored with Axon Framework 4 may not carry a payload revision (Axon Server stores it as an empty
     * {@code String}). When such an event is read back, {@link AggregateBasedAxonServerEventStorageEngine}'s
     * {@code convertToMessage} reconstructs {@code new MessageType(payload.getType(), payload.getRevision())}, and the
     * {@code MessageType} compact constructor rejects the empty version with
     * {@code "The given version is unsupported because it is empty."}.
     * <p>
     * This test stores such an event via the raw gRPC API (bypassing {@code MessageType}, which could not represent an
     * empty version) and then streams it back to observe the current failure. <b>Once the bug is fixed, this should
     * instead read the event successfully with {@link org.axonframework.messaging.core.MessageType#DEFAULT_VERSION};
     * flip the assertion to {@code assertNext(...)} at that point.</b>
     */
    @Test
    void readingEventStoredWithoutRevisionReproduces4625() throws Exception {
        // given - an event appended directly to Axon Server with an empty payload revision, as AF4 would have stored it
        appendEventWithoutRevision();

        // when - streaming the stored events from the beginning, reconstructing each into an EventMessage
        var result = testSubject.stream(StreamingCondition.startingFrom(trackingTokenAt(0)));

        // then - reconstructing the MessageType from the empty revision currently fails (#4625)
        StepVerifier.create(FluxUtils.of(result))
                    .expectErrorSatisfies(error -> assertThat(error)
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("The given version is unsupported because it is empty"))
                    .verify(Duration.ofSeconds(10));
    }

    private void appendEventWithoutRevision() throws Exception {
        Event eventWithoutRevision =
                Event.newBuilder()
                     .setMessageIdentifier(UUID.randomUUID().toString())
                     .setAggregateIdentifier("aggregate-without-revision")
                     .setAggregateType("LegacyAggregate")
                     .setAggregateSequenceNumber(0L)
                     .setTimestamp(0L)
                     .setPayload(SerializedObject.newBuilder()
                                                 .setType("com.example.LegacyEventWithoutRevision")
                                                 .setRevision("") // AF4 stored an absent revision as an empty String
                                                 .setData(ByteString.copyFromUtf8("{}"))
                                                 .build())
                     .build();

        AppendEventsTransaction tx = connection.eventChannel().startAppendEventsTransaction();
        tx.appendEvent(eventWithoutRevision);
        tx.commit().get(5, TimeUnit.SECONDS);
    }
}