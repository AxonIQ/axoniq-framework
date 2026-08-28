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

package io.axoniq.framework.axonserver.connector.command;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.util.SubscriptionRecordingServer;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests the subscription behavior of the {@link AxonServerCommandBusConnector} against a stub Axon Server, asserting
 * which commands that server routes to this connector rather than which registration objects the connector holds.
 * <p>
 * The stub server and the connection to it are shared by all tests, as connecting and disconnecting outweighs
 * the tests themselves. Every test therefore subscribes a command name of its own, keeping it independent of the
 * other tests.
 *
 * @author Allard Buijze
 */
class AxonServerCommandBusConnectorSubscriptionTest {

    private static final int TEST_LOAD_FACTOR = 100;

    private static final AtomicInteger NAME_COUNTER = new AtomicInteger();

    private static SubscriptionRecordingServer axonServer;
    private static AxonServerConnectionManager connectionManager;
    private static AxonServerCommandBusConnector testSubject;

    private QualifiedName testCommand;

    @BeforeAll
    static void startAxonServer() throws Exception {
        axonServer = new SubscriptionRecordingServer();
        axonServer.start();
        AxonServerConfiguration configuration = AxonServerConfiguration.builder()
                                                                      .context("default")
                                                                      .servers(axonServer.address())
                                                                      .build();
        connectionManager = AxonServerConnectionManager.builder()
                                                       .axonServerConfiguration(configuration)
                                                       .build();
        AxonServerConnection connection = connectionManager.getConnection("default");
        testSubject = new AxonServerCommandBusConnector(connection, configuration);
        testSubject.start();
        testSubject.onIncomingCommand((command, callback) -> callback.onSuccess(null));
    }

    @AfterAll
    static void stopAxonServer() throws Exception {
        connectionManager.shutdown();
        axonServer.stop();
    }

    @BeforeEach
    void setUp() {
        testCommand = new QualifiedName("io.axoniq.test.TestCommand" + NAME_COUNTER.incrementAndGet());
    }

    @Nested
    class RepeatedSubscription {

        @Test
        void keepsCommandSubscribedAtAxonServer() {
            // given
            subscribe(testCommand, TEST_LOAD_FACTOR);

            // when
            subscribe(testCommand, TEST_LOAD_FACTOR);

            // then
            awaitProcessingOfEarlierInstructions();
            assertThat(axonServer.isCommandSubscribed(testCommand.name())).isTrue();
        }

        @Test
        void isUndoneByASingleUnsubscribe() {
            // given
            subscribe(testCommand, TEST_LOAD_FACTOR);
            subscribe(testCommand, TEST_LOAD_FACTOR);

            // when
            boolean result = testSubject.unsubscribe(testCommand);

            // then
            assertThat(result).isTrue();
            await().atMost(Duration.ofSeconds(5))
                   .untilAsserted(() -> assertThat(axonServer.isCommandSubscribed(testCommand.name())).isFalse());
        }

        @Test
        void withDifferentLoadFactorAdoptsThatLoadFactor() {
            // given
            subscribe(testCommand, TEST_LOAD_FACTOR);

            // when
            subscribe(testCommand, 42);

            // then
            await().atMost(Duration.ofSeconds(5))
                   .untilAsserted(() -> assertThat(axonServer.loadFactorOf(testCommand.name())).contains(42));
            assertThat(axonServer.isCommandSubscribed(testCommand.name())).isTrue();
        }
    }

    @Nested
    class SubscriptionAfterUnsubscribe {

        @Test
        void makesCommandRoutableAgain() {
            // given
            subscribe(testCommand, TEST_LOAD_FACTOR);
            subscribe(testCommand, TEST_LOAD_FACTOR);
            testSubject.unsubscribe(testCommand);
            await().atMost(Duration.ofSeconds(5))
                   .untilAsserted(() -> assertThat(axonServer.isCommandSubscribed(testCommand.name())).isFalse());

            // when
            subscribe(testCommand, TEST_LOAD_FACTOR);

            // then
            assertThat(axonServer.isCommandSubscribed(testCommand.name())).isTrue();
        }
    }

    private static void subscribe(QualifiedName commandName, int loadFactor) {
        CompletableFuture<Void> subscription = testSubject.subscribe(commandName, loadFactor);
        assertThat(subscription).succeedsWithin(Duration.ofSeconds(5));
    }

    /**
     * Subscribes a command of its own to await the acknowledgement of, which Axon Server only sends once it processed
     * the instructions sent before it. This makes an assertion on the absence of an unsubscribe instruction reliable
     * without waiting for a fixed amount of time.
     */
    private static void awaitProcessingOfEarlierInstructions() {
        subscribe(new QualifiedName("io.axoniq.test.Barrier" + NAME_COUNTER.incrementAndGet()), TEST_LOAD_FACTOR);
    }
}
