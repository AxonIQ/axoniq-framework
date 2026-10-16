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
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.StallingStorageEngine;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.covers;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.execution;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.publish;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.publishAndAwaitCheckpoint;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.startNode;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.storedToken;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.tokenStoreAt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests a new workflow whose STARTED event the event store refuses: the stored token stays before its start event,
 * so a restart starts the workflow.
 *
 * @author Stefan Dragisic
 */
class StartedAppendFailureTest {

    private static final Logger logger = LoggerFactory.getLogger(StartedAppendFailureTest.class);

    private final StallingStorageEngine storageEngine = new StallingStorageEngine(new InMemoryEventStorageEngine());
    private final List<AxonConfiguration> configurations = new ArrayList<>();
    private final Map<String, TrackingToken> startPositions = new HashMap<>();

    @AfterEach
    void tearDown() {
        storageEngine.failStalledAppend();
        configurations.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class StoreRefusesTheStartedAppendOfANewInstance {

        @Test
        void storedTokenStaysBeforeTheStartEventOfTheRefusedInstance() {
            // given
            var tokenStore = new InMemoryTokenStore();
            var refused = start(tokenStore);
            var workflowId = startWorkflowWithRefusedStartedAppend(refused, tokenStore);
            var startPosition = startPosition(workflowId);

            // when
            // An unrelated event gives the node a reason to checkpoint again.
            publish(refused, "later");

            // then
            await().during(Duration.ofMillis(500)).atMost(3, TimeUnit.SECONDS)
                   .until(() -> !covers(storedToken(tokenStore), startPosition));
        }

        @Test
        void restartAfterTheRefusedStartedAppendStartsTheWorkflow() {
            // given
            var tokenStore = new InMemoryTokenStore();
            var refused = start(tokenStore);
            var workflowId = startWorkflowWithRefusedStartedAppend(refused, tokenStore);
            var startPosition = startPosition(workflowId);
            publish(refused, "later");
            // Give the node the time to store whatever token it is going to store.
            await().pollDelay(Duration.ofMillis(500)).until(() -> true);
            var storedAtRestart = storedToken(tokenStore);

            // when
            var restartedTokenStore = tokenStoreAt(storedAtRestart);
            var restarted = start(restartedTokenStore);
            // Once the restarted node checkpoints a later event, it has handled everything before it.
            publishAndAwaitCheckpoint(storageEngine, restarted, restartedTokenStore, "after-restart");

            // then
            logger.info("Restart of [{}] from stored token {} (covers start event at {}: {}): durable statuses {}",
                        workflowId, storedAtRestart, startPosition, covers(storedAtRestart, startPosition),
                        storageEngine.workflowStatuses(workflowId));
            assertThat(storageEngine.workflowStatuses(workflowId))
                    .as("durable statuses of [%s] after a restart from stored token %s, start event at %s",
                        workflowId, storedAtRestart, startPosition)
                    .contains("STARTED");
        }
    }

    /**
     * Starts one workflow whose STARTED append the store refuses while the node stays up, and returns its id once the
     * instance has paused with nothing durable.
     */
    private String startWorkflowWithRefusedStartedAppend(AxonConfiguration node, TokenStore tokenStore) {
        publishAndAwaitCheckpoint(storageEngine, node, tokenStore, "warm-up");
        storageEngine.stallFirstWorkflowStartedAppend();
        var workflowId = publish(node, "start");
        startPositions.put(workflowId, storageEngine.latestPosition());
        await().atMost(5, TimeUnit.SECONDS).until(storageEngine::startedAppendStalled);
        storageEngine.failStalledAppend();
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> execution(node, workflowId).map(e -> !e.isRunning()).orElse(true));
        assertThat(execution(node, workflowId)).as("the new instance is paused, not removed")
                                               .hasValueSatisfying(e -> assertThat(e.isRunning()).isFalse());
        assertThat(storageEngine.workflowStatuses(workflowId)).as("nothing of the new instance is durable").isEmpty();
        return workflowId;
    }

    private TrackingToken startPosition(String workflowId) {
        return startPositions.get(workflowId);
    }

    private AxonConfiguration start(TokenStore tokenStore) {
        var configuration = startNode(storageEngine, tokenStore);
        configurations.add(configuration);
        return configuration;
    }
}
