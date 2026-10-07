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
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineCheckpointingSupport;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineCheckpointingSupport.CheckpointLatchCoordinator;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.covers;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.publish;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.publishAndAwaitCheckpoint;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.startNode;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.storedToken;
import static io.axoniq.framework.workflow.configuration.StartEventCheckpointTest.tokenStoreAt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Checks that the {@link StartEventCheckpointTest} scenario detects a lost start: when the checkpoint never holds for
 * a workflow that has not started, the crash loses the workflow.
 *
 * @author Stefan Dragisic
 */
class StartEventCheckpointWithoutHoldTest {

    private static final Logger logger = LoggerFactory.getLogger(StartEventCheckpointWithoutHoldTest.class);

    private final StallingStorageEngine storageEngine = new StallingStorageEngine(new InMemoryEventStorageEngine());
    private final List<AxonConfiguration> configurations = new ArrayList<>();

    @AfterEach
    void tearDown() {
        storageEngine.failStalledAppend();
        configurations.forEach(AxonConfiguration::shutdown);
    }

    @Nested
    class WithoutTheHoldForANotStartedExecution {

        @Test
        void startBatchStoresTheStartEventAndARestartFromItNeverStartsTheWorkflow() {
            // given
            var crashedTokenStore = new InMemoryTokenStore();
            var crashed = startWithoutHold(crashedTokenStore);
            publishAndAwaitCheckpoint(storageEngine, crashed, crashedTokenStore, "warm-up");
            storageEngine.stallFirstWorkflowStartedAppend();
            var workflowId = publish(crashed, "start");
            var startPosition = storageEngine.latestPosition();
            await().atMost(5, TimeUnit.SECONDS).until(storageEngine::startedAppendStalled);
            var tokenAtCrash = storedToken(crashedTokenStore);

            // when
            var restartedTokenStore = tokenStoreAt(tokenAtCrash);
            var restarted = start(restartedTokenStore);
            // Once the restarted node checkpoints a later event, it has handled everything before it.
            publishAndAwaitCheckpoint(storageEngine, restarted, restartedTokenStore, "later");

            // then
            logger.info("Canary for [{}]: stored token at crash {}, start event at {}, durable statuses after restart {}",
                        workflowId, tokenAtCrash, startPosition, storageEngine.workflowStatuses(workflowId));
            assertThat(covers(tokenAtCrash, startPosition)).as("the start batch stored the start event").isTrue();
            assertThat(storageEngine.workflowStatuses(workflowId)).as("the start is lost").isEmpty();
        }
    }

    private AxonConfiguration startWithoutHold(TokenStore tokenStore) {
        var configuration = startNode(storageEngine, tokenStore, registry -> registry.registerComponent(
                WorkflowEngineCheckpointingSupport.class,
                cfg -> new WorkflowEngineCheckpointingSupport(new NeverHolds(
                        cfg.getComponent(WorkflowEngine.class, StartEventCheckpointTest.ENGINE)
                ))
        ));
        configurations.add(configuration);
        return configuration;
    }

    private AxonConfiguration start(TokenStore tokenStore) {
        var configuration = startNode(storageEngine, tokenStore);
        configurations.add(configuration);
        return configuration;
    }

    /**
     * The real engine as coordinator, except that an execution with queued work that has not started its body never
     * holds the segment's checkpoint.
     */
    private record NeverHolds(WorkflowEngine engine) implements CheckpointLatchCoordinator {

        @Override
        public boolean hasUnsafeCheckpointWork(Segment segment) {
            return engine.hasUnsafeCheckpointWork(segment);
        }

        @Override
        public boolean holdsCheckpoint(Segment segment) {
            return false;
        }

        @Override
        public void addCheckpointLatch(Segment segment, Runnable latch) {
            engine.addCheckpointLatch(segment, latch);
        }
    }
}
