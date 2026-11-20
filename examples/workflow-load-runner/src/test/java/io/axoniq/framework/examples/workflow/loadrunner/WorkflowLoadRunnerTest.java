/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.examples.workflow.loadrunner;

import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs the {@link WorkflowLoadRunner} end to end on an in-memory event store, so it needs no Axon Server.
 *
 * @author Allard Buijze
 */
class WorkflowLoadRunnerTest {

    private static final int INSTANCES = 200;

    private WorkflowLoadRunner runner;

    @AfterEach
    void tearDown() {
        runner.close();
    }

    @Test
    void startsTheInstancesAndCompletesThemThroughApprovalOrTimeout() throws InterruptedException {
        // given - half of the instances are approved, the other half time out after five seconds
        runner = new WorkflowLoadRunner(LoadRunnerOptions.parse(
                String.valueOf(INSTANCES), "--in-memory-events", "--wait=PT5S", "--complete=0.5", "--segments=4"
        ));
        var startup = runner.startUp();
        assertThat(startup.inProgress()).isZero();

        // when
        var start = runner.startInstances();

        // then
        assertThat(start.instances()).isEqualTo(INSTANCES);
        assertThat(runner.statistics().published()).isEqualTo(INSTANCES);
        assertThat(runner.statistics().startedThisRun()).isEqualTo(INSTANCES);
        await().atMost(Duration.ofSeconds(60))
               .untilAsserted(() -> assertThat(runner.statistics().completed()).isEqualTo(INSTANCES));
        assertThat(runner.statistics().approvalsPublished()).isEqualTo(INSTANCES / 2);
        assertThat(runner.statistics().failed()).isZero();
        assertThat(runner.inProgress()).isZero();
    }
}
