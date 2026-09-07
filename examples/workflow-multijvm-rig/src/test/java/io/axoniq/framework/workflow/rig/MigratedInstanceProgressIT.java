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
package io.axoniq.framework.workflow.rig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.resumedWorkflowIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The property that makes sharded failover worth having: an instance whose segment moves to another node must keep
 * running there.
 * <p>
 * This test is red at the time of writing, and the failure is a product defect, not a rig problem. The survivor does
 * restore the instances - {@code ShardFailoverSmokeIT} proves that with the observation channel - but they never react
 * to the event they are waiting for. The node log shows why, once per migrated instance:
 * <pre>
 * WaitForDelegate: Failed to publish completed event for step 'waitForResume':
 *   IllegalStateException: Failed to register handler in phase PREPARE_COMMIT (20000).
 *   ProcessingContext is already in phase COMMIT (30000).
 * </pre>
 * The segment-claim callback hands restored workflow bodies the processing context of the short-lived unit of work it
 * creates for the claim, and that unit of work commits as soon as the callback returns. From then on the restored body
 * cannot append anything, so its wait can never complete. It is the same hazard the engine documents on
 * {@code WorkflowEngine#start}, on the segment-claim path instead of the startup path.
 */
@Tag(RigSplit.A)
class MigratedInstanceProgressIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 8;
    private static final int WORKFLOWS = 16;

    @Test
    void instancesMigratedOnSegmentClaimResumeOnTheSurvivor() {
        var cluster = cluster(SEGMENTS, SEGMENTS / 2);
        var nodeA = cluster.startNode("mig-a");
        var nodeB = cluster.startNode("mig-b");
        awaitSplit(cluster, nodeA, nodeB);

        var run = UUID.randomUUID().toString().substring(0, 8);
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(i -> "mig-" + run + "-" + i).toList();
        nodeA.startWorkflows(workflowIds);
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            assertThat(nodeA.workflows()).isNotEmpty();
            assertThat(nodeB.workflows()).isNotEmpty();
        });
        var migrating = nodeA.workflows();

        nodeB.capacity(SEGMENTS);
        nodeA.kill();
        assertThat(nodeA.alive()).isFalse();

        await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(nodeB.workflows()).containsAll(migrating));
        System.out.printf("EVIDENCE migrated-instances-restored %s%n", migrating);

        nodeB.resumeWorkflows(workflowIds);
        await().atMost(Duration.ofSeconds(90)).untilAsserted(() ->
                assertThat(resumedWorkflowIds(cluster))
                        .as("instances migrated from the killed node must progress on the survivor")
                        .containsAll(migrating));

        assertThat(cluster.stepLog().stream()
                          .filter(entry -> "resume".equals(entry.step()))
                          .map(ShardCluster.StepLogEntry::nodeId))
                .containsOnly(nodeB.nodeId());
    }
}
