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

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Proves the remaining process-level faults land, each from the perturbed thing itself rather than from the absence of
 * an error: {@code SIGSTOP} shows up in the operating system's process table, a database partition shows up as dropped
 * connections and a node that can no longer answer, and a graceful stop shows up as claims released far sooner than a
 * crash's claim timeout would allow.
 */
@Tag(RigSplit.B)
class ProcessFaultsIT extends MultiJvmShardingTestBase {

    @Test
    void pausePartitionAndGracefulStopAllLand() {
        var cluster = cluster(4, 2);
        var nodeA = cluster.startNode("fault-a", cluster.defaults().withClaimTimeoutSeconds(30));
        var nodeB = cluster.startNode("fault-b", cluster.defaults().withClaimTimeoutSeconds(30));
        awaitSplit(cluster, nodeA, nodeB);

        // SIGSTOP: the process is alive but frozen, and the operating system says so.
        nodeA.pause();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(nodeA.processState()).startsWith("T"));
        assertThat(nodeA.alive()).isTrue();
        System.out.printf("EVIDENCE pause-landed %s psState=%s alive=%s%n",
                          nodeA.nodeId(), nodeA.processState(), nodeA.alive());

        nodeA.resume();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(nodeA.processState()).doesNotStartWith("T"));
        System.out.printf("EVIDENCE resume-landed %s psState=%s%n", nodeA.nodeId(), nodeA.processState());

        // Partition from the shared database: connections torn down, and the node can no longer answer a question
        // that needs the store.
        nodeB.partitionFromDatabase();
        assertThat(nodeB.databaseConnectionsDropped()).isPositive();
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThatThrownBy(nodeB::status).isNotNull());
        System.out.printf("EVIDENCE partition-landed %s droppedConnections=%d%n",
                          nodeB.nodeId(), nodeB.databaseConnectionsDropped());
        nodeB.healDatabasePartition();
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(nodeA.status()).isNotNull());

        // Graceful stop: the shutdown hook runs and releases the claims. With a 30 second claim timeout, a claim that
        // is free within seconds can only have been released, not expired - which is exactly what a crash cannot do.
        var segmentsOfA = nodeA.segments();
        var stoppedAt = System.currentTimeMillis();
        nodeA.stopGracefully();
        assertThat(nodeA.fate()).isEqualTo(RigNode.Fate.STOPPED_GRACEFULLY);
        assertThat(nodeA.alive()).isFalse();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(cluster.claimedSegmentOwners().entrySet().stream()
                                  .filter(entry -> segmentsOfA.contains(entry.getKey()))
                                  .filter(entry -> nodeA.nodeId().equals(entry.getValue())))
                        .as("a graceful stop releases the claims immediately").isEmpty());
        System.out.printf("EVIDENCE graceful-release-landed %s releasedSegments=%s afterMs=%d claimTimeoutMs=30000%n",
                          nodeA.nodeId(), segmentsOfA, System.currentTimeMillis() - stoppedAt);
    }
}
