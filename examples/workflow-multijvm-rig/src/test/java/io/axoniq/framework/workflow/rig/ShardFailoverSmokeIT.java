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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the rig works end to end: two real processes split the segments of one shared token store, run workflow
 * instances on both, and the survivor picks up a killed node's segments and instances.
 * <p>
 * Every claim assertion is made twice: once through each node's observation endpoint and once by reading the token
 * store's owner column directly, so a node that merely believes it owns a segment cannot make the test pass.
 */
@Tag(RigSplit.B)
class ShardFailoverSmokeIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 8;
    private static final int WORKFLOWS = 16;

    @Test
    void survivorClaimsSegmentsAndInstancesOfAKilledNode() {
        var cluster = cluster(SEGMENTS, SEGMENTS / 2);
        var nodeA = cluster.startNode("node-a");
        var nodeB = cluster.startNode("node-b");

        // 1. Genuinely separate processes against one store.
        assertThat(nodeA.pid()).isNotEqualTo(nodeB.pid());
        assertThat(nodeA.pid()).isNotEqualTo(ProcessHandle.current().pid());
        assertThat(nodeA.nodeId()).isNotEqualTo(nodeB.nodeId());
        assertThat(nodeA.storeIdentifier()).isEqualTo(nodeB.storeIdentifier());
        System.out.printf("EVIDENCE separate-processes testJvmPid=%d %s pid=%d, %s pid=%d, sharedStoreId=%s%n",
                          ProcessHandle.current().pid(),
                          nodeA.nodeId(), nodeA.pid(), nodeB.nodeId(), nodeB.pid(), nodeA.storeIdentifier());

        // 2. Segments split disjointly over both processes, read from the nodes and from the claim rows.
        awaitSplit(cluster, nodeA, nodeB);
        var segmentsOfA = nodeA.segments();
        var segmentsOfB = nodeB.segments();
        System.out.printf("EVIDENCE segment-split %s=%s %s=%s claimRows=%s%n",
                          nodeA.nodeId(), segmentsOfA, nodeB.nodeId(), segmentsOfB,
                          cluster.claimedSegmentOwners());

        // 3. Workflow instances land on both processes.
        var run = UUID.randomUUID().toString().substring(0, 8);
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(i -> "wf-" + run + "-" + i).toList();
        nodeA.startWorkflows(workflowIds);
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            assertThat(nodeA.workflows()).isNotEmpty();
            assertThat(nodeB.workflows()).isNotEmpty();
            assertThat(concat(nodeA.workflows(), nodeB.workflows()))
                    .containsExactlyInAnyOrderElementsOf(workflowIds);
        });
        var workflowsOnA = nodeA.workflows();
        var workflowsOnB = nodeB.workflows();
        System.out.printf("EVIDENCE instance-placement %s=%s %s=%s%n",
                          nodeA.nodeId(), workflowsOnA, nodeB.nodeId(), workflowsOnB);

        // Both processes actually wrote to the shared event store, and every committed event names its writer.
        var writers = writersOf(nodeB);
        assertThat(writers.keySet()).contains(nodeA.nodeId(), nodeB.nodeId());
        System.out.printf("EVIDENCE per-event-writer-attribution %s%n", writers);

        // The survivor needs room for the dead node's segments. Raising capacity at steady state is stable: a node
        // only claims segments whose tokens are free or expired, never one a live peer is extending.
        nodeB.capacity(SEGMENTS);

        // 4. kill -9. No shutdown hook runs, so the dead node's claims stay in its own name.
        nodeA.kill();
        var killedAt = System.currentTimeMillis();
        assertThat(nodeA.alive()).isFalse();
        assertThat(nodeA.fate()).isEqualTo(RigNode.Fate.CRASHED);
        var ownersRightAfterKill = cluster.claimedSegmentOwners();
        assertThat(ownersRightAfterKill).containsValue(nodeA.nodeId());
        System.out.printf("EVIDENCE fault-landed killed=%s alive=%s claimRowsStillOwnedByVictim=%s%n",
                          nodeA, nodeA.alive(),
                          ownersRightAfterKill.entrySet().stream()
                                              .filter(entry -> nodeA.nodeId().equals(entry.getValue()))
                                              .map(Map.Entry::getKey).sorted().toList());

        // 5. The survivor takes over the dead node's segments, in the claim rows and in its own view.
        await().atMost(Duration.ofSeconds(180)).untilAsserted(() -> {
            assertThat(cluster.claimedSegmentOwners()).containsOnlyKeys(allSegments());
            assertThat(cluster.claimedSegmentOwners().values()).containsOnly(nodeB.nodeId());
            assertThat(nodeB.segments()).containsAll(segmentsOfA).containsAll(segmentsOfB);
        });
        System.out.printf("EVIDENCE segments-moved %s now holds %s (was %s), claimRows=%s%n",
                          nodeB.nodeId(), nodeB.segments(), segmentsOfB, cluster.claimedSegmentOwners());

        // 6. The dead node's instances are restored on the survivor.
        await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                assertThat(nodeB.workflows()).containsExactlyInAnyOrderElementsOf(workflowIds));
        System.out.printf("EVIDENCE instances-moved %s now runs %s%n", nodeB.nodeId(), nodeB.workflows());

        // 7. The surviving process still makes forward progress after the crash: for its own instances, a step
        // committed after the kill. Instances migrated from the dead node are covered by
        // MigratedInstanceProgressIT, which currently fails on a defect this rig found.
        nodeB.resumeWorkflows(workflowIds);
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(resumedWorkflowIds(cluster)).containsAll(workflowsOnB));
        var resumeSteps = cluster.stepLog().stream().filter(entry -> "resume".equals(entry.step())).toList();
        assertThat(resumeSteps).allSatisfy(entry -> {
            assertThat(entry.nodeId()).isEqualTo(nodeB.nodeId());
            assertThat(entry.loggedAt()).isGreaterThanOrEqualTo(killedAt);
        });
        System.out.printf("EVIDENCE forward-progress-after-kill killedAt=%d stepsCommittedBySurvivorAfterKill=%d%n",
                          killedAt, resumeSteps.size());

        // 8. Nothing ran twice, on either side of the handover.
        var duplicates = cluster.stepLog().stream()
                                .collect(Collectors.groupingBy(entry -> entry.workflowId() + "/" + entry.step(),
                                                               Collectors.counting()))
                                .entrySet().stream()
                                .filter(entry -> entry.getValue() > 1)
                                .toList();
        assertThat(duplicates).as("no workflow step may be recorded twice").isEmpty();
        assertThat(cluster.stepLog().stream().filter(entry -> "start".equals(entry.step())))
                .as("every instance started exactly once").hasSize(WORKFLOWS);
    }

    @Test
    void segmentsSplitOverBothProcessesAtSixtyFourSegments() {
        var cluster = cluster(64, 32);
        var nodeA = cluster.startNode("wide-a");
        var nodeB = cluster.startNode("wide-b");

        await().atMost(Duration.ofSeconds(180)).untilAsserted(() -> {
            assertThat(nodeA.segments()).hasSize(32);
            assertThat(nodeB.segments()).hasSize(32);
            assertThat(union(nodeA.segments(), nodeB.segments())).hasSize(64);
            assertThat(cluster.claimedSegmentOwners()).hasSize(64);
            assertThat(Set.copyOf(cluster.claimedSegmentOwners().values()))
                    .containsExactlyInAnyOrder(nodeA.nodeId(), nodeB.nodeId());
        });
        System.out.printf("EVIDENCE wide-split %s=%d segments, %s=%d segments%n",
                          nodeA.nodeId(), nodeA.segments().size(), nodeB.nodeId(), nodeB.segments().size());
    }

    static void awaitSplit(ShardCluster cluster, RigNode nodeA, RigNode nodeB) {
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            assertThat(nodeA.segments()).isNotEmpty();
            assertThat(nodeB.segments()).isNotEmpty();
            assertThat(union(nodeA.segments(), nodeB.segments())).hasSize(nodeA.segments().size()
                                                                                  + nodeB.segments().size());
            assertThat(cluster.claimedSegmentOwners().values().stream().filter(Objects::nonNull)
                              .collect(Collectors.toSet())).hasSize(2);
        });
    }

    static Set<String> resumedWorkflowIds(ShardCluster cluster) {
        return cluster.stepLog().stream()
                      .filter(entry -> "resume".equals(entry.step()))
                      .map(ShardCluster.StepLogEntry::workflowId)
                      .collect(Collectors.toSet());
    }

    private static Map<String, Long> writersOf(RigNode node) {
        return node.committedEventWriters().stream()
                   .collect(Collectors.groupingBy(writer -> writer, Collectors.counting()));
    }

    private static Integer[] allSegments() {
        return IntStream.range(0, SEGMENTS).boxed().toArray(Integer[]::new);
    }

    private static Set<Integer> union(Set<Integer> left, Set<Integer> right) {
        return Stream.concat(left.stream(), right.stream()).collect(Collectors.toSet());
    }

    private static List<String> concat(List<String> left, List<String> right) {
        return Stream.concat(left.stream(), right.stream()).toList();
    }
}
