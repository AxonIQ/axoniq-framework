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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The operation production performs constantly and every other scenario in this rig skips: a cold node joining a
 * cluster that is already running, already owns every segment and already has instances in flight.
 * <p>
 * The joining node's startup scans the segment tokens to work out where the processor is, and every one of them is
 * held by a live peer at that moment. A node that treated a peer's claim as a failure could not boot at all, so the
 * join itself is the assertion; the redistribution afterwards is what makes the joined node useful.
 * <p>
 * Ownership is read out of the token store's own claim rows, never out of a node's opinion of itself or a log line.
 */
@Tag(RigSplit.B)
class NodeJoinIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 4;
    private static final int WORKFLOWS = 16;

    @Test
    void aColdNodeJoinsAClusterThatAlreadyOwnsEverySegment() {
        var cluster = cluster(SEGMENTS, SEGMENTS);
        // A generous claim timeout, so "node A still owns everything" below can only mean the newcomer was genuinely
        // opposed, and never that a claim happened to lapse while it booted.
        var nodeA = cluster.startNode("join-a", cluster.defaults().withClaimTimeoutSeconds(60));

        // 1. One node owns the whole store, with instances running on it.
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(cluster.claimedSegmentOwners().values()).hasSize(SEGMENTS).containsOnly(nodeA.nodeId()));
        // The event store is shared by the whole suite, so a node replaying it restores instances of earlier
        // scenarios too. Only this run's own ids are this test's business.
        var prefix = "join-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(i -> prefix + i).toList();
        nodeA.startWorkflows(workflowIds);
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(ownRunInstances(nodeA, prefix)).containsExactlyInAnyOrderElementsOf(workflowIds));
        var claimsBeforeJoin = cluster.claimedSegmentOwners();
        System.out.printf("EVIDENCE before-join claimRows=%s instancesOn-%s=%s%n",
                          claimsBeforeJoin, nodeA.nodeId(), ownRunInstances(nodeA, prefix));

        // 2. The join itself: a cold node boots while every segment is claimed by the live peer. Nothing is stopped,
        //    quiesced or released first. startNode throws if the node dies during startup.
        var nodeB = cluster.startNode("join-b", cluster.defaults()
                                                      .withMaxClaimedSegments(SEGMENTS / 2)
                                                      .withClaimTimeoutSeconds(60));
        assertThat(nodeB.alive()).isTrue();
        assertThat(cluster.claimedSegmentOwners())
                .as("the peer must still have held every claim while the newcomer booted, or the join was unopposed")
                .isEqualTo(claimsBeforeJoin);
        System.out.printf("EVIDENCE join-succeeded %s booted while claimRows=%s%n", nodeB, claimsBeforeJoin);

        // 3. Make room, the way a production operator does, and let the store's claim rows show the redistribution.
        nodeA.capacity(SEGMENTS / 2);
        await().atMost(Duration.ofSeconds(180)).untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContainNull();
            assertThat(segmentsOwnedBy(owners, nodeB.nodeId())).isNotEmpty();
            assertThat(segmentsOwnedBy(owners, nodeA.nodeId())).isNotEmpty();
            assertThat(Set.copyOf(owners.values()))
                    .containsExactlyInAnyOrder(nodeA.nodeId(), nodeB.nodeId());
        });
        var claimsAfterJoin = cluster.claimedSegmentOwners();
        var movedToB = segmentsOwnedBy(claimsAfterJoin, nodeB.nodeId());
        System.out.printf("EVIDENCE segments-redistributed claimRowsBefore=%s claimRowsAfter=%s %s-gained=%s%n",
                          claimsBeforeJoin, claimsAfterJoin, nodeB.nodeId(), movedToB);
        assertThat(movedToB).as("every segment the newcomer owns was owned by the running node before the join")
                            .allMatch(segment -> nodeA.nodeId().equals(claimsBeforeJoin.get(segment)));

        // 4. The instances follow their segments, so the newcomer took over real work and not just rows in a table.
        await().atMost(Duration.ofSeconds(180)).untilAsserted(() -> {
            assertThat(ownRunInstances(nodeB, prefix)).isNotEmpty();
            assertThat(Stream.concat(ownRunInstances(nodeA, prefix).stream(),
                                     ownRunInstances(nodeB, prefix).stream()).toList())
                    .containsExactlyInAnyOrderElementsOf(workflowIds);
        });
        System.out.printf("EVIDENCE instances-followed %s=%s %s=%s%n",
                          nodeA.nodeId(), ownRunInstances(nodeA, prefix),
                          nodeB.nodeId(), ownRunInstances(nodeB, prefix));
    }

    private static List<String> ownRunInstances(RigNode node, String prefix) {
        return node.workflows().stream().filter(id -> id.startsWith(prefix)).toList();
    }

    private static Set<Integer> segmentsOwnedBy(Map<Integer, String> owners, String nodeId) {
        return owners.entrySet().stream()
                     .filter(entry -> nodeId.equals(entry.getValue()))
                     .map(Map.Entry::getKey)
                     .collect(Collectors.toCollection(TreeSet::new));
    }
}
