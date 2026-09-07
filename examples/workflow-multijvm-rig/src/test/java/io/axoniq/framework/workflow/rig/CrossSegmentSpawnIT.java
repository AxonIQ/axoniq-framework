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
import java.util.UUID;
import java.util.stream.Collectors;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A parent instance whose child lives on another node, with the parent's segment taken away while the child is still
 * running.
 * <p>
 * A workflow that starts another workflow is the one place where two instances have to cooperate, and sharding puts
 * them in different processes: the start event the parent appends is routed by the child's id, so a child id that
 * hashes to another segment starts its instance wherever that segment happens to be claimed. The two halves then have
 * nothing in common but the event store, and each half can be moved without the other noticing.
 * <p>
 * The dangerous moment is the parent waiting for its child. That wait lives on the parent's segment, the work that
 * will satisfy it lives on the child's, and the scenario moves the parent's segment while the child is outstanding.
 * If the parent's wait does not survive the move, nothing fails: the child finishes, reports back, and the parent sits
 * there forever - an orphan that looks exactly like an instance still legitimately waiting.
 * <p>
 * The split is a precondition, not a hope. Both ids are constructed from the segments' own hash masks, the mask
 * arithmetic is asserted, and the claim rows are read to confirm the two segments are held by different processes
 * before anything is published.
 */
@Tag(RigSplit.B)
class CrossSegmentSpawnIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 2;
    private static final int CAP_PER_NODE = 1;
    private static final Duration PATIENCE = Duration.ofSeconds(180);

    @Test
    void aParentAndItsChildOnAnotherSegmentBothCompleteWhenTheParentMoves() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var parentNode = cluster.startNode("spw-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var childNode = cluster.startNode("spw-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, parentNode, childNode);

        // 1. Two ids that provably hash apart, onto two segments that are provably held by different processes.
        var run = "spw-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var masks = cluster.segmentMasks();
        var parentSegment = onlySegmentOf(cluster, parentNode);
        var childSegment = onlySegmentOf(cluster, childNode);
        var parentId = ShardCluster.workflowIdsOnSegment(parentSegment, masks.get(parentSegment), run + "p-", 1)
                                   .getFirst();
        var childId = ShardCluster.workflowIdsOnSegment(childSegment, masks.get(childSegment), run + "c-", 1)
                                  .getFirst();
        System.out.printf("EVIDENCE p5-split parent=%s hashesTo=%d ownedBy=%s child=%s hashesTo=%d ownedBy=%s "
                                  + "masks=%s claimRows=%s%n",
                          parentId, parentId.hashCode() & masks.get(parentSegment), parentNode.nodeId(),
                          childId, childId.hashCode() & masks.get(childSegment), childNode.nodeId(),
                          masks, cluster.claimedSegmentOwners());
        assertThat(parentSegment).as("the two segments must differ, or there is nothing cross-segment about this")
                                 .isNotEqualTo(childSegment);
        assertThat(parentId.hashCode() & masks.get(parentSegment)).isEqualTo(parentSegment);
        assertThat(childId.hashCode() & masks.get(childSegment)).isEqualTo(childSegment);
        assertThat(cluster.claimedSegmentOwners().get(parentSegment)).isEqualTo(parentNode.nodeId());
        assertThat(cluster.claimedSegmentOwners().get(childSegment)).isEqualTo(childNode.nodeId());

        // 2. The parent spawns its child. The spawn is the parent's own step appending the child's start event; the
        //    rig never publishes it, so a child that runs proves the hand-off between the two processes happened.
        childNode.startParent(parentId, childId);
        awaitStep(cluster, parentId, "parent-spawn");
        awaitStep(cluster, childId, "child-start");
        var spawnRow = rowOf(cluster, parentId, "parent-spawn");
        var childRow = rowOf(cluster, childId, "child-start");
        System.out.printf("EVIDENCE p5-spawned parentSpawnOn=%s childStartOn=%s residentOn-%s=%s residentOn-%s=%s%n",
                          spawnRow.nodeId(), childRow.nodeId(),
                          parentNode.nodeId(), parentNode.workflows(run),
                          childNode.nodeId(), childNode.workflows(run));
        assertThat(spawnRow.nodeId()).as("the parent must run on the node owning its segment")
                                     .isEqualTo(parentNode.nodeId());
        assertThat(childRow.nodeId()).as("the child must run on the node owning the child's segment, which is the "
                                                 + "other process")
                                     .isEqualTo(childNode.nodeId());

        // 3. The parent's segment is taken away while the child is still outstanding. The child is not released
        //    until afterwards, so the parent is provably waiting on unfinished work across the move.
        assertThat(cluster.recorded(run, "child-done")).as("the child must still be running when the parent moves")
                                                       .isEmpty();
        childNode.capacity(SEGMENTS);
        parentNode.stopProcessor();
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.claimedSegmentOwners().get(parentSegment)).isEqualTo(childNode.nodeId()));
        System.out.printf("EVIDENCE p5-parent-moved segment=%d from=%s to=%s claimRows=%s childDoneYet=%s%n",
                          parentSegment, parentNode.nodeId(), childNode.nodeId(),
                          cluster.claimedSegmentOwners(), cluster.recorded(run, "child-done"));
        assertThat(cluster.recorded(run, "child-done"))
                .as("the move has to land while the child is still outstanding, or nothing was at risk")
                .isEmpty();
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(childNode.workflows(run))
                        .as("the node that took the segment must have restored the parent")
                        .contains(parentId, childId));

        // 4. Release the child. It reports back to a parent that has since moved to another process.
        childNode.releaseChildren(List.of(childId));
        await().atMost(PATIENCE).untilAsserted(() -> {
            assertThat(cluster.recorded(run, "child-done")).containsExactly(childId);
            assertThat(cluster.recorded(run, "parent-done"))
                    .as("a parent whose segment moved while its child was running must still be woken by the child; "
                                + "a parent that waits forever is the orphan this scenario exists for")
                    .containsExactly(parentId);
        });

        var rows = cluster.stepLog(run);
        System.out.printf("EVIDENCE p5-completed rows=%s counts=%s%n",
                          rows.stream().map(row -> row.workflowId() + "/" + row.step() + "@" + row.nodeId()).toList(),
                          cluster.stepExecutionCounts(run));
        assertThat(rowOf(cluster, parentId, "parent-done").nodeId())
                .as("the parent must have been finished by the node that took its segment")
                .isEqualTo(childNode.nodeId());
        assertThat(cluster.stepExecutionCounts(run))
                .as("neither half may record a step twice across the move")
                .hasSize(4)
                .allSatisfy((step, count) -> assertThat(count).isEqualTo(1L));
        assertThat(rows.stream().filter(row -> row.workflowId().equals(parentId))
                       .map(ShardCluster.StepLogEntry::step).toList())
                .containsExactly("parent-spawn", "parent-done");
        assertThat(rows.stream().filter(row -> row.workflowId().equals(childId))
                       .map(ShardCluster.StepLogEntry::step).toList())
                .containsExactly("child-start", "child-done");
    }

    private static int onlySegmentOf(ShardCluster cluster, RigNode node) {
        var segments = cluster.segmentsOwnedBy(node.nodeId());
        assertThat(segments).as("each node must hold exactly one segment for the ids to be aimable").hasSize(1);
        return segments.iterator().next();
    }

    private static void awaitStep(ShardCluster cluster, String workflowId, String step) {
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.stepLog().stream()
                                  .filter(row -> row.workflowId().equals(workflowId))
                                  .map(ShardCluster.StepLogEntry::step)
                                  .collect(Collectors.toSet()))
                        .contains(step));
    }

    private static ShardCluster.StepLogEntry rowOf(ShardCluster cluster, String workflowId, String step) {
        return cluster.stepLog().stream()
                      .filter(row -> row.workflowId().equals(workflowId) && step.equals(row.step()))
                      .findFirst()
                      .orElseThrow(() -> new AssertionError("No '" + step + "' row for " + workflowId));
    }
}
