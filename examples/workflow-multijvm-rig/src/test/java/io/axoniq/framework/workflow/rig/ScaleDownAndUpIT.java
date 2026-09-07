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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The full elasticity ladder on one set of instances: two nodes down to one, back up to two, all the way down to none,
 * and back to two again.
 * <p>
 * Each rung is covered somewhere in this rig on its own. None of them is covered in sequence, and the sequence is what
 * an autoscaler does. The instances here are never restarted or re-published: the same twelve have to survive being
 * concentrated onto one process, spread again onto a process that never saw them, abandoned entirely, and finally
 * restored by two processes that are the third generation to hold them.
 * <p>
 * The rung with nothing else like it is the return from zero. The token store is not empty then - it is full of claims
 * naming processes that no longer exist, with tokens sitting in the middle of the stream - and both replacements run
 * their startup scan over those rows at the same moment. Reaching that state needs the nodes killed rather than
 * stopped, so nothing releases anything on the way out, and the stale claims are read out of the claim rows and
 * asserted before the replacements are started.
 * <p>
 * The oracle throughout is the step log, filtered to this run's ids, and the property is exactly-once across all four
 * transitions.
 */
@Tag(RigSplit.B)
class ScaleDownAndUpIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 4;
    private static final int CAP_PER_NODE = SEGMENTS / 2;
    private static final int WORKFLOWS = 12;
    private static final Duration PATIENCE = Duration.ofSeconds(240);

    @Test
    void instancesSurviveScalingDownToNothingAndBackUp() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var first = cluster.startNode("scl-a1", cluster.defaults().withTokenClaimIntervalMs(500));
        var second = cluster.startNode("scl-b1", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, first, second);

        var run = "scl-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(index -> run + index).toList();
        first.startWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.recorded(run, "start")).containsExactlyInAnyOrderElementsOf(workflowIds));
        var positionsAtTwo = cluster.storedSegmentPositions();
        System.out.printf("EVIDENCE p6-at-two claimRows=%s storedPositions=%s instances=%d%n",
                          cluster.claimedSegmentOwners(), positionsAtTwo, WORKFLOWS);

        // Two to one. A clean scale-in: the departing node releases, the survivor absorbs everything.
        first.capacity(SEGMENTS);
        second.stopGracefully();
        await().atMost(PATIENCE).untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).containsOnly(first.nodeId());
        });
        awaitResident(run, workflowIds, first);
        System.out.printf("EVIDENCE p6-scaled-in survivor=%s claimRows=%s resident=%d storedPositions=%s%n",
                          first.nodeId(), cluster.claimedSegmentOwners(),
                          first.workflows(run).size(), cluster.storedSegmentPositions());

        // One to two, onto a process that has never seen these instances.
        var third = cluster.startNode("scl-c1", cluster.defaults().withTokenClaimIntervalMs(500));
        first.capacity(CAP_PER_NODE);
        await().atMost(PATIENCE).untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContainNull();
            assertThat(Set.copyOf(owners.values())).containsExactlyInAnyOrder(first.nodeId(), third.nodeId());
        });
        awaitResident(run, workflowIds, first, third);
        System.out.printf("EVIDENCE p6-scaled-out joined=%s gained=%s claimRows=%s resident=%s%n",
                          third.nodeId(), cluster.segmentsOwnedBy(third.nodeId()), cluster.claimedSegmentOwners(),
                          List.of(first.workflows(run).size(), third.workflows(run).size()));

        // Two to zero, hard, so the store is left full of claims naming processes that no longer exist.
        var positionsBeforeZero = cluster.storedSegmentPositions();
        first.kill();
        third.kill();
        var staleClaims = cluster.claimedSegmentOwners();
        System.out.printf("EVIDENCE p6-at-zero aAlive=%s cAlive=%s staleClaimRows=%s storedPositions=%s%n",
                          first.alive(), third.alive(), staleClaims, positionsBeforeZero);
        assertThat(first.alive()).isFalse();
        assertThat(third.alive()).isFalse();
        assertThat(staleClaims).hasSize(SEGMENTS);
        assertThat(Set.copyOf(staleClaims.values()))
                .as("scaling to zero by killing leaves every claim behind; a released claim would make the return "
                            + "from zero an ordinary cold start")
                .containsExactlyInAnyOrder(first.nodeId(), third.nodeId());
        assertThat(positionsBeforeZero.values())
                .as("the return from zero must scan tokens that are somewhere in the stream, not at its start")
                .allSatisfy(position -> assertThat(position).isPositive());

        // Zero to two. Both replacements scan the stale rows at the same moment.
        var fourth = cluster.startNode("scl-a2", cluster.defaults().withTokenClaimIntervalMs(500));
        var fifth = cluster.startNode("scl-b2", cluster.defaults().withTokenClaimIntervalMs(500));
        await().atMost(PATIENCE).untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContainNull();
            assertThat(Set.copyOf(owners.values()))
                    .containsExactlyInAnyOrder(fourth.nodeId(), fifth.nodeId());
        });
        awaitResident(run, workflowIds, fourth, fifth);
        var positionsAfterZero = cluster.storedSegmentPositions();
        System.out.printf("EVIDENCE p6-returned claimRows=%s storedPositionsBefore=%s storedPositionsAfter=%s "
                                  + "resident=%s%n",
                          cluster.claimedSegmentOwners(), positionsBeforeZero, positionsAfterZero,
                          List.of(fourth.workflows(run), fifth.workflows(run)));
        assertThat(positionsAfterZero)
                .as("a return from zero must resume from the stored tokens, never rewind behind them")
                .allSatisfy((segment, position) ->
                                    assertThat(position).isGreaterThanOrEqualTo(positionsBeforeZero.get(segment)));

        // And the instances still finish, on the generation that never started them.
        fifth.resumeWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.recorded(run, "resume")).containsExactlyInAnyOrderElementsOf(workflowIds));

        var counts = cluster.stepExecutionCounts(run);
        System.out.printf("EVIDENCE p6-exactly-once rows=%d distinctSteps=%d repeated=%s writers=%s%n",
                          cluster.stepLog(run).size(), counts.size(),
                          counts.entrySet().stream().filter(entry -> entry.getValue() > 1).toList(),
                          cluster.stepLog(run).stream()
                                 .collect(Collectors.groupingBy(row -> row.step() + "@" + row.nodeId(),
                                                                Collectors.counting())));
        assertThat(counts)
                .as("no instance may repeat a step across four membership transitions")
                .hasSize(2 * WORKFLOWS)
                .allSatisfy((step, count) -> assertThat(count).isEqualTo(1L));
        assertThat(cluster.stepLog(run).stream()
                          .filter(row -> "resume".equals(row.step()))
                          .map(ShardCluster.StepLogEntry::nodeId)
                          .collect(Collectors.toSet()))
                .as("no work after the return from zero may be attributed to a process that was killed")
                .isSubsetOf(Set.of(fourth.nodeId(), fifth.nodeId()));
        for (var workflowId : workflowIds) {
            assertThat(cluster.stepLog(run).stream()
                              .filter(row -> row.workflowId().equals(workflowId))
                              .map(ShardCluster.StepLogEntry::step).toList())
                    .as("instance %s must have run its steps in order", workflowId)
                    .containsExactly("start", "resume");
        }
    }

    /**
     * Waits until the given nodes together hold exactly this run's instances, and no instance is on two of them.
     * <p>
     * The union has to be checked with duplicates intact: an instance resident on two processes at once is the failure
     * a scale transition is most likely to produce, and a set union would silently absorb it.
     */
    private static void awaitResident(String run, List<String> workflowIds, RigNode... nodes) {
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(Stream.of(nodes).flatMap(node -> node.workflows(run).stream()).toList())
                        .containsExactlyInAnyOrderElementsOf(workflowIds));
    }
}
