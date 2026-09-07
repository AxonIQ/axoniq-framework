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
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A rolling deploy: replace one node, then replace the other, with instances in flight throughout.
 * <p>
 * This is the operation a production cluster performs most often and the one the rig never tried. Every other failover
 * scenario here is a single handover; a rolling deploy is two in sequence, and by the end not one process that started
 * the instances is still alive. An instance therefore has to be handed over, picked up, handed over again and picked up
 * again, and the second move happens on a node that itself only learned about the instance by restoring it.
 * <p>
 * The oracle is the step log, and the property is exactly-once: each instance records its {@code start} and its
 * {@code resume}, each once, in that order. Both moves are made to land: the outgoing node is confirmed dead and every
 * one of its segments is confirmed to have changed hands in the token store's claim rows before the scenario
 * continues, so a green result cannot come from a deploy that never happened.
 */
@Tag(RigSplit.B)
class RollingDeployIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 4;
    private static final int CAP_PER_NODE = SEGMENTS / 2;
    private static final int WORKFLOWS = 12;
    private static final Duration PATIENCE = Duration.ofSeconds(180);

    @Test
    void instancesSurviveTwoHandoversInSequenceAndRunExactlyOnce() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var first = cluster.startNode("roll-a1", cluster.defaults().withTokenClaimIntervalMs(500));
        var second = cluster.startNode("roll-b1", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, first, second);

        // The event store is shared by the whole suite; only this run's ids are this scenario's business.
        var run = "roll-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(index -> run + index).toList();
        second.startWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(recorded(cluster, run, "start")).containsExactlyInAnyOrderElementsOf(workflowIds));
        var startWriters = writerHistogram(cluster, run, "start");
        System.out.printf("EVIDENCE rolling-before instances=%d claimRows=%s startWriters=%s%n",
                          WORKFLOWS, cluster.claimedSegmentOwners(), startWriters);
        assertThat(startWriters.keySet())
                .as("the instances must be spread over both original nodes, or one handover moves nothing")
                .containsExactlyInAnyOrder(first.nodeId(), second.nodeId());

        // The deploy itself. Both original processes are gone by the end of it.
        var replacementOfFirst = replace(cluster, first, second, "roll-a2");
        var replacementOfSecond = replace(cluster, second, replacementOfFirst, "roll-b2");
        assertThat(first.alive()).isFalse();
        assertThat(second.alive()).isFalse();

        // Every instance is running again, on nodes that did not exist when it started.
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(Stream.concat(resident(replacementOfFirst, run).stream(),
                                         resident(replacementOfSecond, run).stream()).toList())
                        .containsExactlyInAnyOrderElementsOf(workflowIds));
        System.out.printf("EVIDENCE rolling-restored %s=%s %s=%s%n",
                          replacementOfFirst.nodeId(), resident(replacementOfFirst, run),
                          replacementOfSecond.nodeId(), resident(replacementOfSecond, run));

        // And every instance still makes progress after moving twice.
        replacementOfSecond.resumeWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(recorded(cluster, run, "resume")).containsExactlyInAnyOrderElementsOf(workflowIds));

        var rows = cluster.stepLog().stream().filter(row -> row.workflowId().startsWith(run)).toList();
        System.out.printf("EVIDENCE rolling-after rows=%d startWriters=%s resumeWriters=%s claimRows=%s%n",
                          rows.size(), startWriters, writerHistogram(cluster, run, "resume"),
                          cluster.claimedSegmentOwners());

        assertThat(writerHistogram(cluster, run, "resume").keySet())
                .as("after the deploy no work may be attributed to a process that no longer exists")
                .containsAnyOf(replacementOfFirst.nodeId(), replacementOfSecond.nodeId())
                .isSubsetOf(Set.of(replacementOfFirst.nodeId(), replacementOfSecond.nodeId()));

        assertThat(rows.stream().collect(Collectors.groupingBy(row -> row.workflowId() + "/" + row.step(),
                                                               Collectors.counting())))
                .as("no workflow step may be recorded twice across two handovers")
                .hasSize(2 * WORKFLOWS)
                .allSatisfy((step, count) -> assertThat(count).isEqualTo(1L));

        // Order per instance, taken from the step log's own insertion order rather than from any clock.
        for (var workflowId : workflowIds) {
            assertThat(rows.stream().filter(row -> row.workflowId().equals(workflowId))
                           .map(ShardCluster.StepLogEntry::step).toList())
                    .as("instance %s must have run its steps in order after moving twice", workflowId)
                    .containsExactly("start", "resume");
        }
    }

    /**
     * Replaces one node the way a rolling deploy does: give the survivor room, stop the outgoing node, wait until the
     * claim rows show it holds nothing at all, then bring a fresh node in and hand half the segments back.
     * <p>
     * Both halves are confirmed in the token store before returning, so a scenario cannot continue past a handover
     * that silently did not happen.
     */
    private RigNode replace(ShardCluster cluster, RigNode outgoing, RigNode survivor, String replacementId) {
        var heldByOutgoing = cluster.segmentsOwnedBy(outgoing.nodeId());
        assertThat(heldByOutgoing).as("the node being replaced must actually own segments").isNotEmpty();

        survivor.capacity(SEGMENTS);
        outgoing.stopGracefully();
        assertThat(outgoing.alive()).isFalse();
        assertThat(outgoing.fate()).isEqualTo(RigNode.Fate.STOPPED_GRACEFULLY);
        await().atMost(PATIENCE).untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).containsOnly(survivor.nodeId());
        });
        System.out.printf("EVIDENCE rolling-drained outgoing=%s alive=%s heldBefore=%s claimRows=%s%n",
                          outgoing.nodeId(), outgoing.alive(), heldByOutgoing, cluster.claimedSegmentOwners());

        var replacement = cluster.startNode(replacementId, cluster.defaults().withTokenClaimIntervalMs(500));
        survivor.capacity(CAP_PER_NODE);
        await().atMost(PATIENCE).untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContainNull();
            assertThat(Set.copyOf(owners.values()))
                    .containsExactlyInAnyOrder(survivor.nodeId(), replacement.nodeId());
        });
        System.out.printf("EVIDENCE rolling-rejoined replacement=%s gained=%s claimRows=%s%n",
                          replacement.nodeId(), cluster.segmentsOwnedBy(replacement.nodeId()),
                          cluster.claimedSegmentOwners());
        return replacement;
    }

    private static List<String> resident(RigNode node, String run) {
        return node.workflows().stream().filter(id -> id.startsWith(run)).toList();
    }

    private static Set<String> recorded(ShardCluster cluster, String run, String step) {
        return cluster.stepLog().stream()
                      .filter(row -> row.workflowId().startsWith(run) && step.equals(row.step()))
                      .map(ShardCluster.StepLogEntry::workflowId)
                      .collect(Collectors.toSet());
    }

    private static Map<String, Long> writerHistogram(ShardCluster cluster, String run, String step) {
        return cluster.stepLog().stream()
                      .filter(row -> row.workflowId().startsWith(run) && step.equals(row.step()))
                      .collect(Collectors.groupingBy(ShardCluster.StepLogEntry::nodeId, Collectors.counting()));
    }
}
