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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The shape most deployments actually have and no other scenario in this rig has: a cluster with nothing to do.
 * <p>
 * Every other test here starts workflows within seconds of the nodes coming up and keeps events flowing until it is
 * finished, so the cluster is never left alone. Three things are only exercised when it is:
 * <ul>
 *     <li><b>Claim renewal.</b> A claim survives for a claim timeout, ten seconds by default, and is kept alive by the
 *     owning node extending it. The gap below spans roughly twelve of those windows with nothing on the event stream.
 *     Renewal that turned out to be driven by event traffic would let every claim lapse here.</li>
 *     <li><b>Placement after idleness.</b> The second workflow is deliberately given an id that hashes to the segment
 *     of the node that has done no work, and must run there rather than on the node that is warm.</li>
 *     <li><b>Isolation.</b> Each workflow must run on exactly one node, and leave no trace on the other.</li>
 * </ul>
 * An idle cluster that is quietly broken produces no error at all, so nothing here is inferred from the absence of a
 * failure. Ownership and renewal are read out of the token store's own rows, and placement out of the node column of
 * the step log: the node that recorded the step is the node that ran it.
 * <p>
 * The event store is shared by the whole suite, so a node replaying it restores instances from earlier scenarios.
 * Every assertion below is filtered to this run's id prefix.
 */
@Tag(RigSplit.A)
class IdleClusterSparseTrafficIT extends MultiJvmShardingTestBase {

    /**
     * Two segments and one each: the smallest cluster in which an id can be aimed at a chosen node.
     */
    private static final int SEGMENTS = 2;
    private static final int CAP_PER_NODE = 1;

    /**
     * The claim timeout under test. Left at the engine's default, because the point of the scenario is that the gap is
     * many multiples of the timeout a real deployment runs with.
     */
    private static final int CLAIM_TIMEOUT_SECONDS = 10;

    /**
     * Quiet stretch between the two workflows, and the length of the whole scenario. Shortening this makes the run
     * cheaper and the claim-renewal evidence weaker.
     */
    private static final Duration GAP = Duration.ofSeconds(Integer.getInteger("rig.idle.gap-seconds", 120));

    /**
     * Quiet stretch before the first workflow, so it starts on a cluster that has already been left alone.
     */
    private static final Duration WARM_UP = Duration.ofSeconds(Integer.getInteger("rig.idle.warm-up-seconds", 10));

    /**
     * How often the claim rows are sampled during the gap.
     */
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(2);

    private static final Duration PATIENCE = Duration.ofSeconds(120);

    @Test
    void anIdleClusterKeepsItsClaimsAndStillPlacesWorkByHash() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var nodeA = cluster.startNode("idle-a", cluster.defaults().withClaimTimeoutSeconds(CLAIM_TIMEOUT_SECONDS));
        var nodeB = cluster.startNode("idle-b", cluster.defaults().withClaimTimeoutSeconds(CLAIM_TIMEOUT_SECONDS));
        awaitSplit(cluster, nodeA, nodeB);
        var run = "idle-" + UUID.randomUUID().toString().substring(0, 8) + "-";

        // 1. Both segments claimed, one per node, and this run has nothing running anywhere.
        var initialOwners = cluster.claimedSegmentOwners();
        assertThat(initialOwners).hasSize(SEGMENTS);
        assertThat(initialOwners.values()).doesNotContainNull();
        assertThat(Set.copyOf(initialOwners.values())).containsExactlyInAnyOrder(nodeA.nodeId(), nodeB.nodeId());
        assertThat(residentFrom(nodeA, run)).isEmpty();
        assertThat(residentFrom(nodeB, run)).isEmpty();
        System.out.printf("EVIDENCE idle-start claimRows=%s residentFromThisRun=%s%n",
                          initialOwners, List.of(residentFrom(nodeA, run), residentFrom(nodeB, run)));

        // 2. One id per node, aimed by hash. The precondition is asserted rather than assumed: two ids that happened
        //    to land on the same segment would make the whole scenario vacuous.
        var masks = cluster.segmentMasks();
        var segmentOfA = onlySegmentOf(cluster, nodeA);
        var segmentOfB = onlySegmentOf(cluster, nodeB);
        assertThat(segmentOfA).isNotEqualTo(segmentOfB);
        var onA = ShardCluster.workflowIdsOnSegment(segmentOfA, masks.get(segmentOfA), run + "a-", 1).getFirst();
        var onB = ShardCluster.workflowIdsOnSegment(segmentOfB, masks.get(segmentOfB), run + "b-", 1).getFirst();
        System.out.printf("EVIDENCE idle-placement-plan %s owns segment %d and gets %s, %s owns segment %d and gets"
                                  + " %s%n", nodeA.nodeId(), segmentOfA, onA, nodeB.nodeId(), segmentOfB, onB);

        // 3. First workflow, on a cluster that has been left alone for a while. Published through the peer, so its
        //    placement is decided by the hash and not by which node happened to receive it.
        sleep(WARM_UP);
        runToCompletion(cluster, nodeB, onA, nodeA, nodeB, run);

        // 4. The gap. Nothing is published. The only thing that may move is a claim, and it may only be extended.
        var ownersBeforeGap = cluster.claimedSegmentOwners();
        var claimsBeforeGap = cluster.claimTimestamps();
        var churn = new TreeSet<String>();
        var maxClaimAgeMillis = 0L;
        var gapEnd = System.nanoTime() + GAP.toNanos();
        while (System.nanoTime() < gapEnd) {
            sleep(SAMPLE_INTERVAL);
            var owners = cluster.claimedSegmentOwners();
            if (!owners.equals(ownersBeforeGap)) {
                churn.add(owners.toString());
            }
            var now = Instant.now();
            for (var claim : cluster.claimTimestamps().values()) {
                maxClaimAgeMillis = Math.max(maxClaimAgeMillis, Duration.between(claim, now).toMillis());
            }
        }
        var claimsAfterGap = cluster.claimTimestamps();
        var renewals = claimsAfterGap.entrySet().stream()
                                     .collect(Collectors.toMap(Map.Entry::getKey,
                                                               entry -> Duration.between(
                                                                       claimsBeforeGap.get(entry.getKey()),
                                                                       entry.getValue()).toMillis()));
        System.out.printf("EVIDENCE idle-gap gapSeconds=%d claimTimeoutSeconds=%d maxClaimAgeMs=%d "
                                  + "claimAdvancedByMs=%s ownersBefore=%s ownersAfter=%s observedOtherOwners=%s%n",
                          GAP.toSeconds(), CLAIM_TIMEOUT_SECONDS, maxClaimAgeMillis, renewals,
                          ownersBeforeGap, cluster.claimedSegmentOwners(), churn);

        assertThat(churn).as("no segment may change hands on an idle cluster whose nodes are all healthy").isEmpty();
        assertThat(claimsAfterGap)
                .as("an idle node must keep extending its claims; renewal that needed event traffic would leave every"
                            + " claim here as old as the gap")
                .allSatisfy((segment, claimedAt) -> assertThat(claimedAt).isAfter(claimsBeforeGap.get(segment)));
        assertThat(maxClaimAgeMillis)
                .as("a claim older than the claim timeout is expired and stealable, however healthy its owner looks")
                .isLessThan(TimeUnit.SECONDS.toMillis(CLAIM_TIMEOUT_SECONDS));

        // 5. Second workflow, after the idleness, aimed at the node that has done nothing. Published through the warm
        //    node, so routing by hash and routing to whoever is warm give different answers.
        runToCompletion(cluster, nodeA, onB, nodeB, nodeA, run);

        // 6. Two instances, two steps each, nothing recorded twice.
        var rows = rowsOf(cluster, run);
        System.out.printf("EVIDENCE idle-step-log %s%n",
                          rows.stream().map(row -> row.workflowId() + "/" + row.step() + "@" + row.nodeId()).toList());
        assertThat(rows).hasSize(4);
        assertThat(rows.stream().collect(Collectors.groupingBy(row -> row.workflowId() + "/" + row.step(),
                                                               Collectors.counting())).values())
                .as("no step of either instance may be recorded twice").containsOnly(1L);
    }

    /**
     * Starts one instance, proves it is running on the expected node and nowhere else, and drives it to its final
     * step. Both of its recorded steps must name the owning node: the step log's node column is where the instance
     * actually ran.
     */
    private static void runToCompletion(ShardCluster cluster, RigNode publisher, String workflowId,
                                        RigNode expectedOwner, RigNode expectedIdle, String run) {
        publisher.startWorkflows(List.of(workflowId));
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(stepsOf(cluster, workflowId)).contains("start"));

        var residentOnOwner = residentFrom(expectedOwner, run);
        var residentOnIdle = residentFrom(expectedIdle, run);
        System.out.printf("EVIDENCE idle-landed %s startedOn=%s residentOn-%s=%s residentOn-%s=%s%n",
                          workflowId, nodesThatRecorded(cluster, workflowId),
                          expectedOwner.nodeId(), residentOnOwner, expectedIdle.nodeId(), residentOnIdle);
        assertThat(residentOnOwner).as("the instance must be running on the node owning its segment")
                                   .containsExactly(workflowId);
        assertThat(residentOnIdle).as("the node that does not own the segment must not run the instance")
                                  .isEmpty();

        publisher.resumeWorkflows(List.of(workflowId));
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(stepsOf(cluster, workflowId)).contains("start", "resume"));
        System.out.printf("EVIDENCE idle-completed %s steps=%s nodes=%s%n",
                          workflowId, stepsOf(cluster, workflowId), nodesThatRecorded(cluster, workflowId));
        assertThat(nodesThatRecorded(cluster, workflowId))
                .as("every step of the instance must have run on the node owning its segment")
                .containsExactly(expectedOwner.nodeId());
    }

    private static int onlySegmentOf(ShardCluster cluster, RigNode node) {
        var segments = cluster.segmentsOwnedBy(node.nodeId());
        assertThat(segments).as("each node must hold exactly one segment for the ids to be aimable").hasSize(1);
        return segments.iterator().next();
    }

    private static List<String> residentFrom(RigNode node, String run) {
        return node.workflows().stream().filter(id -> id.startsWith(run)).toList();
    }

    private static List<ShardCluster.StepLogEntry> rowsOf(ShardCluster cluster, String run) {
        return cluster.stepLog().stream().filter(row -> row.workflowId().startsWith(run)).toList();
    }

    private static List<String> stepsOf(ShardCluster cluster, String workflowId) {
        return cluster.stepLog().stream()
                      .filter(row -> row.workflowId().equals(workflowId))
                      .map(ShardCluster.StepLogEntry::step)
                      .toList();
    }

    private static Set<String> nodesThatRecorded(ShardCluster cluster, String workflowId) {
        return cluster.stepLog().stream()
                      .filter(row -> row.workflowId().equals(workflowId))
                      .map(ShardCluster.StepLogEntry::nodeId)
                      .collect(Collectors.toCollection(TreeSet::new));
    }

    private static void sleep(Duration duration) {
        try {
            TimeUnit.MILLISECONDS.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
