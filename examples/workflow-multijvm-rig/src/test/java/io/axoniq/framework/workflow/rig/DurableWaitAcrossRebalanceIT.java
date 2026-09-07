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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A durably recorded wait must survive its segment moving to another node.
 * <p>
 * Every instance here has published {@code WaitForEventStarted} and is waiting on a business event carried by the same
 * stream. The only thing between the instance and a permanently missed wake is the segment's stored token: if that
 * token moves past the event without the wake having been applied, the node claiming the segment next resumes behind
 * it and the event is never delivered again. This is not the window in which a wait was never recorded at all - here
 * the wait is recorded for every instance before any payment is published, and the rig proves that by waiting for the
 * step that runs strictly after the subscription.
 * <p>
 * Three orderings, because they are at risk for different reasons:
 * <ul>
 *     <li>{@link #paymentPublishedBeforeAGracefulReleaseIsNeverLost()} takes the segment away with a processor stop
 *     while payments are being delivered, so the release path decides.</li>
 *     <li>{@link #paymentPublishedBeforeAKillIsNeverLost()} kills the owner mid-delivery, so nothing can drain and the
 *     segment resumes from whatever the periodic checkpoints already stored.</li>
 *     <li>{@link #paymentPublishedAfterTheRebalanceIsDelivered()} publishes after the handover, as a control.</li>
 * </ul>
 * Whether a wait completed is read out of the <em>event store</em>, never out of the side-effect log: a step in flight
 * when its node is killed is never re-run, so its absence from a log says nothing about the wait.
 */
@Tag(RigSplit.B)
class DurableWaitAcrossRebalanceIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 4;
    private static final int CAP_PER_NODE = 2;
    /**
     * Enough instances on the one segment that delivering their payments outlasts the fault. With a handful the owner
     * finishes every wake before the segment leaves it, and the handover then happens with nothing outstanding:
     * green, and vacuous.
     */
    private static final int INSTANCES = 120;
    private static final int ROUNDS = 3;
    /** Where in the delivery the release lands, per round. Zero is the sharpest; the others walk into it. */
    private static final long[] RELEASE_DELAYS_MS = {0, 120, 400};
    private static final String WAIT_STEP = "waitForPayment";
    private static final String COMPLETED = "COMPLETED";
    private static final String PAYMENT_EVENT = DurableWaitWorkflow.PaymentReceived.class.getSimpleName();

    /**
     * The old owner is asked to hand the segment back while payments are being delivered to it.
     */
    @Test
    void paymentPublishedBeforeAGracefulReleaseIsNeverLost() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var nodeA = cluster.startNode("wait-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var nodeB = cluster.startNode("wait-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, nodeA, nodeB);

        var segment = nodeA.segments().iterator().next();
        var roundsAtRisk = 0;
        for (var round = 0; round < ROUNDS; round++) {
            var owner = ownerOf(cluster, segment);
            var other = otherThan(cluster, owner);
            var ids = instancesOn(cluster, segment, "a" + round);

            park(cluster, owner, ids);
            var tokenBefore = storedPosition(cluster, segment);

            // Commit the payments through the peer, so the owning node has to read them off the stream rather than
            // observing its own append, and pull the segment out from under it while it does. The release is a
            // processor stop rather than a capacity change: a capacity change only lands on the coordinator's next
            // pass, by which time the old owner has applied every wake and nothing is at risk.
            other.payOrders(ids);
            sleepMillis(RELEASE_DELAYS_MS[round % RELEASE_DELAYS_MS.length]);
            other.capacity(SEGMENTS);
            var releasing = CompletableFuture.runAsync(owner::stopProcessor);

            awaitHandover(cluster, segment, owner);
            var tokenAtHandover = storedPosition(cluster, segment);
            var completedAtHandover = completedWaits(other, ids).size();
            var atRisk = completedAtHandover < INSTANCES;
            roundsAtRisk += atRisk ? 1 : 0;

            System.out.printf("EVIDENCE graceful round=%d segment=%d releasedBy=%s claimedBy=%s "
                                      + "storedTokenBefore=%d storedTokenAtHandover=%d rawStoredToken=%s "
                                      + "paymentPositions=[%d..%d] waitsCompletedAtHandover=%d/%d atRisk=%s%n",
                              round, segment, owner.nodeId(), ownerOf(cluster, segment).nodeId(),
                              tokenBefore, tokenAtHandover, cluster.storedSegmentTokens().get(segment),
                              other.committedPositionsOf(PAYMENT_EVENT).getFirst(),
                              other.committedPositionsOf(PAYMENT_EVENT).getLast(),
                              completedAtHandover, INSTANCES, atRisk);

            assertThat(ownerOf(cluster, segment).nodeId())
                    .as("the segment must actually have changed hands, or this round proves nothing")
                    .isNotEqualTo(owner.nodeId());

            await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                    assertThat(completedWaits(other, ids))
                            .as("a durably recorded wait must not be lost when its segment is rebalanced")
                            .containsAll(ids));

            releasing.join();
            owner.capacity(CAP_PER_NODE);
            owner.startProcessor();
            System.out.printf("EVIDENCE graceful round=%d waitCompletionWriters=%s%n",
                              round, writerHistogram(other, ids));
        }
        // Not an assertion: a graceful release may legitimately apply every outstanding wake before the token goes.
        System.out.printf("EVIDENCE graceful roundsAtRisk=%d/%d%n", roundsAtRisk, ROUNDS);
    }

    /**
     * The release the engine cannot prepare for: the owner is killed mid-delivery, so the segment resumes from
     * whatever its periodic checkpoints had already stored. Every payment that stored token covers must have been
     * applied, or it is gone.
     */
    @Test
    void paymentPublishedBeforeAKillIsNeverLost() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var nodeA = cluster.startNode("kill-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var nodeB = cluster.startNode("kill-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, nodeA, nodeB);

        var segment = nodeA.segments().iterator().next();
        var owner = ownerOf(cluster, segment);
        var other = otherThan(cluster, owner);
        var ids = instancesOn(cluster, segment, "k0");

        park(cluster, owner, ids);
        var tokenBefore = storedPosition(cluster, segment);

        other.capacity(SEGMENTS);
        other.payOrders(ids);
        owner.kill();
        var tokenAtKill = storedPosition(cluster, segment);
        // Read after the kill: draining the whole stream takes about a second, which would have let the victim apply
        // every wake before it died and made this round vacuous.
        var completedByVictim = completedWaits(other, ids);
        var payments = other.committedPositionsOf(PAYMENT_EVENT);

        System.out.printf("EVIDENCE kill segment=%d killed=%s alive=%s claimRowsStillOwnedByVictim=%s "
                                  + "storedTokenBefore=%d storedTokenAtKill=%d rawStoredToken=%s "
                                  + "paymentPositions=[%d..%d] waitsCompletedByVictim=%d/%d%n",
                          segment, owner.nodeId(), owner.alive(),
                          cluster.claimedSegmentOwners().entrySet().stream()
                                 .filter(entry -> owner.nodeId().equals(entry.getValue()))
                                 .map(Map.Entry::getKey).toList(),
                          tokenBefore, tokenAtKill, cluster.storedSegmentTokens().get(segment),
                          payments.getFirst(), payments.getLast(), completedByVictim.size(), INSTANCES);

        assertThat(owner.alive()).isFalse();
        assertThat(completedByVictim.size())
                .as("the victim applied every wake before it died, so no wake had to survive the kill and a green "
                            + "result here would be vacuous")
                .isLessThan(INSTANCES);

        awaitHandover(cluster, segment, owner);
        await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                assertThat(completedWaits(other, ids))
                        .as("a payment the dead node's stored token already covers must still reach its instance")
                        .containsAll(ids));
        // Attribution of the wakes that had to survive: they were absent from the store while the victim was still
        // alive and present after it died, and the victim's process never came back, so the survivor wrote them.
        var completedAfterKill = completedWaits(other, ids);
        completedAfterKill.removeAll(completedByVictim);
        assertThat(owner.alive()).isFalse();
        System.out.printf("EVIDENCE kill claimedBy=%s waitCompletionWriters=%s completedOnlyAfterTheKill=%d "
                                  + "victimStillDead=%s%n",
                          ownerOf(cluster, segment).nodeId(), writerHistogram(other, ids),
                          completedAfterKill.size(), !owner.alive());
        assertThat(completedAfterKill)
                .as("the wakes the victim never applied must have been applied by the survivor")
                .hasSize(INSTANCES - completedByVictim.size());
    }

    /**
     * The benign ordering, as a control: the payment is committed only after the segment has moved, so it is ahead of
     * the stored token and the new owner has to deliver it.
     */
    @Test
    void paymentPublishedAfterTheRebalanceIsDelivered() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var nodeA = cluster.startNode("late-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var nodeB = cluster.startNode("late-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, nodeA, nodeB);

        var segment = nodeA.segments().iterator().next();
        var owner = ownerOf(cluster, segment);
        var other = otherThan(cluster, owner);
        var ids = instancesOn(cluster, segment, "b0");

        park(cluster, owner, ids);
        var tokenBefore = storedPosition(cluster, segment);

        other.capacity(SEGMENTS);
        owner.stopProcessor();
        awaitHandover(cluster, segment, owner);
        var tokenAtHandover = storedPosition(cluster, segment);

        other.payOrders(ids);
        System.out.printf("EVIDENCE control segment=%d releasedBy=%s claimedBy=%s storedTokenBefore=%d "
                                  + "storedTokenAtHandover=%d rawStoredToken=%s%n",
                          segment, owner.nodeId(), ownerOf(cluster, segment).nodeId(),
                          tokenBefore, tokenAtHandover, cluster.storedSegmentTokens().get(segment));

        await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                assertThat(completedWaits(other, ids)).containsAll(ids));
        System.out.printf("EVIDENCE control waitCompletionWriters=%s%n", writerHistogram(other, ids));
    }

    /**
     * Starts the instances and waits until each has requested its payment. That step runs strictly after the wait has
     * been subscribed and its {@code WaitForEventStarted} durably appended, so its side effect is the rig's proof that
     * every instance really is waiting before any payment is published.
     */
    private static void park(ShardCluster cluster, RigNode owner, List<String> ids) {
        owner.placeOrders(ids);
        await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                assertThat(loggedIds(cluster, "request")).containsAll(ids));
    }

    private static Set<String> completedWaits(RigNode reader, List<String> ids) {
        var completed = reader.instancesWithStep(WAIT_STEP, COMPLETED);
        completed.retainAll(ids);
        return completed;
    }

    private static Map<String, Long> writerHistogram(RigNode reader, List<String> ids) {
        return reader.writersOfStep(WAIT_STEP, COMPLETED).entrySet().stream()
                     .filter(entry -> ids.contains(entry.getKey()))
                     .map(Map.Entry::getValue)
                     .collect(Collectors.groupingBy(writer -> writer, Collectors.counting()));
    }

    private static List<String> instancesOn(ShardCluster cluster, int segment, String tag) {
        var mask = cluster.segmentMasks().get(segment);
        var prefix = "dw-" + tag + "-" + UUID.randomUUID().toString().substring(0, 6) + "-";
        return ShardCluster.workflowIdsOnSegment(segment, mask, prefix, INSTANCES);
    }

    private static RigNode ownerOf(ShardCluster cluster, int segment) {
        var owner = cluster.claimedSegmentOwners().get(segment);
        return cluster.nodes().stream()
                      .filter(node -> node.nodeId().equals(owner))
                      .findFirst()
                      .orElseThrow(() -> new IllegalStateException("Segment " + segment + " is unclaimed"));
    }

    private static RigNode otherThan(ShardCluster cluster, RigNode node) {
        return cluster.nodes().stream()
                      .filter(candidate -> !candidate.nodeId().equals(node.nodeId()))
                      .findFirst()
                      .orElseThrow();
    }

    private static void awaitHandover(ShardCluster cluster, int segment, RigNode from) {
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            var owner = cluster.claimedSegmentOwners().get(segment);
            assertThat(owner).isNotNull().isNotEqualTo(from.nodeId());
        });
    }

    private static long storedPosition(ShardCluster cluster, int segment) {
        return Objects.requireNonNull(cluster.storedSegmentPositions().get(segment),
                                      "Segment " + segment + " has no stored token position");
    }

    private static Set<String> loggedIds(ShardCluster cluster, String step) {
        return cluster.stepLog().stream()
                      .filter(entry -> step.equals(entry.step()))
                      .map(ShardCluster.StepLogEntry::workflowId)
                      .collect(Collectors.toSet());
    }

    private static void sleepMillis(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
