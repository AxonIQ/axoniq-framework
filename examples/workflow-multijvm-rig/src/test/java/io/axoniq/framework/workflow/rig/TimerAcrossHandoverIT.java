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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static io.axoniq.framework.workflow.rig.TimerWorkflow.RETRY_BACKOFF;
import static io.axoniq.framework.workflow.rig.TimerWorkflow.RETRY_MAX;
import static io.axoniq.framework.workflow.rig.TimerWorkflow.WAIT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A running countdown when its segment moves to another node.
 * <p>
 * Timers are node-local. A step timeout and a retry backoff are both armed in an in-memory scheduler inside whichever
 * process happens to own the segment, while the only durable record is the step's start in the event store. Moving the
 * segment therefore throws the timer away, and whether the countdown comes back is entirely up to what the node
 * claiming the segment does when it restores the instance.
 * <p>
 * This is the quietest way a sharded workflow can fail. A dropped countdown raises nothing, logs nothing and leaves no
 * failed step; the instance simply sits there, indistinguishable from one that is legitimately still waiting. So each
 * scenario below establishes which of three things happened, from the recorded moments rather than from the outcome:
 * <ul>
 *     <li><b>resumed</b> - the countdown fires at its original deadline, the elapsed time before the move counted;</li>
 *     <li><b>restarted</b> - it fires a full interval after the move, so the wait is longer than asked for;</li>
 *     <li><b>dropped</b> - it never fires at all.</li>
 * </ul>
 * Every moment comes from {@code rig_step_log}, whose node column says which process ran the step, and the handover
 * itself is confirmed in the token store's claim rows before the countdown is due. A run in which the segment did not
 * actually move while the timer was outstanding proves nothing and fails here rather than passing quietly.
 */
@Tag(RigSplit.A)
class TimerAcrossHandoverIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 2;
    private static final int CAP_PER_NODE = 1;

    /**
     * How far into the countdown the segment is taken away. Far enough in that the timer is genuinely outstanding,
     * early enough that the move is finished well before the deadline.
     * <p>
     * It also has to exceed {@code 2 * SLACK}, or a resumed countdown and a restarted one land inside each other's
     * tolerance and no assertion can tell them apart. {@link #assertVerdictsAreDistinguishable(Duration)} checks that
     * on every run rather than trusting the arithmetic here.
     */
    private static final Duration TIMEOUT_HANDOVER_AT = WAIT_TIMEOUT.dividedBy(2);
    private static final Duration RETRY_HANDOVER_AT = RETRY_BACKOFF.multipliedBy(2).dividedBy(3);

    /**
     * Tolerance when deciding whether a countdown was resumed or restarted. It has to absorb the delay between a step
     * being recorded and the row being written, and the time the handover itself takes.
     */
    private static final Duration SLACK = Duration.ofSeconds(8);

    private static final Duration PATIENCE = Duration.ofSeconds(180);

    /**
     * A wait under a timeout whose segment moves while the timeout is counting down. The awaited event is never
     * published, so the timeout is the only thing that can move the instance forward.
     */
    @Test
    void aStepTimeoutStillFiresAfterItsSegmentMoves() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var nodeA = cluster.startNode("tmo-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var nodeB = cluster.startNode("tmo-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, nodeA, nodeB);

        var run = "tmo-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var segment = onlySegmentOf(cluster, nodeA);
        var workflowId = idOnSegment(cluster, segment, run);
        nodeB.armTimeouts(List.of(workflowId));

        var armed = awaitRow(cluster, workflowId, "armed");
        assertThat(armed.nodeId()).as("the countdown must be armed on the node owning the segment")
                                  .isEqualTo(nodeA.nodeId());
        System.out.printf("EVIDENCE timer-armed %s segment=%d armedOn=%s timeoutSeconds=%d%n",
                          workflowId, segment, armed.nodeId(), WAIT_TIMEOUT.toSeconds());

        sleepUntil(armed.loggedAt() + TIMEOUT_HANDOVER_AT.toMillis());
        var handoverAt = handOver(cluster, segment, nodeA, nodeB);
        var handoverOffset = handoverAt - armed.loggedAt();
        assertThat(handoverOffset)
                .as("the segment has to move while the timeout is still counting, or the scenario proves nothing")
                .isLessThan(WAIT_TIMEOUT.toMillis());
        assertVerdictsAreDistinguishable(Duration.ofMillis(handoverOffset));

        var settled = awaitOptionalRow(cluster, workflowId, "timedout", WAIT_TIMEOUT.multipliedBy(3));
        assertThat(settled)
                .as("a step timeout whose segment moved must still fire; an instance that never times out and never "
                            + "fails is the silent failure this scenario exists for")
                .isPresent();
        var elapsed = settled.orElseThrow().loggedAt() - armed.loggedAt();
        System.out.printf("EVIDENCE timer-fired %s firedOn=%s msFromArmed=%d msFromHandover=%d timeoutMs=%d "
                                  + "verdict=%s%n",
                          workflowId, settled.orElseThrow().nodeId(), elapsed,
                          settled.orElseThrow().loggedAt() - handoverAt, WAIT_TIMEOUT.toMillis(),
                          verdict(elapsed, WAIT_TIMEOUT, handoverOffset));

        assertThat(settled.orElseThrow().nodeId())
                .as("the countdown must have been picked up by the node that took the segment")
                .isEqualTo(nodeB.nodeId());
        assertThat(elapsed)
                .as("the timeout must not fire before the deadline it was given")
                .isGreaterThanOrEqualTo(WAIT_TIMEOUT.minus(SLACK).toMillis());
        assertThat(verdict(elapsed, WAIT_TIMEOUT, handoverOffset))
                .as("""
                    The countdown fired %s ms after it was armed, against a %s ms timeout whose segment moved at \
                    %s ms. RESUMED means the elapsed time before the move counted and the deadline was kept. \
                    RESTARTED (~%s ms) means the node taking the segment armed a fresh full interval, so the wait \
                    was longer than the workflow asked for - that is a different guarantee, and it is the one this \
                    assertion exists to reject.""",
                    elapsed, WAIT_TIMEOUT.toMillis(), handoverOffset, handoverOffset + WAIT_TIMEOUT.toMillis())
                .isEqualTo("resumed");
        assertThat(rowsOf(cluster, workflowId).stream().map(ShardCluster.StepLogEntry::step).toList())
                .as("the move must not make the instance record anything twice")
                .containsExactly("armed", "timedout");
    }

    /**
     * A step in retry backoff whose segment moves before the next attempt is due. The step always fails, so the only
     * thing that can make the workflow settle is the backoff running out and the next attempt being made.
     */
    @Test
    void aRetryBackoffStillFiresAfterItsSegmentMoves() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var nodeA = cluster.startNode("rty-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var nodeB = cluster.startNode("rty-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, nodeA, nodeB);

        var run = "rty-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var segment = onlySegmentOf(cluster, nodeA);
        var workflowId = idOnSegment(cluster, segment, run);
        nodeB.armRetries(List.of(workflowId));

        var armed = awaitRow(cluster, workflowId, "armed");
        assertThat(armed.nodeId()).isEqualTo(nodeA.nodeId());
        var firstAttempt = awaitRow(cluster, workflowId, "attempt");
        assertThat(firstAttempt.nodeId())
                .as("the first attempt must run on the node owning the segment")
                .isEqualTo(nodeA.nodeId());
        System.out.printf("EVIDENCE retry-armed %s segment=%d firstAttemptOn=%s backoffSeconds=%d retries=%d%n",
                          workflowId, segment, firstAttempt.nodeId(), RETRY_BACKOFF.toSeconds(), RETRY_MAX);

        sleepUntil(firstAttempt.loggedAt() + RETRY_HANDOVER_AT.toMillis());
        var handoverAt = handOver(cluster, segment, nodeA, nodeB);
        var handoverOffset = handoverAt - firstAttempt.loggedAt();
        assertThat(handoverOffset)
                .as("the segment has to move while the backoff is still counting, or the scenario proves nothing")
                .isLessThan(RETRY_BACKOFF.toMillis());
        assertVerdictsAreDistinguishable(Duration.ofMillis(handoverOffset));

        var settled = awaitOptionalRow(cluster, workflowId, "exhausted", RETRY_BACKOFF.multipliedBy(RETRY_MAX + 4L));
        var attempts = rowsOf(cluster, workflowId).stream().filter(row -> "attempt".equals(row.step())).toList();
        System.out.printf("EVIDENCE retry-attempts %s attempts=%s committedStepHistory=%s%n",
                          workflowId,
                          attempts.stream().map(row -> (row.loggedAt() - firstAttempt.loggedAt()) + "ms@"
                                  + row.nodeId()).toList(),
                          nodeB.stepHistory(workflowId, "flakyStep"));
        assertThat(settled)
                .as("a step in retry backoff whose segment moved must still make its next attempt; an instance that "
                            + "neither retries nor fails is the silent failure this scenario exists for")
                .isPresent();

        var laterAttempts = attempts.subList(1, attempts.size());
        assertThat(laterAttempts).as("the retry after the move must have happened").isNotEmpty();
        var gap = laterAttempts.getFirst().loggedAt() - firstAttempt.loggedAt();
        System.out.printf("EVIDENCE retry-resumed %s msFromFirstAttempt=%d msFromHandover=%d backoffMs=%d "
                                  + "settledOn=%s verdict=%s%n",
                          workflowId, gap, laterAttempts.getFirst().loggedAt() - handoverAt,
                          RETRY_BACKOFF.toMillis(), settled.orElseThrow().nodeId(),
                          verdict(gap, RETRY_BACKOFF, handoverOffset));

        assertThat(laterAttempts).allSatisfy(row ->
                assertThat(row.nodeId())
                        .as("every attempt after the move must run on the node that took the segment")
                        .isEqualTo(nodeB.nodeId()));
        assertThat(attempts).as("the step must be attempted once, then retried the configured number of times")
                            .hasSize(RETRY_MAX + 1);
        assertThat(gap).as("a retry must not be attempted before its backoff has elapsed")
                       .isGreaterThanOrEqualTo(RETRY_BACKOFF.minus(SLACK).toMillis());
        assertThat(verdict(gap, RETRY_BACKOFF, handoverOffset))
                .as("""
                    The retry was attempted %s ms after the first attempt, against a %s ms backoff whose segment \
                    moved at %s ms. RESUMED means the backoff already served before the move counted. RESTARTED \
                    (~%s ms) means the node taking the segment began the backoff again, so the step was retried \
                    later than the policy asked for - which the attempt count alone cannot tell apart.""",
                    gap, RETRY_BACKOFF.toMillis(), handoverOffset, handoverOffset + RETRY_BACKOFF.toMillis())
                .isEqualTo("resumed");
        assertThat(settled.orElseThrow().nodeId()).isEqualTo(nodeB.nodeId());
    }

    /**
     * Refuses a run whose handover landed so early that "resumed" and "restarted" overlap.
     * <p>
     * {@link #verdict} accepts an elapsed time within {@link #SLACK} of the interval as resumed and within
     * {@code SLACK} of {@code handover + interval} as restarted. Those two windows are disjoint only while the
     * handover offset exceeds {@code 2 * SLACK}; below that a restarted countdown reads as resumed and the assertion
     * on the verdict proves nothing. Checked from the measured offset rather than the configured one, because the
     * handover takes as long as claiming the segment takes.
     */
    private static void assertVerdictsAreDistinguishable(Duration handoverOffset) {
        assertThat(handoverOffset)
                .as("the handover has to land more than 2 x SLACK (%s ms) into the countdown, or a restarted "
                            + "countdown falls inside the resumed tolerance and the verdict cannot discriminate",
                    2 * SLACK.toMillis())
                .isGreaterThan(SLACK.multipliedBy(2));
    }

    /**
     * Takes the segment away from its owner and waits until the token store's claim rows agree it has gone.
     *
     * @return the moment the claim rows showed the new owner.
     */
    private static long handOver(ShardCluster cluster, int segment, RigNode from, RigNode to) {
        to.capacity(SEGMENTS);
        from.stopProcessor();
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.claimedSegmentOwners().get(segment)).isEqualTo(to.nodeId()));
        var handoverAt = System.currentTimeMillis();
        System.out.printf("EVIDENCE timer-handover segment=%d from=%s to=%s claimRows=%s%n",
                          segment, from.nodeId(), to.nodeId(), cluster.claimedSegmentOwners());
        return handoverAt;
    }

    /**
     * Names what the countdown did, from the recorded moments alone.
     */
    private static String verdict(long elapsedMillis, Duration interval, long handoverOffsetMillis) {
        if (within(elapsedMillis, interval.toMillis())) {
            return "resumed";
        }
        if (within(elapsedMillis, handoverOffsetMillis + interval.toMillis())) {
            return "restarted";
        }
        return "neither-resumed-nor-restarted";
    }

    private static boolean within(long actual, long expected) {
        return Math.abs(actual - expected) <= SLACK.toMillis();
    }

    private static int onlySegmentOf(ShardCluster cluster, RigNode node) {
        var segments = cluster.segmentsOwnedBy(node.nodeId());
        assertThat(segments).as("each node must hold exactly one segment for the id to be aimable").hasSize(1);
        return segments.iterator().next();
    }

    private static String idOnSegment(ShardCluster cluster, int segment, String run) {
        return ShardCluster.workflowIdsOnSegment(segment, cluster.segmentMasks().get(segment), run, 1).getFirst();
    }

    private static ShardCluster.StepLogEntry awaitRow(ShardCluster cluster, String workflowId, String step) {
        return awaitOptionalRow(cluster, workflowId, step, PATIENCE)
                .orElseThrow(() -> new AssertionError("No '" + step + "' row for " + workflowId + " within "
                                                              + PATIENCE + "; the log holds "
                                                              + rowsOf(cluster, workflowId)));
    }

    private static Optional<ShardCluster.StepLogEntry> awaitOptionalRow(ShardCluster cluster, String workflowId, String step,
                                                             Duration patience) {
        var deadline = System.nanoTime() + patience.toNanos();
        while (System.nanoTime() < deadline) {
            var row = rowsOf(cluster, workflowId).stream().filter(entry -> step.equals(entry.step())).findFirst();
            if (row.isPresent()) {
                return row;
            }
            sleepMillis(500);
        }
        return Optional.empty();
    }

    private static List<ShardCluster.StepLogEntry> rowsOf(ShardCluster cluster, String workflowId) {
        return cluster.stepLog().stream().filter(row -> row.workflowId().equals(workflowId)).toList();
    }

    private static void sleepUntil(long epochMillis) {
        sleepMillis(epochMillis - System.currentTimeMillis());
    }

    private static void sleepMillis(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
