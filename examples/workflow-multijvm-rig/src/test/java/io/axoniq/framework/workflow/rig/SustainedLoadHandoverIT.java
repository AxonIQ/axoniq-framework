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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A cluster changing size while events keep arriving.
 * <p>
 * Every other membership change in this rig happens on a quiet cluster: the instances are already started, the step
 * log has stopped growing, and the segments move with nothing in flight. That is the easy half of the operation. The
 * races live in the other half - a start event committed in the window between one node giving a segment up and
 * another taking it, or committed to a segment that is claimed twice for an instant, is exactly the event that goes
 * missing or is handled twice, and neither outcome raises anything.
 * <p>
 * Both directions are covered, because they fail differently. A join hands segments to a process that has never seen
 * them; a leave hands them to one that has to absorb them on top of its own. In each case a loader keeps publishing
 * start events throughout, and the number published between the first and the last moment of the transition is
 * printed and asserted: a run whose membership change happened to land in a gap in the traffic proves nothing.
 * <p>
 * The oracle is the step log, filtered to the run's own ids, and the property is exactly-once: every instance the
 * loader published records its {@code start} and its {@code resume}, each exactly once, in that order.
 */
@Tag(RigSplit.A)
class SustainedLoadHandoverIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 4;
    private static final int CAP_PER_NODE = SEGMENTS / 2;

    /**
     * Instances the loader publishes before it stops on its own. Enough that the traffic outlives a node boot, which
     * is the slowest part of a membership change and the part the load has to cover end to end.
     */
    private static final int MAX_INSTANCES = 60;

    /**
     * Ids per publish and the pause between publishes: a steady trickle rather than a burst, so the transition is
     * guaranteed to have events landing inside it rather than around it.
     */
    private static final int BATCH = 3;
    private static final Duration PUBLISH_INTERVAL = Duration.ofMillis(300);

    /**
     * Ids per resume request. One request for the whole run would append sixty events inside a single call and run
     * into the rig's own HTTP timeout.
     */
    private static final int RESUME_CHUNK = 20;

    /**
     * How many instances must already be in flight before the membership change starts, so the cluster is genuinely
     * loaded rather than merely being written to.
     */
    private static final int LOADED_AT = 6;

    /**
     * Instances that must be published strictly between the first and the last moment of the membership change. One
     * straggler would technically overlap; this is the difference between traffic meeting the handover and traffic
     * happening to touch its edge.
     */
    private static final int MIN_DURING_TRANSITION = 3;

    private static final Duration PATIENCE = Duration.ofSeconds(240);

    /**
     * A cold node joins and takes half the segments while start events keep arriving.
     */
    @Test
    void noInstanceIsLostWhenANodeJoinsUnderLoad() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var first = cluster.startNode("p4j-a", cluster.defaults()
                                                      .withMaxClaimedSegments(SEGMENTS)
                                                      .withTokenClaimIntervalMs(500));
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.claimedSegmentOwners().values())
                        .hasSize(SEGMENTS).containsOnly(first.nodeId()));

        var run = "p4j-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        try (var load = new Load(first, run)) {
            awaitLoaded(load);
            var claimsBeforeJoin = cluster.claimedSegmentOwners();
            var publishedBeforeJoin = load.published().size();

            var second = cluster.startNode("p4j-b", cluster.defaults().withTokenClaimIntervalMs(500));
            first.capacity(CAP_PER_NODE);
            await().atMost(PATIENCE).untilAsserted(() -> {
                var owners = cluster.claimedSegmentOwners();
                assertThat(owners).hasSize(SEGMENTS);
                assertThat(owners.values()).doesNotContainNull();
                assertThat(Set.copyOf(owners.values()))
                        .containsExactlyInAnyOrder(first.nodeId(), second.nodeId());
            });
            var publishedDuringJoin = load.published().size() - publishedBeforeJoin;
            load.close();

            System.out.printf("EVIDENCE p4-join-under-load publishedBeforeJoin=%d publishedDuringJoin=%d "
                                      + "claimRowsBefore=%s claimRowsAfter=%s%n",
                              publishedBeforeJoin, publishedDuringJoin,
                              claimsBeforeJoin, cluster.claimedSegmentOwners());
            assertThat(publishedDuringJoin)
                    .as("start events must have been committed while the join was happening, or the load never met "
                                + "the handover and this run proves nothing")
                    .isGreaterThanOrEqualTo(MIN_DURING_TRANSITION);

            everyInstanceRunsExactlyOnce(cluster, run, load.published(), second, "p4-join");
        }
    }

    /**
     * A node leaves and the survivor absorbs its segments while start events keep arriving.
     */
    @Test
    void noInstanceIsLostWhenANodeLeavesUnderLoad() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var survivor = cluster.startNode("p4l-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var leaving = cluster.startNode("p4l-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, survivor, leaving);

        var run = "p4l-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        // Published through the survivor: the leaving node must not be the one the load depends on, or the scenario
        // would be measuring the publisher going away rather than the segments moving.
        try (var load = new Load(survivor, run)) {
            awaitLoaded(load);
            var claimsBeforeLeave = cluster.claimedSegmentOwners();
            var heldByLeaving = cluster.segmentsOwnedBy(leaving.nodeId());
            var publishedBeforeLeave = load.published().size();
            assertThat(heldByLeaving).as("the node that leaves must own segments").isNotEmpty();

            survivor.capacity(SEGMENTS);
            leaving.stopGracefully();
            await().atMost(PATIENCE).untilAsserted(() -> {
                var owners = cluster.claimedSegmentOwners();
                assertThat(owners).hasSize(SEGMENTS);
                assertThat(owners.values()).containsOnly(survivor.nodeId());
            });
            var publishedDuringLeave = load.published().size() - publishedBeforeLeave;
            load.close();

            System.out.printf("EVIDENCE p4-leave-under-load leaving=%s alive=%s heldBefore=%s "
                                      + "publishedBeforeLeave=%d publishedDuringLeave=%d claimRowsBefore=%s "
                                      + "claimRowsAfter=%s%n",
                              leaving.nodeId(), leaving.alive(), heldByLeaving,
                              publishedBeforeLeave, publishedDuringLeave,
                              claimsBeforeLeave, cluster.claimedSegmentOwners());
            assertThat(leaving.alive()).isFalse();
            assertThat(publishedDuringLeave)
                    .as("start events must have been committed while the node was leaving, or the load never met the "
                                + "handover and this run proves nothing")
                    .isGreaterThanOrEqualTo(MIN_DURING_TRANSITION);

            everyInstanceRunsExactlyOnce(cluster, run, load.published(), survivor, "p4-leave");
        }
    }

    /**
     * Drives every published instance to its second step and holds the whole run to exactly-once.
     * <p>
     * Three separate ways an instance can be mishandled are checked, because they are separate failures: an instance
     * that never started at all is lost, an instance whose {@code start} appears twice was started on two segments at
     * once, and an instance whose steps come out in the wrong order was restored halfway and re-entered.
     */
    private static void everyInstanceRunsExactlyOnce(ShardCluster cluster, String run, List<String> ids,
                                                     RigNode driver, String label) {
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.recorded(run, "start")).containsExactlyInAnyOrderElementsOf(ids));
        for (var from = 0; from < ids.size(); from += RESUME_CHUNK) {
            driver.resumeWorkflows(ids.subList(from, Math.min(from + RESUME_CHUNK, ids.size())));
        }
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.recorded(run, "resume")).containsExactlyInAnyOrderElementsOf(ids));

        var counts = cluster.stepExecutionCounts(run);
        var writers = cluster.stepLog(run).stream()
                             .collect(Collectors.groupingBy(row -> row.step() + "@" + row.nodeId(),
                                                            Collectors.counting()));
        System.out.printf("EVIDENCE %s-exactly-once instances=%d rows=%d distinctSteps=%d repeated=%s writers=%s%n",
                          label, ids.size(), cluster.stepLog(run).size(), counts.size(),
                          counts.entrySet().stream().filter(entry -> entry.getValue() > 1).toList(), writers);

        assertThat(counts)
                .as("every instance must run each of its steps exactly once across the membership change")
                .hasSize(2 * ids.size())
                .allSatisfy((step, count) -> assertThat(count).isEqualTo(1L));
        for (var workflowId : ids) {
            assertThat(cluster.stepLog(run).stream()
                              .filter(row -> row.workflowId().equals(workflowId))
                              .map(ShardCluster.StepLogEntry::step).toList())
                    .as("instance %s must have run its steps in order", workflowId)
                    .containsExactly("start", "resume");
        }
    }

    private static void awaitLoaded(Load load) {
        await().atMost(PATIENCE).untilAsserted(() -> {
            load.rethrowFailure();
            assertThat(load.published().size()).isGreaterThanOrEqualTo(LOADED_AT);
        });
    }

    /**
     * Publishes start events at a steady rate on a thread of its own until stopped or until {@link #MAX_INSTANCES}
     * have been published.
     * <p>
     * Only ids whose publish call returned are counted, so the set the assertions run against is the set the cluster
     * was definitely told about.
     */
    private static final class Load implements AutoCloseable {

        private final RigNode publisher;
        private final String run;
        private final List<String> published = Collections.synchronizedList(new ArrayList<>());
        private final Thread thread;

        private volatile boolean running = true;
        private volatile RuntimeException failure;

        Load(RigNode publisher, String run) {
            this.publisher = publisher;
            this.run = run;
            this.thread = Thread.ofPlatform().daemon().name("rig-load-" + run).start(this::pump);
        }

        List<String> published() {
            return List.copyOf(published);
        }

        void rethrowFailure() {
            if (failure != null) {
                throw failure;
            }
        }

        private void pump() {
            var next = 0;
            while (running && next < MAX_INSTANCES) {
                var batch = new ArrayList<String>();
                for (var i = 0; i < BATCH && next < MAX_INSTANCES; i++, next++) {
                    batch.add(run + next);
                }
                try {
                    publisher.startWorkflows(batch);
                } catch (RuntimeException e) {
                    failure = e;
                    return;
                }
                published.addAll(batch);
                sleep(PUBLISH_INTERVAL);
            }
        }

        @Override
        public void close() {
            running = false;
            try {
                thread.join(TimeUnit.SECONDS.toMillis(60));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            rethrowFailure();
        }

        private static void sleep(Duration duration) {
            try {
                TimeUnit.MILLISECONDS.sleep(duration.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
