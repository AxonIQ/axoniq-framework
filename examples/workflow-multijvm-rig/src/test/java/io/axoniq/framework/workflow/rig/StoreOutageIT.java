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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The token store itself failing under a cluster that is holding claims.
 * <p>
 * {@code ProcessFaultsIT} cuts one node off from the database, which is a partition: the store is healthy and its
 * other client carries on. An outage is a different shape. Nobody is isolated, nothing fails over to anyone, and every
 * node in the cluster loses the same thing at the same instant - including the claims they are all in the middle of
 * extending. What comes back afterwards has to be the same cluster, holding the same segments, running the same
 * instances.
 * <p>
 * Two outages, because a store can fail in two unrelated ways:
 * <ul>
 *     <li>{@link #aCrashRestartOfTheStoreLosesNoInstance()} kills the server under everyone. Every pooled connection
 *     in every node dies at once and the server performs crash recovery.</li>
 *     <li>{@link #aDrainedConnectionPoolLosesNoInstance()} empties one node's connection pool from inside that node.
 *     Nothing is unreachable - the store is healthy and its peer is unaffected - yet the node cannot make a single
 *     database call, so its claims go unextended until they are older than the claim timeout.</li>
 * </ul>
 * Both are proved to have landed before anything is concluded from them: the crash by a connection held open across
 * it, which must be dead afterwards, and the drain by the claim rows' own timestamps, which must stop advancing and
 * grow older than the timeout that decides whether a claim is still alive.
 */
@Tag(RigSplit.A)
class StoreOutageIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 4;
    private static final int CAP_PER_NODE = SEGMENTS / 2;
    private static final int WORKFLOWS = 12;
    private static final Duration PATIENCE = Duration.ofSeconds(240);

    /**
     * How often the store is crashed. Once proves the path; three times means a node that only survives the first one
     * cannot pass.
     */
    private static final int CRASHES = 3;
    private static final Duration BETWEEN_CRASHES = Duration.ofSeconds(6);

    /**
     * Claim timeout the drained node runs with, and how long its pool stays empty. The hold is deliberately longer
     * than both the timeout - so the claims provably expire - and Hikari's thirty-second acquisition timeout - so the
     * node's database calls genuinely fail rather than queueing until the pool comes back.
     */
    private static final int CLAIM_TIMEOUT_SECONDS = 10;
    private static final Duration POOL_HOLD = Duration.ofSeconds(40);
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(2);

    @Test
    void aCrashRestartOfTheStoreLosesNoInstance() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var nodeA = cluster.startNode("out-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var nodeB = cluster.startNode("out-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, nodeA, nodeB);

        var run = "out-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(index -> run + index).toList();
        nodeA.startWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.recorded(run, "start")).containsExactlyInAnyOrderElementsOf(workflowIds));
        var claimsBefore = cluster.claimedSegmentOwners();
        var positionsBefore = cluster.storedSegmentPositions();
        System.out.printf("EVIDENCE p7-crash-before claimRows=%s storedPositions=%s instances=%d%n",
                          claimsBefore, positionsBefore, WORKFLOWS);

        for (var crash = 0; crash < CRASHES; crash++) {
            crashTheStore(cluster, crash);
            sleep(BETWEEN_CRASHES);
        }

        // Recovery, read out of the claim rows: the same two processes hold every segment again.
        await().atMost(PATIENCE).ignoreExceptions().untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContainNull();
            assertThat(Set.copyOf(owners.values())).containsExactlyInAnyOrder(nodeA.nodeId(), nodeB.nodeId());
        });
        var positionsAfter = cluster.storedSegmentPositions();
        System.out.printf("EVIDENCE p7-crash-recovered aAlive=%s bAlive=%s claimRows=%s storedPositionsBefore=%s "
                                  + "storedPositionsAfter=%s%n",
                          nodeA.alive(), nodeB.alive(), cluster.claimedSegmentOwners(),
                          positionsBefore, positionsAfter);
        assertThat(nodeA.alive()).as("a store outage must not take a node process with it").isTrue();
        assertThat(nodeB.alive()).as("a store outage must not take a node process with it").isTrue();
        assertThat(positionsAfter)
                .as("a store that came back must not have rewound any segment behind where it was")
                .allSatisfy((segment, position) ->
                                    assertThat(position).isGreaterThanOrEqualTo(positionsBefore.get(segment)));

        assertNoInstanceLostOrDuplicated(cluster, run, workflowIds, nodeB, "p7-crash");
    }

    @Test
    void aDrainedConnectionPoolLosesNoInstance() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var drained = cluster.startNode("pool-a", cluster.defaults()
                                                        .withClaimTimeoutSeconds(CLAIM_TIMEOUT_SECONDS)
                                                        .withTokenClaimIntervalMs(500));
        var peer = cluster.startNode("pool-b", cluster.defaults()
                                                     .withClaimTimeoutSeconds(CLAIM_TIMEOUT_SECONDS)
                                                     .withTokenClaimIntervalMs(500));
        awaitSplit(cluster, drained, peer);

        var run = "pool-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(index -> run + index).toList();
        drained.startWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.recorded(run, "start")).containsExactlyInAnyOrderElementsOf(workflowIds));

        var starvedSegments = cluster.segmentsOwnedBy(drained.nodeId());
        assertThat(starvedSegments).as("the node whose pool is drained must own segments").isNotEmpty();
        var claimsBefore = claimTimestampsOf(cluster, starvedSegments);

        // The fault. The node keeps its claims and its process, and loses only its ability to reach the store.
        var drainStartedAt = System.currentTimeMillis();
        var report = drained.drainConnectionPool(POOL_HOLD);
        System.out.printf("EVIDENCE p7-pool-drained node=%s held=%d activeConnections=%d idleConnections=%d "
                                  + "holdMillis=%d claimTimeoutSeconds=%d starvedSegments=%s%n",
                          drained.nodeId(), report.get("held").asInt(), report.get("activeConnections").asInt(),
                          report.get("idleConnections").asInt(), report.get("holdMillis").asLong(),
                          CLAIM_TIMEOUT_SECONDS, starvedSegments);
        assertThat(report.get("held").asInt()).as("the drain must have taken the pool").isPositive();
        assertThat(report.get("idleConnections").asInt()).as("the pool must be empty for this to be a drain")
                                                         .isZero();

        // Landing evidence from the claim rows alone: the starved node stops extending, and its claims age past the
        // timeout that decides whether they are still alive. The peer's are read too, so a run in which the whole
        // store stalled - which would prove nothing about one node's pool - fails here instead of passing.
        var maxStarvedAgeMillis = 0L;
        var maxPeerAgeMillis = 0L;
        var untilHealed = System.nanoTime() + POOL_HOLD.toNanos();
        while (System.nanoTime() < untilHealed) {
            sleep(SAMPLE_INTERVAL);
            var now = Instant.now();
            for (var claim : cluster.claimTimestamps().entrySet()) {
                var age = Duration.between(claim.getValue(), now).toMillis();
                if (starvedSegments.contains(claim.getKey())) {
                    maxStarvedAgeMillis = Math.max(maxStarvedAgeMillis, age);
                } else {
                    maxPeerAgeMillis = Math.max(maxPeerAgeMillis, age);
                }
            }
        }
        var claimsDuring = claimTimestampsOf(cluster, starvedSegments);
        System.out.printf("EVIDENCE p7-pool-starved node=%s maxStarvedClaimAgeMs=%d maxPeerClaimAgeMs=%d "
                                  + "claimTimeoutMs=%d claimsBefore=%s claimsAtEndOfHold=%s%n",
                          drained.nodeId(), maxStarvedAgeMillis, maxPeerAgeMillis,
                          TimeUnit.SECONDS.toMillis(CLAIM_TIMEOUT_SECONDS), claimsBefore, claimsDuring);
        assertThat(maxStarvedAgeMillis)
                .as("a node that cannot reach the store cannot extend its claims; a claim that stayed fresh means the"
                            + " drain never touched the path the token store uses and this run proves nothing")
                .isGreaterThan(TimeUnit.SECONDS.toMillis(CLAIM_TIMEOUT_SECONDS));
        assertThat(maxPeerAgeMillis)
                .as("the peer must have kept renewing throughout; if it did not, the store stalled rather than one "
                            + "node's pool")
                .isLessThan(TimeUnit.SECONDS.toMillis(CLAIM_TIMEOUT_SECONDS));

        // Recovery: every segment claimed again, by one of the two live processes.
        await().atMost(PATIENCE).ignoreExceptions().untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContainNull();
            assertThat(Set.copyOf(owners.values())).isSubsetOf(Set.of(drained.nodeId(), peer.nodeId()));
        });
        await().atMost(PATIENCE).ignoreExceptions().untilAsserted(() -> {
            var fresh = cluster.claimTimestamps();
            assertThat(starvedSegments).allSatisfy(segment ->
                    assertThat(fresh.get(segment)).isAfter(claimsDuring.get(segment)));
        });
        System.out.printf("EVIDENCE p7-pool-recovered node=%s alive=%s peerAlive=%s outageMs=%d claimRows=%s%n",
                          drained.nodeId(), drained.alive(), peer.alive(),
                          System.currentTimeMillis() - drainStartedAt, cluster.claimedSegmentOwners());
        assertThat(drained.alive()).as("an exhausted pool must not take the node process with it").isTrue();
        assertThat(peer.alive()).isTrue();

        assertNoInstanceLostOrDuplicated(cluster, run, workflowIds, peer, "p7-pool");
    }

    /**
     * Crashes the shared store and proves the crash landed with a connection held open across it.
     * <p>
     * A connection that answered before the crash and cannot answer after it is direct evidence that every backend was
     * terminated, which the length of the outage - crash recovery over a database this small takes tens of
     * milliseconds - would be far too short to catch by polling.
     */
    private static void crashTheStore(ShardCluster cluster, int round) {
        try (var canary = DriverManager.getConnection(ShardCluster.POSTGRES.getJdbcUrl(),
                                                      ShardCluster.POSTGRES.getUsername(),
                                                      ShardCluster.POSTGRES.getPassword())) {
            var backendPid = scalar(canary, "SELECT pg_backend_pid()");
            var killed = cluster.crashDatabaseServer();
            var canarySurvived = survives(canary);
            System.out.printf("EVIDENCE p7-crash-landed round=%d killedPid=%s canaryBackendPid=%s "
                                      + "canaryStillAnswers=%s%n",
                              round, killed, backendPid, canarySurvived);
            assertThat(canarySurvived)
                    .as("a connection open across the crash must be dead afterwards, or the server never went down")
                    .isFalse();
        } catch (SQLException e) {
            throw new IllegalStateException("Could not hold a connection across the store crash", e);
        }
    }

    /**
     * Drives the run to completion and holds it to exactly-once. An instance missing from the step log was lost by the
     * outage; a step recorded twice was run twice across it.
     */
    private static void assertNoInstanceLostOrDuplicated(ShardCluster cluster, String run, List<String> workflowIds,
                                                         RigNode driver, String label) {
        await().atMost(PATIENCE).ignoreExceptions()
               .untilAsserted(() -> assertThat(driver.status()).isNotNull());
        driver.resumeWorkflows(workflowIds);
        await().atMost(PATIENCE).ignoreExceptions().untilAsserted(() ->
                assertThat(cluster.recorded(run, "resume")).containsExactlyInAnyOrderElementsOf(workflowIds));

        var counts = cluster.stepExecutionCounts(run);
        System.out.printf("EVIDENCE %s-exactly-once instances=%d rows=%d distinctSteps=%d repeated=%s writers=%s%n",
                          label, workflowIds.size(), cluster.stepLog(run).size(), counts.size(),
                          counts.entrySet().stream().filter(entry -> entry.getValue() > 1).toList(),
                          cluster.stepLog(run).stream()
                                 .collect(Collectors.groupingBy(row -> row.step() + "@" + row.nodeId(),
                                                                Collectors.counting())));
        assertThat(counts)
                .as("a store outage must not lose an instance or make one repeat a step")
                .hasSize(2 * workflowIds.size())
                .allSatisfy((step, count) -> assertThat(count).isEqualTo(1L));
        for (var workflowId : workflowIds) {
            assertThat(cluster.stepLog(run).stream()
                              .filter(row -> row.workflowId().equals(workflowId))
                              .map(ShardCluster.StepLogEntry::step).toList())
                    .as("instance %s must have run its steps in order", workflowId)
                    .containsExactly("start", "resume");
        }
    }

    private static Map<Integer, Instant> claimTimestampsOf(ShardCluster cluster, Set<Integer> segments) {
        var timestamps = new TreeMap<Integer, Instant>();
        cluster.claimTimestamps().forEach((segment, claimedAt) -> {
            if (segments.contains(segment)) {
                timestamps.put(segment, claimedAt);
            }
        });
        return timestamps;
    }

    private static String scalar(Connection connection, String sql) throws SQLException {
        try (var statement = connection.prepareStatement(sql); var results = statement.executeQuery()) {
            results.next();
            return results.getString(1);
        }
    }

    private static boolean survives(Connection connection) {
        try {
            scalar(connection, "SELECT 1");
            return true;
        } catch (SQLException e) {
            return false;
        }
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
