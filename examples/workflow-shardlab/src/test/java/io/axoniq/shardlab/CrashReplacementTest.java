package io.axoniq.shardlab;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Sequential failover: SIGKILL the only node, then bring up a replacement over the same durable token store and
 * check the in-flight instances come back. This is the shape a cluster degrades to, because a second node cannot
 * be started next to a live one (see {@link MultiNodeStartupTest}).
 */
class CrashReplacementTest {

    private static final Duration READY = Duration.ofSeconds(90);
    private static final int SEGMENTS = 4;

    @Test
    void replacementNodeResumesInstancesAwaitingAnEvent() {
        try (var cluster = new Cluster("replace-awaiting"); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", SEGMENTS);
            a.awaitReady(READY);

            var ids = IntStream.range(0, 12).mapToObj(i -> "await-" + i).toList();
            ids.forEach(id -> publisher.publish(new Events.OrderPlaced(id)));
            awaitStep(cluster, "reserve", ids);
            var ownersBefore = cluster.tokens();
            System.out.println("tokens before kill: " + ownersBefore);

            a.kill();
            a.awaitExit();
            assertThat(a.alive()).as("evidence: node A really died").isFalse();
            assertThat(cluster.tokens())
                    .as("evidence: SIGKILL left the claims behind, they were not released")
                    .allMatch(row -> !"null".equals(row.get("owner")));

            Cluster.sleep(15_000); // let the 10s claim timeout lapse
            var b = cluster.startNode("B", SEGMENTS);
            b.awaitReady(READY);
            await().atMost(Duration.ofSeconds(90)).untilAsserted(() ->
                    assertThat(cluster.tokens().stream().map(r -> r.get("owner")).distinct().toList())
                            .as("evidence: segments actually moved to the replacement")
                            .hasSize(1));
            System.out.println("tokens after replacement: " + cluster.tokens());
            assertThat(cluster.tokens()).isNotEqualTo(ownersBefore);

            ids.forEach(id -> publisher.publish(new Events.PaymentReceived(id)));
            await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                    assertThat(shipped(cluster)).as("all orders must ship; steps=" + cluster.steps())
                                                .containsAll(ids));
            assertThat(nodesThatRan(cluster, "ship")).containsExactly("B");
        }
    }

    @Test
    void eventsPublishedWhileTheClusterIsDownStillWakeTheInstances() {
        try (var cluster = new Cluster("replace-downtime"); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", SEGMENTS);
            a.awaitReady(READY);

            var ids = IntStream.range(0, 12).mapToObj(i -> "down-" + i).toList();
            ids.forEach(id -> publisher.publish(new Events.OrderPlaced(id)));
            awaitStep(cluster, "reserve", ids);

            a.kill();
            a.awaitExit();
            assertThat(a.alive()).isFalse();

            // the whole cluster is down while the resume events are published
            ids.forEach(id -> publisher.publish(new Events.PaymentReceived(id)));
            assertThat(cluster.steps().stream().noneMatch(r -> r.get("step").equals("ship")))
                    .as("evidence: nothing shipped while the cluster was down").isTrue();

            Cluster.sleep(15_000);
            var b = cluster.startNode("B", SEGMENTS);
            b.awaitReady(READY);

            await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                    assertThat(shipped(cluster)).as("all orders must ship; steps=" + cluster.steps())
                                                .containsAll(ids));
        }
    }

    @Test
    void replacementNodeResumesAStepThatWasRunningWhenTheNodeDied() {
        try (var cluster = new Cluster("replace-midstep"); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", SEGMENTS);
            a.awaitReady(READY);

            var ids = IntStream.range(0, 6).mapToObj(i -> "mid-" + i).toList();
            ids.forEach(id -> publisher.publish(new Events.SlowJobRequested(id, 60)));
            awaitStep(cluster, "slowStep-begin", ids);
            assertThat(cluster.steps().stream().noneMatch(r -> r.get("step").equals("slowStep-end")))
                    .as("evidence: the slow steps were still running at kill time").isTrue();

            a.kill();
            a.awaitExit();
            assertThat(a.alive()).isFalse();

            Cluster.sleep(15_000);
            var b = cluster.startNode("B", SEGMENTS);
            b.awaitReady(READY);

            await().atMost(Duration.ofSeconds(240)).untilAsserted(() -> {
                var finished = cluster.steps().stream()
                                      .filter(r -> r.get("step").equals("finish"))
                                      .map(r -> r.get("wf_id"))
                                      .collect(Collectors.toSet());
                assertThat(finished).as("all slow jobs must finish; steps=" + cluster.steps()).containsAll(ids);
            });
        }
    }

    @Test
    void replacementNodeResumesAStepThatWasInRetryBackoff() {
        try (var cluster = new Cluster("replace-retry"); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", SEGMENTS);
            a.awaitReady(READY);

            var ids = IntStream.range(0, 6).mapToObj(i -> "retry-" + i).toList();
            ids.forEach(id -> publisher.publish(new Events.FlakyJobRequested(id, 4)));
            await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
                var attempted = cluster.steps().stream()
                                       .filter(r -> r.get("step").startsWith("flakyStep-attempt-"))
                                       .map(r -> r.get("wf_id"))
                                       .collect(Collectors.toSet());
                assertThat(attempted).containsAll(ids);
            });
            assertThat(cluster.steps().stream().noneMatch(r -> r.get("step").equals("flakyDone")))
                    .as("evidence: still retrying at kill time").isTrue();

            a.kill();
            a.awaitExit();
            assertThat(a.alive()).isFalse();

            Cluster.sleep(15_000);
            var b = cluster.startNode("B", SEGMENTS);
            b.awaitReady(READY);

            await().atMost(Duration.ofSeconds(240)).untilAsserted(() -> {
                var done = cluster.steps().stream()
                                  .filter(r -> r.get("step").equals("flakyDone"))
                                  .map(r -> r.get("wf_id"))
                                  .collect(Collectors.toSet());
                assertThat(done).as("all flaky jobs must finish; steps=" + cluster.steps()).containsAll(ids);
            });
        }
    }

    static java.util.Set<String> nodesThatRan(Cluster cluster, String step) {
        return cluster.steps().stream()
                      .filter(r -> r.get("step").equals(step))
                      .map(r -> r.get("node_id"))
                      .collect(Collectors.toCollection(java.util.TreeSet::new));
    }

    private static java.util.Set<String> shipped(Cluster cluster) {
        return cluster.steps().stream()
                      .filter(r -> r.get("step").equals("ship"))
                      .map(r -> r.get("wf_id"))
                      .collect(Collectors.toSet());
    }

    private static void awaitStep(Cluster cluster, String step, List<String> ids) {
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            var seen = cluster.steps().stream()
                              .filter(r -> r.get("step").equals(step))
                              .map(r -> r.get("wf_id"))
                              .collect(Collectors.toSet());
            assertThat(seen).containsAll(ids);
        });
    }
}
