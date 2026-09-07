package io.axoniq.shardlab;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A backlog of resume events accumulates while the cluster is down and is then caught up by the node that takes
 * the segments over. Run at one segment and at four, so the segment count is the only difference.
 */
class CatchUpResumeTest {

    private static final Duration READY = Duration.ofSeconds(90);

    @ParameterizedTest(name = "segments={0}")
    @ValueSource(ints = {1, 4})
    void backlogOfResumeEventsWakesEveryInstance(int segments) {
        try (var cluster = new Cluster("catchup-" + segments); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", segments);
            a.awaitReady(READY);
            assertThat(cluster.tokens()).hasSize(segments);

            var ids = IntStream.range(0, 12).mapToObj(i -> "cu" + segments + "-" + i).toList();
            ids.forEach(id -> publisher.publish(new Events.OrderPlaced(id)));
            await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                    assertThat(step(cluster, "reserve")).containsAll(ids));

            a.kill();
            a.awaitExit();
            assertThat(a.alive()).as("evidence: the node really died").isFalse();

            // backlog builds up with nobody running
            ids.forEach(id -> publisher.publish(new Events.PaymentReceived(id)));
            assertThat(step(cluster, "ship")).as("evidence: nothing shipped while down").isEmpty();

            Cluster.sleep(15_000);
            var b = cluster.startNode("B", segments);
            b.awaitReady(READY);

            try {
                await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                        assertThat(step(cluster, "ship")).containsAll(ids));
            } finally {
                System.out.println("segments=" + segments
                                           + " shipped=" + step(cluster, "ship").size() + "/" + ids.size()
                                           + " commitPhaseWarnings="
                                           + countOccurrences(b.log(), "ProcessingContext is already in phase COMMIT"));
            }
        }
    }

    /** Same resume events, but delivered to a live node instead of caught up after a crash. */
    @ParameterizedTest(name = "segments={0}")
    @ValueSource(ints = {1, 4})
    void burstOfResumeEventsOnALiveNodeWakesEveryInstance(int segments) {
        try (var cluster = new Cluster("burst-" + segments); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", segments);
            a.awaitReady(READY);

            var ids = IntStream.range(0, 12).mapToObj(i -> "burst" + segments + "-" + i).toList();
            ids.forEach(id -> publisher.publish(new Events.OrderPlaced(id)));
            await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                    assertThat(step(cluster, "reserve")).containsAll(ids));

            ids.forEach(id -> publisher.publish(new Events.PaymentReceived(id)));
            try {
                await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                        assertThat(step(cluster, "ship")).containsAll(ids));
            } finally {
                System.out.println("live segments=" + segments
                                           + " shipped=" + step(cluster, "ship").size() + "/" + ids.size()
                                           + " commitPhaseWarnings="
                                           + countOccurrences(a.log(),
                                                              "ProcessingContext is already in phase COMMIT"));
            }
        }
    }

    private static Set<String> step(Cluster cluster, String step) {
        return cluster.steps().stream()
                      .filter(r -> r.get("step").equals(step))
                      .map(r -> r.get("wf_id"))
                      .collect(Collectors.toSet());
    }

    private static long countOccurrences(String haystack, String needle) {
        return List.of(haystack.split("\n")).stream().filter(l -> l.contains(needle)).count();
    }
}
