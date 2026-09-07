package io.axoniq.shardlab;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * What the segment count actually does: on a restart with a different value, and at a count a user might plausibly
 * pick for a large cluster.
 */
class SegmentCountTest {

    private static final Duration READY = Duration.ofSeconds(90);

    @Test
    void wideSegmentCountOnOneNodeStillRunsEveryWorkflowEndToEnd() {
        try (var cluster = new Cluster("segments-wide"); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", 64);
            a.awaitReady(READY);
            assertThat(cluster.tokens()).as("64 segments must be initialized").hasSize(64);

            var ids = IntStream.range(0, 24).mapToObj(i -> "wide-" + i).toList();
            ids.forEach(id -> publisher.publish(new Events.OrderPlaced(id)));
            await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                    assertThat(cluster.steps().stream()
                                      .filter(r -> r.get("step").equals("reserve"))
                                      .map(r -> r.get("wf_id"))
                                      .collect(Collectors.toSet())).containsAll(ids));

            ids.forEach(id -> publisher.publish(new Events.PaymentReceived(id)));
            await().atMost(Duration.ofSeconds(180)).untilAsserted(() ->
                    assertThat(cluster.steps().stream()
                                      .filter(r -> r.get("step").equals("ship"))
                                      .map(r -> r.get("wf_id"))
                                      .collect(Collectors.toSet())).containsAll(ids));

            TwoNodeSmokeTest.assertNoDuplicateSteps(cluster.steps());
        }
    }

    @Test
    void restartingWithADifferentSegmentCountKeepsTheOriginalLayout() {
        try (var cluster = new Cluster("segments-change")) {
            var a = cluster.startNode("A", 4);
            a.awaitReady(READY);
            assertThat(cluster.tokens()).hasSize(4);
            a.stopGracefully();
            a.awaitExit();

            var a2 = cluster.startNode("A2", 16);
            a2.awaitReady(READY);
            System.out.println("segments after restart with 16 configured: " + cluster.tokens().size());
            assertThat(cluster.tokens())
                    .as("initial-segment-count only applies to an empty store; existing layout wins")
                    .hasSize(4);
        }
    }
}
