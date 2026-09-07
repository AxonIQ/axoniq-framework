package io.axoniq.shardlab;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * First-run experience: two nodes, durable JDBC token store, real Axon Server, 4 segments.
 */
class TwoNodeSmokeTest {

    @Test
    void twoNodesShareSegmentsAndCompleteWorkflows() {
        try (var cluster = new Cluster("smoke"); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", 4);
            a.awaitReady(Duration.ofSeconds(120));
            var b = cluster.startNode("B", 4);
            b.awaitReady(Duration.ofSeconds(120));

            // owner identity must actually differ, otherwise every claim contest passes vacuously
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                var owners = cluster.tokens().stream().map(r -> r.get("owner")).collect(Collectors.toSet());
                System.out.println("TOKENS: " + cluster.tokens());
                assertThat(owners).hasSizeGreaterThanOrEqualTo(2);
            });

            var ids = List.of("o-1", "o-2", "o-3", "o-4", "o-5", "o-6", "o-7", "o-8");
            ids.forEach(id -> publisher.publish(new Events.OrderPlaced(id)));

            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                var reserved = cluster.steps().stream()
                                      .filter(r -> r.get("step").equals("reserve"))
                                      .map(r -> r.get("wf_id"))
                                      .collect(Collectors.toSet());
                assertThat(reserved).containsAll(ids);
            });

            ids.forEach(id -> publisher.publish(new Events.PaymentReceived(id)));

            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                var shipped = cluster.steps().stream()
                                     .filter(r -> r.get("step").equals("ship"))
                                     .map(r -> r.get("wf_id"))
                                     .collect(Collectors.toSet());
                assertThat(shipped).containsAll(ids);
            });

            var byNode = cluster.steps().stream()
                                .collect(Collectors.groupingBy(r -> r.get("node_id"), Collectors.counting()));
            System.out.println("STEPS BY NODE: " + byNode);
            System.out.println("TOKENS: " + cluster.tokens());
            assertThat(byNode.keySet()).as("work must actually be spread over both nodes").hasSize(2);

            assertNoDuplicateSteps(cluster.steps());
        }
    }

    static void assertNoDuplicateSteps(List<Map<String, String>> steps) {
        var counts = steps.stream().collect(Collectors.groupingBy(
                r -> r.get("wf_id") + "/" + r.get("step"), Collectors.counting()));
        var duplicates = counts.entrySet().stream().filter(e -> e.getValue() > 1).toList();
        assertThat(duplicates).as("no step may execute twice").isEmpty();
    }
}
