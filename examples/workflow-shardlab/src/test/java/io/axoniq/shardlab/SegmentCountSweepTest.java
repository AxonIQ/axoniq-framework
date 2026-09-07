package io.axoniq.shardlab;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * One node, the whole plausible range of {@code axoniq.workflow.initial-segment-count}, including values that are
 * not powers of two. Every workflow must spawn exactly once and run to completion whatever the count is.
 */
class SegmentCountSweepTest {

    private static final Duration READY = Duration.ofSeconds(90);
    private static final int WORKFLOWS = 24;

    @ParameterizedTest(name = "segments={0}")
    @ValueSource(ints = {1, 2, 3, 7})
    void lowSegmentCounts(int segments) {
        runSweep(segments);
    }

    @ParameterizedTest(name = "segments={0}")
    @ValueSource(ints = {8, 32, 100})
    void highSegmentCounts(int segments) {
        runSweep(segments);
    }

    /**
     * A user who configures segments the way Axon Framework documents it, through
     * {@code axon.eventhandling.processors[Workflow].initial-segment-count}, instead of through the
     * workflow-specific property.
     */
    @org.junit.jupiter.api.Test
    void axonProcessorPropertyConfiguresTheWorkflowProcessor() {
        try (var cluster = new Cluster("sweep-axonprop")) {
            var a = cluster.startProcess(NodeApp.class.getName(), "A", 4, Cluster.POSTGRES.getJdbcUrl(),
                                         "--axon.eventhandling.processors.Workflow.mode=pooled",
                                         "--axon.eventhandling.processors.Workflow.initial-segment-count=16");
            a.awaitReady(READY);
            System.out.println("segments with axon property=16 and workflow property=4: " + cluster.tokens().size());
            assertThat(cluster.tokens()).hasSize(16);
        }
    }

    private void runSweep(int segments) {
        try (var cluster = new Cluster("sweep-" + segments); var publisher = cluster.publisher()) {
            var a = cluster.startNode("A", segments);
            a.awaitReady(READY);
            assertThat(cluster.tokens()).as("the configured segment count must be what gets initialized")
                                        .hasSize(segments);

            var ids = IntStream.range(0, WORKFLOWS).mapToObj(i -> "s" + segments + "-" + i).toList();
            ids.forEach(id -> publisher.publish(new Events.OrderPlaced(id)));
            try {
                await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                        assertThat(step(cluster, "reserve")).containsAll(ids));
            } finally {
                var missing = ids.stream().filter(id -> !step(cluster, "reserve").contains(id)).toList();
                System.out.println("SWEEP segments=" + segments
                                           + " spawned=" + step(cluster, "reserve").size() + "/" + ids.size()
                                           + " missing=" + missing);
            }

            ids.forEach(id -> publisher.publish(new Events.PaymentReceived(id)));
            try {
                await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                        assertThat(step(cluster, "ship")).containsAll(ids));
            } finally {
                System.out.println("SWEEP segments=" + segments
                                           + " shipped=" + step(cluster, "ship").size() + "/" + ids.size());
            }

            var duplicates = cluster.steps().stream()
                                    .collect(Collectors.groupingBy(r -> r.get("wf_id") + "/" + r.get("step"),
                                                                   Collectors.counting()))
                                    .entrySet().stream()
                                    .filter(e -> e.getValue() > 1)
                                    .map(Map.Entry::getKey)
                                    .toList();
            assertThat(duplicates).as("no step may run twice").isEmpty();
        }
    }

    private static Set<String> step(Cluster cluster, String step) {
        return cluster.steps().stream()
                      .filter(r -> r.get("step").equals(step))
                      .map(r -> r.get("wf_id"))
                      .collect(Collectors.toSet());
    }
}
