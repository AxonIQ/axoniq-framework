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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The same business event delivered twice, which is what any at-least-once upstream eventually does.
 * <p>
 * On one node this is an ordinary idempotency question. Sharded it is a different one, because the two deliveries need
 * not be handled by the same process: a start event carries a spawn candidate and is routed by hash, while the resume
 * event carries none and is broadcast to every segment, so each copy of it is offered to every node in the cluster.
 * Whatever makes the second copy a no-op therefore has to be a property of the durable state, not of anything a single
 * process remembers.
 * <p>
 * Nothing here is inferred from the absence of an error. The duplicate is proved to have been committed by counting
 * the events that came back out of the event store, and the effect is counted in the step log, whose rows are written
 * from inside the step bodies and so exist once per execution rather than once per delivery. A quiet period is held
 * after each duplicate, because a second execution that has simply not happened yet looks exactly like one that never
 * will.
 */
@Tag(RigSplit.B)
class DuplicateStartEventsIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 4;
    private static final int CAP_PER_NODE = SEGMENTS / 2;
    private static final int WORKFLOWS = 8;

    /**
     * How long a duplicate is given to produce a second execution before it is called a no-op.
     */
    private static final Duration QUIET = Duration.ofSeconds(Integer.getInteger("rig.duplicate.quiet-seconds", 15));

    private static final Duration PATIENCE = Duration.ofSeconds(120);

    @Test
    void aBusinessEventDeliveredTwiceRunsItsWorkflowOnce() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var nodeA = cluster.startNode("dup-a", cluster.defaults().withTokenClaimIntervalMs(500));
        var nodeB = cluster.startNode("dup-b", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, nodeA, nodeB);

        // The event store is shared by the whole suite; only this run's ids are this scenario's business.
        var run = "dup-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(index -> run + index).toList();

        // First delivery of the start event, routed by hash.
        var startsBefore = committed(nodeB, RigWorkflow.RigStartEvent.class.getSimpleName());
        nodeA.startWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(recorded(cluster, run, "start")).containsExactlyInAnyOrderElementsOf(workflowIds));
        var startWriters = writerHistogram(cluster, run, "start");
        assertThat(startWriters.keySet())
                .as("the instances must be spread over both nodes, or the duplicate never crosses a shard boundary")
                .containsExactlyInAnyOrder(nodeA.nodeId(), nodeB.nodeId());

        // Second delivery of exactly the same events, and proof that it really reached the store.
        var startsAfterFirst = committed(nodeB, RigWorkflow.RigStartEvent.class.getSimpleName());
        nodeA.startWorkflows(workflowIds);
        var startsAfterSecond = committed(nodeB, RigWorkflow.RigStartEvent.class.getSimpleName());
        System.out.printf("EVIDENCE duplicate-start committedBefore=%d afterFirst=%d afterSecond=%d "
                                  + "startWriters=%s%n",
                          startsBefore, startsAfterFirst, startsAfterSecond, startWriters);
        assertThat(startsAfterSecond - startsAfterFirst)
                .as("the duplicate has to be committed, or nothing was deduplicated and the result is vacuous")
                .isEqualTo(WORKFLOWS);

        await().during(QUIET).atMost(QUIET.plusSeconds(60)).untilAsserted(() ->
                assertThat(rowsOf(cluster, run, "start"))
                        .as("a start event delivered twice must start its workflow once")
                        .hasSize(WORKFLOWS));
        System.out.printf("EVIDENCE duplicate-start-settled quietSeconds=%d startRows=%d instances=%s%n",
                          QUIET.toSeconds(), rowsOf(cluster, run, "start").size(),
                          resident(nodeA, run).size() + resident(nodeB, run).size());
        assertThat(resident(nodeA, run).size() + resident(nodeB, run).size())
                .as("a duplicate must not leave a second copy of an instance running").isEqualTo(WORKFLOWS);

        // The same again for the event with no spawn candidate, which every segment is offered.
        var resumesBefore = committed(nodeB, RigWorkflow.RigResumeEvent.class.getSimpleName());
        nodeB.resumeWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(recorded(cluster, run, "resume")).containsExactlyInAnyOrderElementsOf(workflowIds));
        var resumesAfterFirst = committed(nodeB, RigWorkflow.RigResumeEvent.class.getSimpleName());
        nodeB.resumeWorkflows(workflowIds);
        var resumesAfterSecond = committed(nodeB, RigWorkflow.RigResumeEvent.class.getSimpleName());
        System.out.printf("EVIDENCE duplicate-resume committedBefore=%d afterFirst=%d afterSecond=%d writers=%s%n",
                          resumesBefore, resumesAfterFirst, resumesAfterSecond,
                          writerHistogram(cluster, run, "resume"));
        assertThat(resumesAfterSecond - resumesAfterFirst)
                .as("the duplicate has to be committed, or nothing was deduplicated and the result is vacuous")
                .isEqualTo(WORKFLOWS);

        await().during(QUIET).atMost(QUIET.plusSeconds(60)).untilAsserted(() ->
                assertThat(rowsOf(cluster, run, "resume"))
                        .as("an event broadcast to every segment must wake its instance once, on one node")
                        .hasSize(WORKFLOWS));
        var rows = cluster.stepLog().stream().filter(row -> row.workflowId().startsWith(run)).toList();
        System.out.printf("EVIDENCE duplicate-settled rows=%d writers=%s%n", rows.size(),
                          rows.stream().collect(Collectors.groupingBy(row -> row.step() + "@" + row.nodeId(),
                                                                      Collectors.counting())));
        assertThat(rows.stream().collect(Collectors.groupingBy(row -> row.workflowId() + "/" + row.step(),
                                                               Collectors.counting())))
                .hasSize(2 * WORKFLOWS)
                .allSatisfy((step, count) -> assertThat(count).isEqualTo(1L));
    }

    private static int committed(RigNode reader, String eventTypeSuffix) {
        return reader.committedPositionsOf(eventTypeSuffix).size();
    }

    private static List<String> resident(RigNode node, String run) {
        return node.workflows().stream().filter(id -> id.startsWith(run)).toList();
    }

    private static List<ShardCluster.StepLogEntry> rowsOf(ShardCluster cluster, String run, String step) {
        return cluster.stepLog().stream()
                      .filter(row -> row.workflowId().startsWith(run) && step.equals(row.step()))
                      .toList();
    }

    private static Set<String> recorded(ShardCluster cluster, String run, String step) {
        return rowsOf(cluster, run, step).stream()
                                         .map(ShardCluster.StepLogEntry::workflowId)
                                         .collect(Collectors.toSet());
    }

    private static Map<String, Long> writerHistogram(ShardCluster cluster, String run, String step) {
        return rowsOf(cluster, run, step).stream()
                                         .collect(Collectors.groupingBy(ShardCluster.StepLogEntry::nodeId,
                                                                        Collectors.counting()));
    }
}
