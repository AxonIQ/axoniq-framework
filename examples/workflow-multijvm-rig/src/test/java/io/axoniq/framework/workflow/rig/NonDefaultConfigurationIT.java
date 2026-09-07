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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Proves the rig's configuration knobs reach the running processor, and that the effective configuration can be read
 * back and compared with the requested one.
 * <p>
 * The configuration here is deliberately awkward: three segments rather than a power of two, a batch size other than
 * the engine's pinned {@code 1}, asymmetric claim timeouts across the two nodes as a stand-in for clock skew, caps
 * that add up to less than the segment count so one segment is left unclaimed on purpose, and a second node asking
 * for a different segment count than the store was initialised with.
 */
@Tag(RigSplit.B)
class NonDefaultConfigurationIT extends MultiJvmShardingTestBase {

    @Test
    void aNonDefaultConfigurationReachesTheProcessorAndIsReadableBack() {
        var cluster = cluster(3, 1);
        var nodeA = cluster.startNode("cfg-a", cluster.defaults()
                                                      .withSegmentCount(3)
                                                      .withMaxClaimedSegments(1)
                                                      .withClaimTimeoutSeconds(7)
                                                      .withTokenClaimIntervalMs(1500)
                                                      .withThreads(2, 6));
        // Deliberately mismatched: a different requested segment count and asymmetric timing, which is what a
        // half-finished rolling deploy and a skewed clock look like.
        var nodeB = cluster.startNode("cfg-b", cluster.defaults()
                                                      .withSegmentCount(16)
                                                      .withMaxClaimedSegments(1)
                                                      .withClaimTimeoutSeconds(20)
                                                      .withTokenClaimIntervalMs(3000));

        var effectiveA = nodeA.effectiveConfiguration();
        var effectiveB = nodeB.effectiveConfiguration();
        System.out.printf("EVIDENCE effective-configuration %s=%s %s=%s%n",
                          nodeA.nodeId(), effectiveA, nodeB.nodeId(), effectiveB);

        // Asymmetric, non-default timing actually reached each processor.
        assertThat(effectiveA.get("tokenClaimIntervalMs").asLong()).isEqualTo(1500);
        assertThat(effectiveB.get("tokenClaimIntervalMs").asLong()).isEqualTo(3000);
        assertThat(effectiveA.get("maxCapacity").asInt()).isEqualTo(1);

        // Batch size is the one knob that does not move. The workflow module pins it to 1 after any customization,
        // so the rig refuses the node rather than letting a scenario believe it varied something.
        assertThatThrownBy(() -> cluster.startNode("cfg-batch", cluster.defaults().withBatchSize(5)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("batch size 1 instead of 5");

        // Three segments, not a power of two, and the store keeps the count the first node created it with even
        // though the second node asked for sixteen. Requested and effective differ, visibly.
        assertThat(effectiveA.get("segmentCount").asInt()).isEqualTo(3);
        assertThat(effectiveB.get("segmentCount").asInt()).isEqualTo(3);
        assertThat(effectiveB.get("requestedSegmentCount").asInt()).isEqualTo(16);

        // Caps of one each over three segments leaves exactly one segment with no owner, and the rig can see it.
        await().atMost(Duration.ofSeconds(120)).untilAsserted(() -> {
            assertThat(nodeA.segments()).hasSize(1);
            assertThat(nodeB.segments()).hasSize(1);
            assertThat(cluster.unclaimedSegments()).hasSize(1);
        });
        var orphan = cluster.unclaimedSegments().iterator().next();
        System.out.printf("EVIDENCE unclaimed-segment segment=%d owners=%s%n",
                          orphan, cluster.claimedSegmentOwners());

        // An instance on the unclaimed segment never runs, and the ids are chosen to land where the test needs them
        // rather than left to chance.
        var run = UUID.randomUUID().toString().substring(0, 8);
        var masks = cluster.segmentMasks();
        var claimed = cluster.claimedSegmentOwners().entrySet().stream()
                             .filter(entry -> entry.getValue() != null)
                             .map(Map.Entry::getKey)
                             .findFirst().orElseThrow();
        var stranded = ShardCluster.workflowIdsOnSegment(orphan, masks.get(orphan), "cfg-" + run + "-orphan-", 3);
        var runnable = ShardCluster.workflowIdsOnSegment(claimed, masks.get(claimed), "cfg-" + run + "-live-", 3);
        nodeA.startWorkflows(Stream.concat(stranded.stream(), runnable.stream()).toList());

        await().atMost(Duration.ofSeconds(120)).untilAsserted(() ->
                assertThat(startedWorkflowIds(cluster)).containsAll(runnable));
        await().during(Duration.ofSeconds(10)).atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                assertThat(startedWorkflowIds(cluster)).doesNotContainAnyElementsOf(stranded));
        System.out.printf("EVIDENCE instances-stranded-on-unclaimed-segment orphanSegment=%d stranded=%s ran=%s%n",
                          orphan, stranded, runnable);
    }

    private static Set<String> startedWorkflowIds(ShardCluster cluster) {
        return cluster.stepLog().stream()
                      .map(ShardCluster.StepLogEntry::workflowId)
                      .collect(Collectors.toSet());
    }
}
