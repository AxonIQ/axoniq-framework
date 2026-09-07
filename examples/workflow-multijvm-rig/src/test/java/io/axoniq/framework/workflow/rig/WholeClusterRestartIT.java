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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.axoniq.framework.workflow.rig.ShardFailoverSmokeIT.awaitSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The whole cluster goes down at once and the whole cluster comes back.
 * <p>
 * Every other start in this rig is a cold start: the token store is empty, the segments are created from scratch and
 * the first node up finds nothing to argue with. A restart is a different operation. The tokens already exist and sit
 * somewhere in the middle of the stream, the claim rows still name processes that no longer exist, instances are
 * half-finished, and both replacements run their startup scan at the same moment over the same rows. Nothing about
 * that path is reached by starting on an empty store.
 * <p>
 * The nodes are killed rather than stopped, so no shutdown hook releases anything: the replacements have to take
 * claims that are still held, in name, by the dead. They come up under fresh node ids so that a claim row or a step log
 * entry naming a dead process after the restart is visible as exactly that, rather than hidden behind a reused name.
 */
@Tag(RigSplit.B)
class WholeClusterRestartIT extends MultiJvmShardingTestBase {

    private static final int SEGMENTS = 4;
    private static final int CAP_PER_NODE = SEGMENTS / 2;
    private static final int WORKFLOWS = 12;
    private static final Duration PATIENCE = Duration.ofSeconds(180);
    private static final String START_EVENT = RigWorkflow.RigStartEvent.class.getSimpleName();

    @Test
    void aClusterRestartedOntoItsOwnTokensPicksUpEveryInstance() {
        var cluster = cluster(SEGMENTS, CAP_PER_NODE);
        var firstGenerationA = cluster.startNode("wcr-a1", cluster.defaults().withTokenClaimIntervalMs(500));
        var firstGenerationB = cluster.startNode("wcr-b1", cluster.defaults().withTokenClaimIntervalMs(500));
        awaitSplit(cluster, firstGenerationA, firstGenerationB);

        // The event store is shared by the whole suite; only this run's ids are this scenario's business.
        var run = "wcr-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        var workflowIds = IntStream.range(0, WORKFLOWS).mapToObj(index -> run + index).toList();
        firstGenerationA.startWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(recorded(cluster, run, "start")).containsExactlyInAnyOrderElementsOf(workflowIds));
        // The one place in the suite that ties the parsed token position to a position known from the event store.
        // Four scenarios assert stored positions never rewind; on a parse that returned something else in the
        // serialized token - a schema version, a digit inside a type name - all four would compare a constant to
        // itself and pass. This is the positive control for that parse: the numbers have to reach a position the
        // store can be asked for independently, and the store already holds every earlier scenario's events, so a
        // small constant cannot satisfy it. Awaited because storing a token is not synchronous with consuming.
        var lastStart = firstGenerationA.committedPositionsOf(START_EVENT).getLast();
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(cluster.storedSegmentPositions().values())
                        .as("every segment has consumed this run's %d start events, the last of them committed at "
                                    + "position %d, so every stored position must have reached it", WORKFLOWS, lastStart)
                        .isNotEmpty()
                        .allSatisfy(position -> assertThat(position).isGreaterThanOrEqualTo(lastStart)));

        var tokensBefore = cluster.storedSegmentPositions();
        assertThat(tokensBefore).as("the store must hold a token per segment before the restart means anything")
                                .hasSize(SEGMENTS);
        assertThat(tokensBefore.values()).as("the restart must resume from somewhere, not from the start of the stream")
                                         .allSatisfy(position -> assertThat(position).isPositive());

        // Everything down, hard. No hook runs, so the claim rows keep naming the dead.
        firstGenerationA.kill();
        firstGenerationB.kill();
        var ownersWhileDown = cluster.claimedSegmentOwners();
        System.out.printf("EVIDENCE restart-down aAlive=%s bAlive=%s claimRows=%s storedPositions=%s%n",
                          firstGenerationA.alive(), firstGenerationB.alive(), ownersWhileDown, tokensBefore);
        assertThat(firstGenerationA.alive()).isFalse();
        assertThat(firstGenerationB.alive()).isFalse();
        assertThat(firstGenerationA.fate()).isEqualTo(RigNode.Fate.CRASHED);
        assertThat(firstGenerationB.fate()).isEqualTo(RigNode.Fate.CRASHED);
        assertThat(Set.copyOf(ownersWhileDown.values()))
                .as("a crashed cluster leaves its claims behind; a released claim would make the restart unopposed")
                .containsExactlyInAnyOrder(firstGenerationA.nodeId(), firstGenerationB.nodeId());

        // Everything up again, onto the tokens and the instances that were already there.
        var secondGenerationA = cluster.startNode("wcr-a2", cluster.defaults().withTokenClaimIntervalMs(500));
        var secondGenerationB = cluster.startNode("wcr-b2", cluster.defaults().withTokenClaimIntervalMs(500));
        await().atMost(PATIENCE).untilAsserted(() -> {
            var owners = cluster.claimedSegmentOwners();
            assertThat(owners).hasSize(SEGMENTS);
            assertThat(owners.values()).doesNotContainNull();
            assertThat(Set.copyOf(owners.values()))
                    .containsExactlyInAnyOrder(secondGenerationA.nodeId(), secondGenerationB.nodeId());
        });
        var tokensAfter = cluster.storedSegmentPositions();
        System.out.printf("EVIDENCE restart-up claimRows=%s storedPositionsBefore=%s storedPositionsAfter=%s%n",
                          cluster.claimedSegmentOwners(), tokensBefore, tokensAfter);
        assertThat(tokensAfter)
                .as("a restart must resume from the stored tokens, never rewind behind them")
                .allSatisfy((segment, position) ->
                                    assertThat(position).isGreaterThanOrEqualTo(tokensBefore.get(segment)));

        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(Stream.concat(resident(secondGenerationA, run).stream(),
                                         resident(secondGenerationB, run).stream()).toList())
                        .containsExactlyInAnyOrderElementsOf(workflowIds));
        System.out.printf("EVIDENCE restart-restored %s=%s %s=%s%n",
                          secondGenerationA.nodeId(), resident(secondGenerationA, run),
                          secondGenerationB.nodeId(), resident(secondGenerationB, run));

        // And the restored instances still make progress.
        secondGenerationB.resumeWorkflows(workflowIds);
        await().atMost(PATIENCE).untilAsserted(() ->
                assertThat(recorded(cluster, run, "resume")).containsExactlyInAnyOrderElementsOf(workflowIds));

        var rows = cluster.stepLog().stream().filter(row -> row.workflowId().startsWith(run)).toList();
        System.out.printf("EVIDENCE restart-progress rows=%d writers=%s%n", rows.size(),
                          rows.stream().collect(Collectors.groupingBy(row -> row.step() + "@" + row.nodeId(),
                                                                      Collectors.counting())));
        assertThat(rows.stream().filter(row -> "resume".equals(row.step()))
                       .map(ShardCluster.StepLogEntry::nodeId).collect(Collectors.toSet()))
                .as("no work after the restart may be attributed to a process that was killed")
                .isSubsetOf(Set.of(secondGenerationA.nodeId(), secondGenerationB.nodeId()));
        assertThat(rows.stream().collect(Collectors.groupingBy(row -> row.workflowId() + "/" + row.step(),
                                                               Collectors.counting())))
                .as("a restart must not make an instance repeat a step it already ran")
                .hasSize(2 * WORKFLOWS)
                .allSatisfy((step, count) -> assertThat(count).isEqualTo(1L));
    }

    private static List<String> resident(RigNode node, String run) {
        return node.workflows().stream().filter(id -> id.startsWith(run)).toList();
    }

    private static Set<String> recorded(ShardCluster cluster, String run, String step) {
        return cluster.stepLog().stream()
                      .filter(row -> row.workflowId().startsWith(run) && step.equals(row.step()))
                      .map(ShardCluster.StepLogEntry::workflowId)
                      .collect(Collectors.toSet());
    }
}
