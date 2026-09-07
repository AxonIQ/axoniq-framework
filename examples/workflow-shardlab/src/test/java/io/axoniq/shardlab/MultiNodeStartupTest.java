package io.axoniq.shardlab;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Can a workflow cluster of more than one node exist at all on a shared, durable token store?
 */
class MultiNodeStartupTest {

    private static final Duration READY_TIMEOUT = Duration.ofSeconds(90);

    /** Control: a single node over the durable store starts, stops and restarts. */
    @Test
    void singleNodeStartsAndRestarts() {
        try (var cluster = new Cluster("startup-single")) {
            var a = cluster.startNode("A", 4);
            a.awaitReady(READY_TIMEOUT);
            assertThat(cluster.tokens()).as("segments must be initialized in the durable store").hasSize(4);
            a.stopGracefully();
            a.awaitExit();

            var a2 = cluster.startNode("A2", 4);
            a2.awaitReady(READY_TIMEOUT);
            assertThat(cluster.tokens()).hasSize(4);
        }
    }

    /** Restart right after a crash, while the dead node's claims are still in the table but not yet stale. */
    @Test
    void replacementStartedImmediatelyAfterACrashComesUp() {
        try (var cluster = new Cluster("startup-after-crash")) {
            var a = cluster.startNode("A", 4);
            a.awaitReady(READY_TIMEOUT);
            a.kill();
            a.awaitExit();
            assertThat(a.alive()).as("evidence: the node really died").isFalse();
            assertThat(cluster.tokens()).as("evidence: SIGKILL left the claims in place")
                                        .allMatch(r -> !"null".equals(r.get("owner")));

            var b = cluster.startNode("B", 4);
            try {
                b.awaitReady(READY_TIMEOUT);
            } finally {
                System.out.println("replacement alive=" + b.alive()
                                           + " claimFailure=" + b.log().contains("Unable to claim token"));
            }
            assertThat(b.alive()).isTrue();
        }
    }

    /** Scale up: a second node joins a cluster that is already running. */
    @RepeatedTest(3)
    void secondNodeJoinsARunningCluster() {
        try (var cluster = new Cluster("startup-join")) {
            var a = cluster.startNode("A", 4);
            a.awaitReady(READY_TIMEOUT);
            assertThat(cluster.tokens()).hasSize(4);

            var b = cluster.startNode("B", 4);
            b.awaitReady(READY_TIMEOUT);

            assertThat(a.alive()).as("node A must stay up").isTrue();
            assertThat(b.alive()).as("node B must stay up").isTrue();
        }
    }

    /** Fresh deployment: every replica comes up at the same moment against an empty token store. */
    @RepeatedTest(3)
    void threeNodesStartSimultaneouslyOnAnEmptyStore() {
        try (var cluster = new Cluster("startup-simultaneous")) {
            var nodes = List.of(cluster.startNode("A", 8),
                                cluster.startNode("B", 8),
                                cluster.startNode("C", 8));
            try {
                nodes.forEach(n -> n.awaitReady(READY_TIMEOUT));
            } finally {
                nodes.forEach(n -> System.out.println(
                        "node " + n.id() + " alive=" + n.alive()
                                + " initFailure=" + n.log().contains("Could not initialize segments")
                                + " claimFailure=" + n.log().contains("Unable to claim token")));
            }
            assertThat(nodes).allMatch(Cluster.Node::alive, "every replica must be up");
        }
    }
}
