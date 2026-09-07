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

import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;

/**
 * A cluster of workflow nodes, each in its own OS process, sharing one Postgres token store and one Axon Server.
 * <p>
 * This is the only way the rig creates a node, and it always forks. There is no in-JVM node, because a single JVM
 * cannot express what sharding tests need: {@code ClockUtils} is JVM-global, the default token-store {@code nodeId} is
 * {@code pid@host} and therefore identical for every in-JVM "node", and closing a Spring context runs shutdown hooks
 * that release claims, which makes a crash indistinguishable from a clean stop.
 * <p>
 * Every {@link #startNode} call re-runs {@link #assertGenuinelyDistributed()}, which fails loudly if two nodes share a
 * pid, a node id, or disagree about the token store's storage identifier.
 */
public final class ShardCluster implements AutoCloseable {

    /**
     * Postgres image with a real {@code owner} column, {@code mayClaim} semantics and claim expiry. Shared by every
     * node and by the assertions that read the claim rows directly.
     */
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("rig")
            .withUsername("rig")
            .withPassword("rig");

    /**
     * A real Axon Server in dev mode with a DCB context, the event store every node sources from.
     */
    static final AxonServerContainer AXON_SERVER = new AxonServerContainer()
            .withAxonServerHostname("localhost")
            .withDevMode(true)
            .withDcbContext(true);

    static {
        POSTGRES.start();
        AXON_SERVER.start();
    }

    private static final String PROCESSOR_NAME = "Workflow";
    private static final Duration READY_TIMEOUT = Duration.ofMinutes(3);
    /**
     * The stream position inside a serialized token, read from the field that holds it rather than from the first digit
     * run in the text.
     * <p>
     * The token stays raw JSON here on purpose; resolving it back into a token class would drag the framework's
     * converter into the rig. But the position has to come from a named field. Taken as "the first number", a schema
     * version, a digit inside a type name or the head of a gap list would all read as a position, and every "never
     * rewound" assertion in the suite would then be comparing a constant to itself.
     */
    private static final Pattern TOKEN_POSITION =
            Pattern.compile("\"(?:globalIndex|index|position|sequence)\"\\s*:\\s*(\\d+)");

    private final int segmentCount;
    private final int maxClaimedSegmentsPerNode;
    private final Path logDirectory;
    private final List<RigNode> nodes = new ArrayList<>();

    /**
     * Creates a cluster description. No process starts until {@link #startNode(String)} is called.
     *
     * @param segmentCount              number of segments the workflow processor is initialized with.
     * @param maxClaimedSegmentsPerNode cap on segments a single node may hold, so segments actually split. Pass
     *                                  {@code 0} to let one node take everything.
     */
    public ShardCluster(int segmentCount, int maxClaimedSegmentsPerNode) {
        this.segmentCount = segmentCount;
        this.maxClaimedSegmentsPerNode = maxClaimedSegmentsPerNode;
        try {
            this.logDirectory = Files.createTempDirectory("shard-rig-");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        resetSharedState();
    }

    /**
     * Starts a node as a new OS process and blocks until it announces readiness on its stdout.
     *
     * @param nodeId explicit identity for the node; it lands in the token store's owner column.
     * @return the started node.
     */
    public RigNode startNode(String nodeId) {
        return startNode(nodeId, defaults());
    }

    /**
     * Returns the cluster's default per-node configuration, as a starting point for a scenario that wants to change
     * one knob or deliberately mismatch two nodes.
     *
     * @return the default node configuration.
     */
    public NodeConfig defaults() {
        return new NodeConfig(segmentCount, maxClaimedSegmentsPerNode, 10, 2000, 1, 1, 4, "replay");
    }

    /**
     * Starts a node with an explicit configuration. Nodes may deliberately disagree - a different segment count on a
     * later node is what a half-finished rolling deploy looks like, and the effective configuration read back from
     * {@link RigNode#effectiveConfiguration()} is how a scenario notices.
     * <p>
     * Nothing is done to the running nodes first: a node joins a cluster whose segments are already claimed, which is
     * what a production deploy does and the only way the rig can prove a join works.
     *
     * @param nodeId            explicit identity for the node; it lands in the token store's owner column.
     * @param nodeConfiguration the node's event-processor configuration.
     * @return the started node.
     */
    public RigNode startNode(String nodeId, NodeConfig nodeConfiguration) {
        TcpProxy databaseProxy;
        try {
            databaseProxy = new TcpProxy(POSTGRES.getHost(), POSTGRES.getMappedPort(5432));
        } catch (IOException e) {
            throw new UncheckedIOException("Could not open the database proxy for node " + nodeId, e);
        }

        var logFile = logDirectory.resolve("node-" + nodeId + ".log");
        var command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", childClasspath(),
                "-Drig.node-id=" + nodeId,
                "-Dspring.datasource.url=jdbc:postgresql://127.0.0.1:" + databaseProxy.port() + "/"
                        + POSTGRES.getDatabaseName(),
                "-Dspring.datasource.username=" + POSTGRES.getUsername(),
                "-Dspring.datasource.password=" + POSTGRES.getPassword(),
                "-Daxon.axonserver.servers=" + AXON_SERVER.getHost() + ":" + AXON_SERVER.getGrpcPort(),
                "-Daxoniq.workflow.initial-segment-count=" + nodeConfiguration.segmentCount(),
                "-Drig.max-claimed-segments=" + nodeConfiguration.maxClaimedSegments(),
                "-Drig.claim-timeout-seconds=" + nodeConfiguration.claimTimeoutSeconds(),
                "-Drig.token-claim-interval-ms=" + nodeConfiguration.tokenClaimIntervalMs(),
                "-Drig.batch-size=" + nodeConfiguration.batchSize(),
                "-Drig.coordinator-threads=" + nodeConfiguration.coordinatorThreads(),
                "-Drig.worker-threads=" + nodeConfiguration.workerThreads(),
                "-Drig.initial-position=" + nodeConfiguration.initialPosition(),
                ShardNodeApplication.class.getName()
        ));

        Process process;
        try {
            process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectOutput(logFile.toFile())
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not fork node " + nodeId, e);
        }

        var ready = awaitReadyLine(nodeId, process, logFile);
        if (ready.get("storeType").toLowerCase(Locale.ROOT).contains("inmemory")) {
            process.destroyForcibly();
            throw notDistributed("node " + nodeId + " resolved " + ready.get("storeType")
                                         + " as its processor token store; claims would be process-local");
        }
        var node = new RigNode(nodeId,
                               process,
                               Long.parseLong(ready.get("pid")),
                               Integer.parseInt(ready.get("port")),
                               ready.get("storeId"),
                               logFile,
                               databaseProxy);
        var mismatch = configurationMismatch(node, nodeConfiguration);
        if (mismatch != null) {
            node.shutdownIfAlive();
            throw new AssertionError("Node " + nodeId + " did not get the configuration it was given: " + mismatch);
        }
        nodes.add(node);
        assertGenuinelyDistributed();
        return node;
    }

    /**
     * Per-node event-processor configuration. Everything here is a suspect: sharding is only as correct as the
     * settings nobody deliberately varied.
     *
     * @param segmentCount         segments to initialize the processor with. Only honoured on a virgin token store.
     * @param maxClaimedSegments   cap on segments this node holds; {@code 0} means "as many as available". Setting the
     *                             caps so they do not add up to the segment count leaves segments unclaimed on
     *                             purpose.
     * @param claimTimeoutSeconds  how long this node's claims survive without being extended.
     * @param tokenClaimIntervalMs how often this node retries claiming segments it does not hold.
     * @param batchSize            events per processing batch; the engine pins this to 1 by default.
     * @param coordinatorThreads   coordinator pool size.
     * @param workerThreads        worker pool size.
     * @param initialPosition      {@code head}, {@code tail} or {@code replay}.
     */
    public record NodeConfig(int segmentCount,
                             int maxClaimedSegments,
                             int claimTimeoutSeconds,
                             long tokenClaimIntervalMs,
                             int batchSize,
                             int coordinatorThreads,
                             int workerThreads,
                             String initialPosition) {

        /**
         * @param segments the segment count to use.
         * @return a copy with a different segment count.
         */
        public NodeConfig withSegmentCount(int segments) {
            return new NodeConfig(segments, maxClaimedSegments, claimTimeoutSeconds, tokenClaimIntervalMs,
                                  batchSize, coordinatorThreads, workerThreads, initialPosition);
        }

        /**
         * @param segments the cap to use.
         * @return a copy with a different segment cap.
         */
        public NodeConfig withMaxClaimedSegments(int segments) {
            return new NodeConfig(segmentCount, segments, claimTimeoutSeconds, tokenClaimIntervalMs,
                                  batchSize, coordinatorThreads, workerThreads, initialPosition);
        }

        /**
         * @param seconds the claim timeout to use.
         * @return a copy with a different claim timeout.
         */
        public NodeConfig withClaimTimeoutSeconds(int seconds) {
            return new NodeConfig(segmentCount, maxClaimedSegments, seconds, tokenClaimIntervalMs,
                                  batchSize, coordinatorThreads, workerThreads, initialPosition);
        }

        /**
         * @param millis the claim retry interval to use.
         * @return a copy with a different claim retry interval.
         */
        public NodeConfig withTokenClaimIntervalMs(long millis) {
            return new NodeConfig(segmentCount, maxClaimedSegments, claimTimeoutSeconds, millis,
                                  batchSize, coordinatorThreads, workerThreads, initialPosition);
        }

        /**
         * Currently rejected by {@code ShardCluster#startNode}: the workflow module pins the processor's batch size
         * to 1 after any customization, so a node cannot actually run with another value.
         *
         * @param size the batch size to use.
         * @return a copy with a different batch size.
         */
        public NodeConfig withBatchSize(int size) {
            return new NodeConfig(segmentCount, maxClaimedSegments, claimTimeoutSeconds, tokenClaimIntervalMs,
                                  size, coordinatorThreads, workerThreads, initialPosition);
        }

        /**
         * @param coordinators coordinator pool size.
         * @param workers      worker pool size.
         * @return a copy with different pool sizes.
         */
        public NodeConfig withThreads(int coordinators, int workers) {
            return new NodeConfig(segmentCount, maxClaimedSegments, claimTimeoutSeconds, tokenClaimIntervalMs,
                                  batchSize, coordinators, workers, initialPosition);
        }

        /**
         * @param position {@code head}, {@code tail} or {@code replay}.
         * @return a copy starting from a different position.
         */
        public NodeConfig withInitialPosition(String position) {
            return new NodeConfig(segmentCount, maxClaimedSegments, claimTimeoutSeconds, tokenClaimIntervalMs,
                                  batchSize, coordinatorThreads, workerThreads, position);
        }
    }

    /**
     * Refuses a node whose processor did not actually take the settings it was handed. A knob that silently does
     * nothing is worse than a missing one: every scenario built on it returns a confident wrong answer.
     *
     * @return a description of the first mismatch, or {@code null} when the node is configured as asked.
     */
    private String configurationMismatch(RigNode node, NodeConfig requested) {
        var effective = node.effectiveConfiguration();
        if (effective.get("batchSize").asInt() != requested.batchSize()) {
            return "batch size " + effective.get("batchSize").asInt() + " instead of " + requested.batchSize()
                    + ". The workflow module pins it: AllEventEventHandlingComponent#anyEventInSegments ends with"
                    + " .batchSize(1) and runs after any processor customization, so batch size cannot be varied"
                    + " without an engine change";
        }
        if (effective.get("tokenClaimIntervalMs").asLong() != requested.tokenClaimIntervalMs()) {
            return "token claim interval " + effective.get("tokenClaimIntervalMs").asLong()
                    + " instead of " + requested.tokenClaimIntervalMs();
        }
        if (requested.maxClaimedSegments() > 0
                && effective.get("maxCapacity").asInt() != requested.maxClaimedSegments()) {
            return "segment cap " + effective.get("maxCapacity").asInt() + " instead of "
                    + requested.maxClaimedSegments() + "; one node would take every segment and the split would"
                    + " be fake";
        }
        return null;
    }

    /**
     * Returns every node started by this cluster, dead ones included.
     *
     * @return the nodes in start order.
     */
    public List<RigNode> nodes() {
        return List.copyOf(nodes);
    }

    /**
     * Reads the token store's claim rows straight from Postgres: the authoritative answer to who owns which segment,
     * independent of anything a node reports about itself.
     *
     * @return segment id to owning node id, with {@code null} for an unclaimed segment.
     */
    public Map<Integer, String> claimedSegmentOwners() {
        var owners = new TreeMap<Integer, String>();
        withConnection(connection -> {
            try (var statement = connection.prepareStatement(
                    "SELECT segment, owner FROM " + tokenTable(connection) + " WHERE processor_name = ?")) {
                statement.setString(1, PROCESSOR_NAME);
                try (var results = statement.executeQuery()) {
                    while (results.next()) {
                        owners.put(results.getInt("segment"), results.getString("owner"));
                    }
                }
            }
            return null;
        });
        return owners;
    }

    /**
     * Reads the stored token of each segment straight from Postgres, as raw text.
     * <p>
     * This is the position a node claiming the segment next resumes from. Reading it here rather than asking a node
     * keeps it independent of what any node believes about itself, and keeps it readable while no node holds the
     * segment at all.
     *
     * @return segment id to the stored token's serialized form, absent for a segment without a stored token.
     */
    public Map<Integer, String> storedSegmentTokens() {
        var tokens = new TreeMap<Integer, String>();
        withConnection(connection -> {
            try (var statement = connection.prepareStatement(
                    "SELECT segment, token FROM " + tokenTable(connection) + " WHERE processor_name = ?")) {
                statement.setString(1, PROCESSOR_NAME);
                try (var results = statement.executeQuery()) {
                    while (results.next()) {
                        var token = results.getBytes("token");
                        if (token != null) {
                            tokens.put(results.getInt("segment"), new String(token, StandardCharsets.UTF_8));
                        }
                    }
                }
            }
            return null;
        });
        return tokens;
    }

    /**
     * Returns the stream position inside each segment's stored token.
     *
     * @return segment id to stored position, for every segment whose stored token holds one.
     * @throws IllegalStateException if a stored token carries a number but no position field, which would otherwise
     *                               make every assertion reading these numbers compare a constant.
     */
    public Map<Integer, Long> storedSegmentPositions() {
        var positions = new TreeMap<Integer, Long>();
        storedSegmentTokens().forEach((segment, token) -> {
            var number = TOKEN_POSITION.matcher(token);
            if (number.find()) {
                positions.put(segment, Long.parseLong(number.group(1)));
            } else if (token.matches(".*\\d.*")) {
                throw new IllegalStateException(
                        "The token stored for segment " + segment + " carries numbers but none of them is in a field "
                                + TOKEN_POSITION.pattern() + " recognises, so its stream position cannot be read: "
                                + token + ". Add the field this token type uses rather than falling back to the first "
                                + "number in the text.");
            }
        });
        return positions;
    }

    /**
     * Reads the moment each segment's claim was last taken or extended, straight from Postgres.
     * <p>
     * The owner column cannot tell a live claim from a dead one: an expired claim keeps its owner until some peer
     * bothers to steal it, and no peer does while every node is already at capacity. This timestamp is the only thing
     * the owning node moves, and it moves only when the node extends the claim, so a cluster that stops renewing while
     * no events flow is visible here and nowhere else.
     *
     * @return segment id to the moment its claim was last taken or extended.
     */
    public Map<Integer, Instant> claimTimestamps() {
        var timestamps = new TreeMap<Integer, Instant>();
        withConnection(connection -> {
            // Everything rather than the one column: the field is called timestamp, which needs quoting in Postgres,
            // and whether it ends up quoted depends on Hibernate's naming strategy.
            try (var statement = connection.prepareStatement(
                    "SELECT * FROM " + tokenTable(connection) + " WHERE processor_name = ?")) {
                statement.setString(1, PROCESSOR_NAME);
                try (var results = statement.executeQuery()) {
                    while (results.next()) {
                        timestamps.put(results.getInt("segment"),
                                       OffsetDateTime.parse(results.getString("timestamp")).toInstant());
                    }
                }
            }
            return null;
        });
        return timestamps;
    }

    /**
     * Returns the segments the given node currently holds a claim for, according to the claim rows.
     *
     * @param nodeId the node to look for.
     * @return the segment ids owned by that node.
     */
    public Set<Integer> segmentsOwnedBy(String nodeId) {
        return claimedSegmentOwners().entrySet().stream()
                                     .filter(entry -> nodeId.equals(entry.getValue()))
                                     .map(Map.Entry::getKey)
                                     .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Returns the hash mask of each segment, so a scenario can construct workflow ids that land on a chosen segment
     * instead of hoping random ids spread the way it needs.
     *
     * @return segment id to mask.
     */
    public Map<Integer, Integer> segmentMasks() {
        var masks = new TreeMap<Integer, Integer>();
        withConnection(connection -> {
            try (var statement = connection.prepareStatement(
                    "SELECT segment, mask FROM " + tokenTable(connection) + " WHERE processor_name = ?")) {
                statement.setString(1, PROCESSOR_NAME);
                try (var results = statement.executeQuery()) {
                    while (results.next()) {
                        masks.put(results.getInt("segment"), results.getInt("mask"));
                    }
                }
            }
            return null;
        });
        return masks;
    }

    /**
     * Builds workflow ids that the engine's routing will place on the given segment.
     *
     * @param segmentId the target segment.
     * @param mask      that segment's hash mask, from {@link #segmentMasks()}.
     * @param prefix    prefix for the generated ids.
     * @param count     how many ids to produce.
     * @return ids owned by the given segment.
     */
    public static List<String> workflowIdsOnSegment(int segmentId, int mask, String prefix, int count) {
        List<String> ids = new ArrayList<>();
        for (var i = 0; ids.size() < count; i++) {
            var candidate = prefix + i;
            if ((candidate.hashCode() & mask) == segmentId) {
                ids.add(candidate);
            }
            if (i > 100_000) {
                throw new IllegalStateException("No ids found for segment " + segmentId + " with mask " + mask);
            }
        }
        return ids;
    }

    /**
     * Returns the segments no node currently holds. An instance living on an unclaimed segment simply never runs,
     * which looks like a hang rather than an error, so a scenario has to be able to see this directly.
     *
     * @return the unclaimed segment ids.
     */
    public Set<Integer> unclaimedSegments() {
        return claimedSegmentOwners().entrySet().stream()
                                     .filter(entry -> entry.getValue() == null)
                                     .map(Map.Entry::getKey)
                                     .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Reads the workflow step log: the rig's oracle for forward progress, node attribution and duplication.
     *
     * @return one entry per recorded step execution, in insertion order.
     */
    public List<StepLogEntry> stepLog() {
        List<StepLogEntry> entries = new ArrayList<>();
        withConnection(connection -> {
            try (var statement = connection.prepareStatement(
                    "SELECT workflow_id, step, node_id, logged_at FROM rig_step_log ORDER BY id");
                 var results = statement.executeQuery()) {
                while (results.next()) {
                    entries.add(new StepLogEntry(results.getString("workflow_id"),
                                                 results.getString("step"),
                                                 results.getString("node_id"),
                                                 results.getLong("logged_at")));
                }
            }
            return null;
        });
        return entries;
    }

    /**
     * Reads the step log of one run.
     * <p>
     * The event store is shared by the whole suite and is never reset, so a node replaying it restores instances of
     * other scenarios as well. Filtering on the run's own id prefix is what keeps a scenario's assertions about its
     * own instances; the unfiltered {@link #stepLog()} is only safe for a question that is genuinely about everything.
     *
     * @param runPrefix the id prefix of the run.
     * @return that run's entries, in insertion order.
     */
    public List<StepLogEntry> stepLog(String runPrefix) {
        return stepLog().stream().filter(row -> row.workflowId().startsWith(runPrefix)).toList();
    }

    /**
     * Returns the instances of one run that recorded the given step.
     *
     * @param runPrefix the id prefix of the run.
     * @param step      the step to look for.
     * @return the ids that recorded it.
     */
    public Set<String> recorded(String runPrefix, String step) {
        return stepLog(runPrefix).stream()
                                 .filter(row -> step.equals(row.step()))
                                 .map(StepLogEntry::workflowId)
                                 .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Counts how often each {@code workflowId/step} pair was recorded in one run. Anything above one is a step that
     * ran twice.
     *
     * @param runPrefix the id prefix of the run.
     * @return {@code workflowId/step} to the number of times it was recorded.
     */
    public Map<String, Long> stepExecutionCounts(String runPrefix) {
        return stepLog(runPrefix).stream()
                                 .collect(Collectors.groupingBy(row -> row.workflowId() + "/" + row.step(),
                                                                TreeMap::new,
                                                                Collectors.counting()));
    }

    /**
     * Crashes the shared Postgres server: every node loses the token store at the same moment.
     * <p>
     * The checkpointer is killed rather than the container stopped, for two reasons. Stopping the container makes
     * Docker hand out a new host port on the way back, which would leave every node pointing at nothing and turn a
     * store outage into a permanent one. And a killed auxiliary process is what a Postgres crash actually is: the
     * postmaster terminates every backend, runs recovery over the write-ahead log and comes back with the data
     * intact, which is the shape this models. Nothing is dropped and nothing is reconfigured.
     *
     * @return the pid of the process that was killed, as landing evidence.
     */
    public String crashDatabaseServer() {
        try {
            var pid = POSTGRES.execInContainer("sh", "-c", "ps ax | grep 'check[p]ointer' | awk '{print $1}'")
                              .getStdout().trim();
            if (pid.isEmpty()) {
                throw new IllegalStateException("No Postgres checkpointer to kill; the server is not running");
            }
            var kill = POSTGRES.execInContainer("sh", "-c", "kill -9 " + pid);
            if (kill.getExitCode() != 0) {
                throw new IllegalStateException("Could not kill Postgres pid " + pid + ": " + kill.getStderr());
            }
            return pid;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Fails loudly when the nodes are not genuinely separate processes against one store. Called after every node
     * start, and callable from a scenario that wants to re-check mid-run.
     */
    public void assertGenuinelyDistributed() {
        var live = nodes.stream().filter(RigNode::alive).toList();
        var testJvmPid = ProcessHandle.current().pid();
        for (var node : live) {
            if (node.pid() == testJvmPid) {
                throw notDistributed("node " + node.nodeId() + " runs inside the test JVM (pid " + testJvmPid + ")");
            }
        }
        for (var i = 0; i < live.size(); i++) {
            for (var j = i + 1; j < live.size(); j++) {
                var left = live.get(i);
                var right = live.get(j);
                if (left.pid() == right.pid()) {
                    throw notDistributed("nodes " + left.nodeId() + " and " + right.nodeId()
                                                 + " share pid " + left.pid());
                }
                if (left.nodeId().equals(right.nodeId())) {
                    throw notDistributed("two nodes were started with node id " + left.nodeId()
                                                 + "; claims cannot contend");
                }
                if (!left.storeIdentifier().equals(right.storeIdentifier())) {
                    throw notDistributed("nodes " + left.nodeId() + " and " + right.nodeId()
                                                 + " report different token store identifiers ("
                                                 + left.storeIdentifier() + " vs " + right.storeIdentifier()
                                                 + "); they are not sharing a store");
                }
            }
        }
    }

    /**
     * Dumps every node's captured output. Call this from a failure path: a rig whose failures are invisible is
     * useless.
     *
     * @return the concatenated node logs.
     */
    public String dumpNodeLogs() {
        var dump = new StringBuilder();
        for (var node : nodes) {
            dump.append("\n===== ").append(node).append(" log ").append(node.logFile()).append(" =====\n")
                .append(node.log());
        }
        return dump.toString();
    }

    @Override
    public void close() {
        for (var node : nodes) {
            node.shutdownIfAlive();
        }
        nodes.clear();
    }

    private AssertionError notDistributed(String detail) {
        return new AssertionError("""

                                          ################################################################
                                          # NOT A MULTI-JVM RUN - this result proves nothing about       \s
                                          # sharding, claims or failover.                                \s
                                          # %s
                                          ################################################################
                                          """.formatted(detail));
    }

    /**
     * Drops the rig's tables so each cluster starts from an empty token store and an empty step log. The event store
     * keeps its history; workflow ids are unique per run, so old events do not interfere.
     */
    private void resetSharedState() {
        withConnection(connection -> {
            try (var statement = connection.createStatement()) {
                var existing = tokenTableOrNull(connection);
                if (existing != null) {
                    statement.execute("DROP TABLE " + existing);
                }
                statement.execute("DROP TABLE IF EXISTS rig_step_log");
                statement.execute("""
                                          CREATE TABLE rig_step_log (
                                              id BIGSERIAL PRIMARY KEY,
                                              workflow_id VARCHAR(255) NOT NULL,
                                              step VARCHAR(64) NOT NULL,
                                              node_id VARCHAR(64) NOT NULL,
                                              logged_at BIGINT NOT NULL
                                          )""");
            }
            return null;
        });
    }

    private String tokenTable(Connection connection) throws SQLException {
        var table = tokenTableOrNull(connection);
        if (table == null) {
            throw new IllegalStateException(
                    "No token table in the shared database yet - has a node started and initialized its segments?");
        }
        return table;
    }

    /**
     * Finds the token table by name rather than assuming one: Hibernate's naming strategy decides whether the JPA
     * {@code TokenEntry} entity lands as {@code token_entry} or {@code tokenentry}, and guessing wrong would turn the
     * strongest ownership assertion in the rig into a silent error.
     */
    private String tokenTableOrNull(Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
             var results = statement.executeQuery()) {
            while (results.next()) {
                var table = results.getString(1);
                if (table.toLowerCase(Locale.ROOT).replace("_", "").equals("tokenentry")) {
                    return table;
                }
            }
        }
        return null;
    }

    private <T> T withConnection(SqlFunction<T> work) {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),
                                                          POSTGRES.getUsername(),
                                                          POSTGRES.getPassword())) {
            return work.apply(connection);
        } catch (SQLException e) {
            throw new IllegalStateException("Shared database query failed", e);
        }
    }

    private Map<String, String> awaitReadyLine(String nodeId, Process process, Path logFile) {
        var deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            var line = readyLineIn(logFile);
            if (line != null) {
                return parseReadyLine(line);
            }
            if (!process.isAlive()) {
                throw new IllegalStateException("Node " + nodeId + " died during startup (exit "
                                                        + process.exitValue() + ").\n" + read(logFile));
            }
            sleep(200);
        }
        process.destroyForcibly();
        throw new IllegalStateException("Node " + nodeId + " never became ready within " + READY_TIMEOUT + ".\n"
                                                + read(logFile));
    }

    private static String readyLineIn(Path logFile) {
        for (var line : read(logFile).lines().toList()) {
            if (line.startsWith(ShardNodeApplication.READY_PREFIX)) {
                return line;
            }
        }
        return null;
    }

    private static Map<String, String> parseReadyLine(String line) {
        var values = new LinkedHashMap<String, String>();
        for (var token : line.substring(ShardNodeApplication.READY_PREFIX.length()).trim().split("\\s+")) {
            var split = token.indexOf('=');
            values.put(token.substring(0, split), token.substring(split + 1));
        }
        return values;
    }

    private static String read(Path logFile) {
        try {
            return Files.exists(logFile) ? Files.readString(logFile) : "";
        } catch (IOException e) {
            return "";
        }
    }

    private static void sleep(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Hands the test JVM's own classpath to the forked node. Failsafe is configured with
     * {@code useManifestOnlyJar=false} in this module so the property holds the real entries.
     */
    private static String childClasspath() {
        var classpath = System.getProperty("java.class.path");
        if (classpath == null || classpath.isBlank()) {
            throw new IllegalStateException("No java.class.path to hand to the node processes");
        }
        return classpath;
    }

    /**
     * One recorded step execution.
     *
     * @param workflowId the workflow instance.
     * @param step       {@code start} or {@code resume}.
     * @param nodeId     the node that ran the step.
     * @param loggedAt   epoch millis at which the node recorded it.
     */
    public record StepLogEntry(String workflowId, String step, String nodeId, long loggedAt) {

    }

    @FunctionalInterface
    private interface SqlFunction<T> {

        T apply(Connection connection) throws SQLException;
    }
}
