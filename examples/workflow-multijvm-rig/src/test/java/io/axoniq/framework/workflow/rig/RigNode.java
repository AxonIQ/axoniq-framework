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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A workflow node running as its own OS process.
 * <p>
 * Everything a scenario can do to a node goes through here: read what it owns, feed it events, and break it. The
 * process boundary is the point of the class, so there is deliberately no in-JVM variant.
 */
public final class RigNode {

    /**
     * How a node stopped, so an assertion can tell a crash from a clean release.
     */
    public enum Fate {
        /**
         * Still running.
         */
        RUNNING,
        /**
         * {@code SIGKILL}: no shutdown hook ran, so no claim was released.
         */
        CRASHED,
        /**
         * {@code SIGTERM}: Spring's shutdown hook ran and released the claims.
         */
        STOPPED_GRACEFULLY
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
                                                     .connectTimeout(Duration.ofSeconds(5))
                                                     .build();

    private final String nodeId;
    private final Process process;
    private final long pid;
    private final int port;
    private final String storeIdentifier;
    private final Path logFile;
    private final TcpProxy databaseProxy;

    private Fate fate = Fate.RUNNING;

    RigNode(String nodeId, Process process, long pid, int port, String storeIdentifier, Path logFile,
            TcpProxy databaseProxy) {
        this.nodeId = nodeId;
        this.process = process;
        this.pid = pid;
        this.port = port;
        this.storeIdentifier = storeIdentifier;
        this.logFile = logFile;
        this.databaseProxy = databaseProxy;
    }

    /**
     * Returns the explicit node id this node was started with and writes into the token store's owner column.
     *
     * @return the node id.
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * Returns the operating-system process id the node reported about itself.
     *
     * @return the node's pid.
     */
    public long pid() {
        return pid;
    }

    /**
     * Returns the token store's storage identifier as this node sees it. Nodes that disagree are not sharing a store.
     *
     * @return the storage identifier.
     */
    public String storeIdentifier() {
        return storeIdentifier;
    }

    /**
     * Returns the file this node's stdout and stderr are captured to.
     *
     * @return the node's log file.
     */
    public Path logFile() {
        return logFile;
    }

    /**
     * Returns how this node stopped, or {@link Fate#RUNNING}.
     *
     * @return the node's fate.
     */
    public Fate fate() {
        return fate;
    }

    /**
     * Returns whether the OS process is still alive.
     *
     * @return {@code true} while the process runs.
     */
    public boolean alive() {
        return process.isAlive();
    }

    /**
     * Returns the segments this node currently holds a token claim for.
     *
     * @return the claimed segment ids.
     */
    public Set<Integer> segments() {
        return StreamSupport.stream(status().get("segments").spliterator(), false)
                            .map(JsonNode::asInt)
                            .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Returns the ids of the workflow instances this node currently runs.
     *
     * @return the running workflow ids.
     */
    public List<String> workflows() {
        return StreamSupport.stream(status().get("workflows").spliterator(), false)
                            .map(JsonNode::asText)
                            .sorted()
                            .toList();
    }

    /**
     * Returns the workflow instances of one run that this node currently holds.
     * <p>
     * The event store is shared by the whole suite, so a node that replays it restores instances of other scenarios
     * too. Every residency assertion has to be about one run's own ids.
     *
     * @param runPrefix the id prefix of the run.
     * @return the running workflow ids of that run.
     */
    public List<String> workflows(String runPrefix) {
        return workflows().stream().filter(id -> id.startsWith(runPrefix)).toList();
    }

    /**
     * Changes how many segments this node may hold. A node never steals a live claim, so growing capacity at steady
     * state is stable; it only takes effect when a peer's claims expire.
     *
     * @param segments the new capacity.
     */
    public void capacity(int segments) {
        post("/rig/capacity?segments=" + segments);
    }

    /**
     * Returns the maximum number of segments this node's processor will hold.
     *
     * @return the node's segment cap.
     */
    public int maxCapacity() {
        return effectiveConfiguration().get("maxCapacity").asInt();
    }

    /**
     * Returns the configuration this node actually ended up with, as opposed to the one it was given. Segment count
     * in particular is only honoured on a virgin token store, so {@code segmentCount} and {@code
     * requestedSegmentCount} can differ.
     *
     * @return the effective processor configuration.
     */
    public JsonNode effectiveConfiguration() {
        return status().get("effective");
    }

    /**
     * Returns, per committed event in the shared event store, the id of the node that wrote it. Read back from the
     * store, so the attribution outlives the writer.
     *
     * @return the writer node id of every committed event, oldest first.
     */
    public List<String> committedEventWriters() {
        return StreamSupport.stream(get("/rig/committed-events").spliterator(), false)
                            .map(entry -> entry.get("writer").asText())
                            .toList();
    }

    /**
     * Reads the node's observation endpoint.
     *
     * @return the raw status document.
     */
    public JsonNode status() {
        return get("/rig/status");
    }

    /**
     * Publishes start events for the given workflow ids into the shared event store through this node.
     *
     * @param workflowIds ids to start.
     */
    public void startWorkflows(List<String> workflowIds) {
        post("/rig/publish?type=start&id=" + String.join(",", workflowIds));
    }

    /**
     * Publishes resume events for the given workflow ids into the shared event store through this node.
     *
     * @param workflowIds ids to resume.
     */
    public void resumeWorkflows(List<String> workflowIds) {
        post("/rig/publish?type=resume&id=" + String.join(",", workflowIds));
    }

    /**
     * Publishes {@code OrderPlaced} events, starting {@link DurableWaitWorkflow} instances.
     *
     * @param workflowIds ids to start.
     */
    public void placeOrders(List<String> workflowIds) {
        post("/rig/publish?type=order&id=" + String.join(",", workflowIds));
    }

    /**
     * Publishes the {@code PaymentReceived} events the durable-wait instances are waiting for.
     *
     * @param workflowIds ids to pay.
     */
    public void payOrders(List<String> workflowIds) {
        post("/rig/publish?type=payment&id=" + String.join(",", workflowIds));
    }

    /**
     * Publishes {@code TimeoutArmed} events, starting {@link TimerWorkflow} instances that wait under a timeout.
     *
     * @param workflowIds ids to start.
     */
    public void armTimeouts(List<String> workflowIds) {
        post("/rig/publish?type=timeout&id=" + String.join(",", workflowIds));
    }

    /**
     * Publishes {@code RetryArmed} events, starting {@link TimerWorkflow} instances whose step fails and is retried.
     *
     * @param workflowIds ids to start.
     */
    public void armRetries(List<String> workflowIds) {
        post("/rig/publish?type=retry&id=" + String.join(",", workflowIds));
    }

    /**
     * Publishes a {@code ParentStarted} event, starting a {@link ParentChildWorkflow} parent that will spawn the given
     * child.
     *
     * @param parentId the parent instance to start.
     * @param childId  the child the parent's own step will spawn.
     */
    public void startParent(String parentId, String childId) {
        post("/rig/publish?type=parent&id=" + parentId + "&childId=" + childId);
    }

    /**
     * Publishes the {@code ChildReleased} events the spawned children are waiting for.
     *
     * @param childIds children to release.
     */
    public void releaseChildren(List<String> childIds) {
        post("/rig/publish?type=release&id=" + String.join(",", childIds));
    }

    /**
     * Empties this node's database connection pool and keeps it empty, so the node holds its claims while being unable
     * to reach the store to extend them. The connections are handed back by the node itself when the hold elapses.
     *
     * @param hold how long the pool stays empty.
     * @return the node's own account of the drain: how many connections it took and what the pool has left.
     */
    public JsonNode drainConnectionPool(Duration hold) {
        return post("/rig/pool/drain?holdMillis=" + hold.toMillis());
    }

    /**
     * Returns every committed outcome of one step of one instance, oldest first, as {@code status@writerNode}.
     * <p>
     * A retried step produces one entry per attempt, so this is the event store's own account of how many attempts
     * there were and which node ran each of them - independent of anything a node says about itself.
     *
     * @param workflowId the instance to look at.
     * @param stepName   the step to look at.
     * @return the step's committed outcomes, oldest first.
     */
    public List<String> stepHistory(String workflowId, String stepName) {
        return StreamSupport.stream(get("/rig/committed-events").spliterator(), false)
                            .filter(entry -> workflowId.equals(entry.get("workflowId").asText())
                                    && stepName.equals(entry.get("stepName").asText()))
                            .map(entry -> entry.get("stepStatus").asText() + "@" + entry.get("writer").asText())
                            .toList();
    }

    /**
     * Returns the stream positions of every committed event of the given type, read back out of the shared event
     * store.
     *
     * @param typeSuffix trailing part of the event type name; the resolver's namespacing is not this test's business.
     * @return the positions of matching events, in stream order.
     */
    public List<Long> committedPositionsOf(String typeSuffix) {
        var positions = StreamSupport.stream(get("/rig/committed-events").spliterator(), false)
                                     .filter(entry -> entry.get("type").asText().endsWith(typeSuffix))
                                     .map(entry -> entry.get("position").asLong())
                                     .toList();
        if (positions.isEmpty()) {
            throw new IllegalStateException("No committed event of type *" + typeSuffix + "; the store holds "
                                                    + committedEventTypes());
        }
        return positions;
    }

    /**
     * Returns the workflow instances for which the given step reached the given status, read out of the shared event
     * store.
     * <p>
     * The event store is the only crash-proof oracle for a step outcome. A side-effect log is not: a step that was in
     * flight when its node was killed is never re-run, so it is missing from the log whether or not the workflow ever
     * got that far.
     *
     * @param stepName   the step to look for.
     * @param stepStatus the status the step must have reached, e.g. {@code COMPLETED}.
     * @return the workflow ids whose step reached that status.
     */
    public Set<String> instancesWithStep(String stepName, String stepStatus) {
        return StreamSupport.stream(get("/rig/committed-events").spliterator(), false)
                            .filter(entry -> stepName.equals(entry.get("stepName").asText())
                                    && stepStatus.equals(entry.get("stepStatus").asText()))
                            .map(entry -> entry.get("workflowId").asText())
                            .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Returns, per workflow instance, the node that committed the given step outcome.
     *
     * @param stepName   the step to look for.
     * @param stepStatus the status the step must have reached.
     * @return workflow id to the node that wrote that step outcome.
     */
    public Map<String, String> writersOfStep(String stepName, String stepStatus) {
        Map<String, String> writers = new TreeMap<>();
        get("/rig/committed-events").forEach(entry -> {
            if (stepName.equals(entry.get("stepName").asText())
                    && stepStatus.equals(entry.get("stepStatus").asText())) {
                writers.put(entry.get("workflowId").asText(), entry.get("writer").asText());
            }
        });
        return writers;
    }

    /**
     * Returns the distinct event type names in the shared event store, so a type-name mismatch is diagnosable.
     *
     * @return the distinct committed event type names.
     */
    public Set<String> committedEventTypes() {
        return StreamSupport.stream(get("/rig/committed-events").spliterator(), false)
                            .map(entry -> entry.get("type").asText())
                            .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Returns the node that wrote every committed event of the given type.
     *
     * @param typeSuffix trailing part of the event type name.
     * @return the writer node ids, in stream order.
     */
    public List<String> committedWritersOf(String typeSuffix) {
        return StreamSupport.stream(get("/rig/committed-events").spliterator(), false)
                            .filter(entry -> entry.get("type").asText().endsWith(typeSuffix))
                            .map(entry -> entry.get("writer").asText())
                            .toList();
    }

    /**
     * Stops this node's event processor, releasing all its token claims immediately. The one fault that hands a
     * segment over without waiting for a claim to expire, so a scenario can rebalance mid-delivery.
     */
    void stopProcessor() {
        post("/rig/processor/stop");
    }

    /**
     * Restarts this node's event processor so it claims segments again.
     */
    void startProcessor() {
        post("/rig/processor/start");
    }

    /**
     * Kills the node with {@code SIGKILL}. No shutdown hook runs, so the node's token claims stay in the store with
     * its own node id until they expire and another node steals them.
     */
    public void kill() {
        process.destroyForcibly();
        awaitExit();
        fate = Fate.CRASHED;
    }

    /**
     * Stops the node with {@code SIGTERM}, letting Spring's shutdown hook release the claims. The counterpart of
     * {@link #kill()} for scenarios comparing a clean release against a crash.
     */
    public void stopGracefully() {
        process.destroy();
        awaitExit();
        fate = Fate.STOPPED_GRACEFULLY;
    }

    /**
     * Returns the operating system's process state letter, {@code T} while the process is stopped by {@code SIGSTOP}.
     * Landing evidence for {@link #pause()} that does not depend on anything the node itself reports.
     *
     * @return the {@code ps} state letter, or {@code ""} when the process is gone.
     */
    public String processState() {
        try {
            var ps = new ProcessBuilder("ps", "-o", "state=", "-p", Long.toString(pid)).start();
            var output = new String(ps.getInputStream().readAllBytes()).trim();
            ps.waitFor(10, TimeUnit.SECONDS);
            return output;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * Freezes the node with {@code SIGSTOP}: the process stays alive and keeps its claims, but stops extending them.
     */
    public void pause() {
        signal("-STOP");
    }

    /**
     * Thaws a {@link #pause() paused} node with {@code SIGCONT}.
     */
    public void resume() {
        signal("-CONT");
    }

    /**
     * Partitions this node from the shared database by dropping its connections and refusing new ones.
     */
    public void partitionFromDatabase() {
        databaseProxy.cut();
    }

    /**
     * Reconnects this node to the shared database.
     */
    public void healDatabasePartition() {
        databaseProxy.heal();
    }

    /**
     * Returns how many database connections the partition dropped, as landing evidence.
     *
     * @return the dropped connection count.
     */
    public int databaseConnectionsDropped() {
        return databaseProxy.connectionsDropped();
    }

    /**
     * Returns everything the node has written to stdout and stderr so far.
     *
     * @return the captured node log.
     */
    public String log() {
        try {
            return Files.readString(logFile);
        } catch (IOException e) {
            return "<unreadable log " + logFile + ": " + e + ">";
        }
    }

    void shutdownIfAlive() {
        if (process.isAlive()) {
            process.destroy();
            try {
                if (!process.waitFor(20, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        databaseProxy.close();
    }

    private void awaitExit() {
        try {
            if (!process.waitFor(20, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Node " + nodeId + " (pid " + pid + ") refused to die");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void signal(String signal) {
        try {
            var result = new ProcessBuilder("kill", signal, Long.toString(pid))
                    .redirectErrorStream(true)
                    .start();
            if (!result.waitFor(10, TimeUnit.SECONDS) || result.exitValue() != 0) {
                throw new IllegalStateException("kill " + signal + " " + pid + " failed for node " + nodeId);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private JsonNode get(String path) {
        return send(HttpRequest.newBuilder(uri(path)).GET());
    }

    private JsonNode post(String path) {
        return send(HttpRequest.newBuilder(uri(path)).POST(HttpRequest.BodyPublishers.noBody()));
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }

    private JsonNode send(HttpRequest.Builder builder) {
        try {
            var response = HTTP.send(builder.timeout(Duration.ofSeconds(30)).build(),
                                     HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "Node " + nodeId + " answered " + response.statusCode() + ": " + response.body());
            }
            return JSON.readTree(response.body());
        } catch (IOException e) {
            throw new UncheckedIOException("Node " + nodeId + " unreachable", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        return "node " + nodeId + " (pid " + pid + ", port " + port + ", " + fate + ")";
    }
}
