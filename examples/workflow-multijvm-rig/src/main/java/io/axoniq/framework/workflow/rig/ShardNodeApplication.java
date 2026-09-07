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

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.jspecify.annotations.Nullable;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.core.correlation.CorrelationDataProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.TrackingTokenSource;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.axoniq.framework.workflow.configuration.WorkflowEventProcessingRegistrationEnhancer.DEFAULT_MODULE_NAME;

/**
 * A single sharding-rig node: an ordinary Spring Boot workflow application, started as its own OS process by
 * {@code ShardCluster}. Nothing here is aware of the rig beyond the observation endpoints in
 * {@link RigObservationController} and the readiness line printed on startup.
 * <p>
 * All configuration arrives as system properties from the launching test: {@code rig.node-id},
 * {@code spring.datasource.url}, {@code axon.axonserver.servers} and {@code axoniq.workflow.initial-segment-count}.
 */
@SpringBootApplication
public class ShardNodeApplication {

    /**
     * Prefix of the single line the rig waits for on the node's stdout. Everything the rig needs to assert that this
     * really is a separate process against the shared store is on it.
     */
    public static final String READY_PREFIX = "RIG-READY ";

    /**
     * Metadata key under which every event carries the id of the node that committed it.
     */
    public static final String WRITER_METADATA_KEY = "rigWriterNode";

    /**
     * Starts the node.
     *
     * @param args ignored; configuration is passed as system properties.
     */
    public static void main(String[] args) {
        SpringApplication.run(ShardNodeApplication.class, args);
    }

    /**
     * Token store shared by every node: one Postgres database, an explicit {@code nodeId} per node, and the default
     * ten-second claim timeout so a crashed node's segments become stealable.
     *
     * @param converter            converter used to de-/serialize tracking tokens.
     * @param entityManagerFactory entity manager factory over the shared Postgres database.
     * @param nodeId               identity of this node, asserted to be distinct by the rig.
     * @param claimTimeoutSeconds  seconds after which another node may steal this node's claims.
     * @return the shared token store.
     */
    @Bean
    @Primary
    public TokenStore tokenStore(GeneralConverter converter,
                                 EntityManagerFactory entityManagerFactory,
                                 @Value("${rig.node-id}") String nodeId,
                                 @Value("${rig.claim-timeout-seconds:10}") long claimTimeoutSeconds) {
        return new JpaTokenStore(
                new JpaTransactionalExecutorProvider(entityManagerFactory),
                converter,
                JpaTokenStoreConfiguration.DEFAULT
                                          .nodeId(nodeId)
                                          .claimTimeout(Duration.ofSeconds(claimTimeoutSeconds))
        );
    }

    /**
     * Holds this node's segment capacity so a scenario can grow or shrink it while the node runs.
     *
     * @param maxClaimedSegments initial capacity; {@code 0} leaves the framework default of "as many as available".
     * @return the mutable capacity holder.
     */
    @Bean
    public NodeCapacity nodeCapacity(@Value("${rig.max-claimed-segments:0}") int maxClaimedSegments) {
        return new NodeCapacity(maxClaimedSegments);
    }

    /**
     * Caps how many segments this node may hold and how often it retries claiming the rest.
     * <p>
     * Without a cap the first node up claims every segment and the second one idles, which is a correct processor but
     * a useless sharding rig. The cap is applied as a type-level customization, so it lands before the workflow
     * module's own customization and neither overwrites the other.
     *
     * @param capacity             holder for the maximum number of segments this node may hold.
     * @param tokenClaimIntervalMs how often the coordinator retries claiming segments it does not hold.
     * @return the enhancer applying both settings.
     */
    @Bean
    public ConfigurationEnhancer rigProcessorTuning(
            NodeCapacity capacity,
            @Value("${rig.token-claim-interval-ms:2000}") long tokenClaimIntervalMs,
            @Value("${rig.batch-size:1}") int batchSize,
            @Value("${rig.coordinator-threads:1}") int coordinatorThreads,
            @Value("${rig.worker-threads:4}") int workerThreads,
            @Value("${rig.initial-position:replay}") String initialPosition) {
        PooledStreamingEventProcessorModule.Customization tuning =
                (axonConfiguration, processorConfiguration) -> {
                    processorConfiguration.tokenClaimInterval(tokenClaimIntervalMs);
                    processorConfiguration.batchSize(batchSize);
                    processorConfiguration.coordinatorExecutor(
                            Executors.newScheduledThreadPool(coordinatorThreads,
                                                             new AxonThreadFactory("RigCoordinator")));
                    processorConfiguration.workerExecutor(
                            Executors.newScheduledThreadPool(workerThreads, new AxonThreadFactory("RigWorker")));
                    processorConfiguration.initialToken(initialTokenFor(initialPosition));
                    if (capacity.get() > 0) {
                        // Read through the holder rather than captured: the coordinator asks the provider on every
                        // pass, so a scenario can grow capacity after a peer dies or shrink it to force a release.
                        processorConfiguration.maxSegmentProvider(processorName -> capacity.get());
                    }
                    return processorConfiguration;
                };
        // Register-if-absent plus decorate: the framework may already have a Customization component, in which case a
        // plain register is skipped silently and the cap would never apply. The decorator appends to whichever one
        // ends up in place.
        return registry -> registry
                .registerIfNotPresent(ComponentDefinition
                                              .ofType(PooledStreamingEventProcessorModule.Customization.class)
                                              .withInstance(PooledStreamingEventProcessorModule.Customization.noOp()))
                .registerDecorator(PooledStreamingEventProcessorModule.Customization.class,
                                   0,
                                   (config, name, delegate) -> delegate.andThen(tuning));
    }

    /**
     * Stamps the committing node's identity onto every event this node appends.
     * <p>
     * Without it no committed event can be attributed to a writer, and a scenario checking for a two-writer window
     * would return a confident wrong answer instead of failing. Application-side only: a correlation data provider is
     * a supported extension point and no engine code is involved.
     * <p>
     * Limit worth knowing: correlation data is attached to messages produced <em>while handling another message</em>.
     * Workflow engine events qualify. Events injected through {@code /rig/publish} outside any handler do not, and
     * come back with a {@code null} writer.
     *
     * @param nodeId identity of this node.
     * @return the provider stamping this node's id on everything it commits.
     */
    @Bean
    public CorrelationDataProvider rigWriterAttribution(@Value("${rig.node-id}") String nodeId) {
        return message -> Map.of(WRITER_METADATA_KEY, nodeId);
    }

    /**
     * Prints the readiness line once the node is fully up. The rig blocks on this line instead of sleeping, and parses
     * the pid, port, node id and token-store identifier out of it.
     *
     * @param nodeId identity of this node.
     * @return the listener printing the readiness line.
     */
    @Bean
    public ApplicationListener<ApplicationReadyEvent> readinessAnnouncer(@Value("${rig.node-id}") String nodeId) {
        return event -> {
            var context = event.getApplicationContext();
            var port = ((WebServerApplicationContext) context).getWebServer().getPort();
            // Deliberately the store the processor itself resolves, not the Spring bean: those are looked up along
            // different paths and only the processor's one decides whether claims are shared or process-local.
            var processorStore = context.getBean(Configuration.class)
                                        .getModuleConfiguration("EventProcessor[" + DEFAULT_MODULE_NAME + "]")
                                        .orElseThrow()
                                        .getComponent(TokenStore.class, "TokenStore[" + DEFAULT_MODULE_NAME + "]");
            System.out.println(READY_PREFIX
                                       + "nodeId=" + nodeId
                                       + " pid=" + ProcessHandle.current().pid()
                                       + " port=" + port
                                       + " storeId=" + processorStore.retrieveStorageIdentifier(null).join()
                                       + " storeType=" + processorStore.getClass().getName());
            System.out.flush();
        };
    }

    /**
     * Where a virgin token store starts streaming from: {@code head} (everything, no replay flag), {@code tail} (only
     * new events) or {@code replay} (the framework default: everything, flagged as a replay).
     */
    private static Function<TrackingTokenSource, CompletableFuture<TrackingToken>> initialTokenFor(String position) {
        return switch (position) {
            case "head" -> source -> source.firstToken(null);
            case "tail" -> source -> source.latestToken(null);
            case "replay" -> source -> source.firstToken(null).thenApply(ReplayToken::createReplayToken);
            default -> throw new IllegalArgumentException("Unknown rig.initial-position '" + position + "'");
        };
    }

    /**
     * How many segments this node may claim. Mutable so a scenario can model a node growing after a peer dies, or
     * shrinking to force it to hand segments back.
     */
    public static final class NodeCapacity {

        private final AtomicInteger value;

        NodeCapacity(int initial) {
            this.value = new AtomicInteger(initial);
        }

        /**
         * Returns the current capacity.
         *
         * @return the maximum number of segments this node will hold.
         */
        public int get() {
            return value.get();
        }

        void set(int capacity) {
            value.set(capacity);
        }
    }

    /**
     * Read-only view on what this node currently owns and runs, plus a way to inject events into the shared event
     * store. Test scope in spirit: it observes the engine through its public API and never steers it.
     */
    @RestController
    public static class RigObservationController {

        private final Configuration configuration;
        private final NodeCapacity capacity;
        private final RigEventPublisher publisher;
        private final DataSource dataSource;
        private final String nodeId;
        private final int requestedSegmentCount;

        RigObservationController(Configuration configuration,
                                 NodeCapacity capacity,
                                 RigEventPublisher publisher,
                                 DataSource dataSource,
                                 @Value("${rig.node-id}") String nodeId,
                                 @Value("${axoniq.workflow.initial-segment-count:4}") int requestedSegmentCount) {
            this.requestedSegmentCount = requestedSegmentCount;
            this.configuration = configuration;
            this.capacity = capacity;
            this.publisher = publisher;
            this.dataSource = dataSource;
            this.nodeId = nodeId;
        }

        /**
         * Reports the segments this node currently holds and the workflow instances it currently runs.
         *
         * @return observation snapshot of this node.
         */
        @GetMapping("/rig/status")
        public Map<String, Object> status() {
            var processor = processor();
            var engine = configuration.getComponent(io.axoniq.framework.workflow.runtime.execution.WorkflowEngine.class);
            var processorTokenStore = processorTokenStore();
            return Map.of(
                    "nodeId", nodeId,
                    "pid", ProcessHandle.current().pid(),
                    "running", processor.isRunning(),
                    "segments", new TreeSet<>(processor.processingStatus().keySet()),
                    "workflows", engine.workflowExecutions()
                                       .stream()
                                       .map(execution -> execution.workflowId())
                                       .sorted()
                                       .toList(),
                    // Probe: the processor resolves its token store inside its own module. If it ever falls back to an
                    // in-memory store, every claim in the rig would be process-local while still looking healthy.
                    "processorTokenStore", processorTokenStore.getClass().getName(),
                    "processorStoreId", processorTokenStore.retrieveStorageIdentifier(null).join(),
                    // The configuration this node ended up with, not the one it was given. Segment count in
                    // particular is only honoured on a virgin token store, so the requested and effective values
                    // diverge silently on a node that joins later or is rolled out with a different setting.
                    "effective", effectiveConfiguration(processor, processorTokenStore)
            );
        }

        private Map<String, Object> effectiveConfiguration(StreamingEventProcessor processor,
                                                           TokenStore tokenStore) {
            var processorConfiguration =
                    processorModule().getComponent(PooledStreamingEventProcessorConfiguration.class);
            return Map.of(
                    "segmentCount", tokenStore.fetchSegments(DEFAULT_MODULE_NAME, null).join().size(),
                    "requestedSegmentCount", requestedSegmentCount,
                    "batchSize", processorConfiguration.batchSize(),
                    "tokenClaimIntervalMs", processorConfiguration.tokenClaimInterval(),
                    "maxCapacity", processor.maxCapacity()
            );
        }

        private StreamingEventProcessor processor() {
            return processorModule().getComponent(StreamingEventProcessor.class, DEFAULT_MODULE_NAME);
        }

        private TokenStore processorTokenStore() {
            return processorModule().getComponent(TokenStore.class, "TokenStore[" + DEFAULT_MODULE_NAME + "]");
        }

        /**
         * The processor and its token store live in the event-processor module's own registry, not in the root one.
         */
        private Configuration processorModule() {
            return configuration.getModuleConfiguration("EventProcessor[" + DEFAULT_MODULE_NAME + "]")
                                .orElseThrow(() -> new IllegalStateException(
                                        "No EventProcessor[" + DEFAULT_MODULE_NAME + "] module in this node"));
        }

        /**
         * Replays the committed event history and reports, per event, which node wrote it.
         * <p>
         * This is the read side of {@link #WRITER_METADATA_KEY}: the attribution is read back out of the shared event
         * store, not out of any node's memory, so it survives the death of the node that wrote it.
         *
         * Each entry also carries the {@code position} of the event in the stream and, where the payload has one, the
         * {@code id} it names. Together with the stored segment token those two answer whether a segment's token had
         * already moved past a given business event.
         *
         * @return one entry per committed event, oldest first.
         */
        @GetMapping("/rig/committed-events")
        public List<Map<String, String>> committedEvents() {
            var source = configuration.getComponent(StreamableEventSource.class);
            List<Map<String, String>> events = new ArrayList<>();
            var firstToken = source.firstToken(null).join();
            var stream = source.open(StreamingCondition.startingFrom(firstToken), null);
            try {
                // The stream is asynchronous: absence of a next entry means "not delivered yet", not "end of
                // history". Drain until it has been quiet for a second, capped so a long-lived Axon Server does not
                // turn this into an unbounded read.
                var quietUntil = System.nanoTime() + Duration.ofSeconds(1).toNanos();
                while (System.nanoTime() < quietUntil && events.size() < 20_000) {
                    var entry = stream.next().orElse(null);
                    if (entry == null) {
                        Thread.onSpinWait();
                        continue;
                    }
                    quietUntil = System.nanoTime() + Duration.ofSeconds(1).toNanos();
                    var message = entry.message();
                    events.add(Map.of(
                            "type", message.type().name(),
                            "writer", String.valueOf(message.metadata().get(WRITER_METADATA_KEY)),
                            // Read off the stream entry itself, so it is the store's own position and not a count of
                            // what this loop happened to see.
                            "position", TrackingToken.fromContext(entry)
                                                     .flatMap(token -> token.position().stream().boxed().findFirst())
                                                     .map(String::valueOf)
                                                     .orElse("-1"),
                            // Engine events name their instance, step and step status. A step outcome read out of the
                            // event store survives the death of the node that produced it, which a side-effect log
                            // does not: a step in flight when a node is killed is never re-run, so its absence from a
                            // log says nothing about whether the step was ever reached.
                            "workflowId", metadataOf(message, "workflowId"),
                            "stepName", metadataOf(message, "stepName"),
                            "stepStatus", metadataOf(message, "stepType")
                    ));
                }
            } finally {
                stream.close();
            }
            return events;
        }

        private static String metadataOf(org.axonframework.messaging.core.Message message, String key) {
            var value = message.metadata().get(key);
            return value == null ? "" : String.valueOf(value);
        }

        /**
         * Changes how many segments this node may hold. Growing it lets a survivor absorb a dead peer's segments;
         * shrinking it makes the coordinator release the surplus.
         *
         * @param segments the new capacity.
         * @return the capacity in force afterwards.
         */
        @PostMapping("/rig/capacity")
        public Map<String, Object> capacity(@RequestParam int segments) {
            capacity.set(segments);
            return Map.of("capacity", capacity.get());
        }

        /**
         * Stops or starts this node's workflow event processor, releasing or re-acquiring its token claims.
         * <p>
         * A fault, not plumbing: stopping the processor releases the claims at once, which is the only way a scenario
         * can move a segment mid-delivery instead of waiting out a claim timeout.
         *
         * @param action {@code stop} or {@code start}.
         * @return whether the processor is running afterwards.
         */
        @PostMapping("/rig/processor/{action}")
        public Map<String, Object> processor(@PathVariable String action) {
            var processor = processor();
            if ("stop".equals(action)) {
                processor.shutdown().join();
            } else {
                processor.start().join();
            }
            return Map.of("running", processor.isRunning());
        }

        /**
         * Publishes a rig event into the shared event store, so both the sending node and its peers observe it.
         *
         * @param type    {@code start}, {@code resume}, {@code order}, {@code payment}, {@code timeout},
         *                {@code retry}, {@code parent} or {@code release}.
         * @param id      workflow id the event carries.
         * @param childId child id, for {@code parent} only.
         * @return the published id.
         */
        @PostMapping("/rig/publish")
        public Map<String, Object> publish(@RequestParam String type,
                                           @RequestParam List<String> id,
                                           @RequestParam(required = false) String childId) {
            for (var workflowId : id) {
                Object payload = switch (type) {
                    case "start" -> new RigWorkflow.RigStartEvent(workflowId);
                    case "resume" -> new RigWorkflow.RigResumeEvent(workflowId);
                    case "order" -> new DurableWaitWorkflow.OrderPlaced(workflowId);
                    case "payment" -> new DurableWaitWorkflow.PaymentReceived(workflowId);
                    case "timeout" -> new TimerWorkflow.TimeoutArmed(workflowId);
                    case "retry" -> new TimerWorkflow.RetryArmed(workflowId);
                    case "parent" -> new ParentChildWorkflow.ParentStarted(workflowId, childId);
                    case "release" -> new ParentChildWorkflow.ChildReleased(workflowId);
                    default -> throw new IllegalArgumentException("Unknown rig event type '" + type + "'");
                };
                publisher.publish(payload);
            }
            return Map.of("published", id);
        }

        /**
         * Takes every connection out of this node's database pool and holds them, so the node keeps its claims but can
         * no longer reach the store to extend them.
         * <p>
         * A fault, and one the rig cannot inject from outside: a pool is exhausted inside the process that owns it. It
         * differs from a partition in that nothing is unreachable - the store is healthy, the network is up, and the
         * node still fails every database call it makes.
         * <p>
         * The connections are released by a thread of their own after {@code holdMillis}, so a scenario cannot leave a
         * node wedged, and the response returns immediately with how many were taken.
         *
         * @param holdMillis how long to keep the pool empty.
         * @return the number of connections held and the pool's own account of what is left.
         */
        @PostMapping("/rig/pool/drain")
        public Map<String, Object> drainPool(@RequestParam long holdMillis) {
            var held = new ArrayList<Connection>();
            try {
                // The pool's own maximum, so the drain is exact rather than a guess: one more acquisition would block
                // for the connection timeout instead of failing, and would make this endpoint hang.
                var maximumPoolSize = dataSource.unwrap(HikariDataSource.class)
                                                .getMaximumPoolSize();
                for (var i = 0; i < maximumPoolSize; i++) {
                    held.add(dataSource.getConnection());
                }
            } catch (SQLException e) {
                throw new IllegalStateException("Could not drain the pool of node " + nodeId, e);
            }
            var pool = dataSourcePool();
            var report = Map.<String, Object>of("nodeId", nodeId,
                                                "held", held.size(),
                                                "activeConnections", pool.getActiveConnections(),
                                                "idleConnections", pool.getIdleConnections(),
                                                "holdMillis", holdMillis);
            Thread.ofPlatform().daemon().name("rig-pool-drain").start(() -> {
                try {
                    Thread.sleep(holdMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    for (var connection : held) {
                        try {
                            connection.close();
                        } catch (SQLException ignored) {
                            // Nothing useful to do while handing a connection back.
                        }
                    }
                }
            });
            return report;
        }

        private HikariPoolMXBean dataSourcePool() {
            try {
                return dataSource.unwrap(HikariDataSource.class).getHikariPoolMXBean();
            } catch (SQLException e) {
                throw new IllegalStateException("No Hikari pool behind the data source of node " + nodeId, e);
            }
        }
    }
}
