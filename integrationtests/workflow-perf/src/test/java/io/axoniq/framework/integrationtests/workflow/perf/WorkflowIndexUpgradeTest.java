/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow.perf;

import io.axoniq.framework.integrationtests.workflow.perf.HibernationLoadTest.AxonServerRig;
import io.axoniq.framework.integrationtests.workflow.perf.HibernationLoadTest.Jpa;
import io.axoniq.framework.integrationtests.workflow.perf.HibernationLoadTest.Rig;
import io.axoniq.framework.integrationtests.workflow.upgrade.UpgradeCase;
import io.axoniq.framework.integrationtests.workflow.upgrade.UpgradeCase.CaseApproved;
import io.axoniq.framework.integrationtests.workflow.upgrade.UpgradeCase.CaseClosed;
import io.axoniq.framework.integrationtests.workflow.upgrade.UpgradeCase.CaseOpened;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndexConfiguration;
import io.axoniq.framework.workflow.runtime.execution.HibernationProbe;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The second phase of the upgrade test of the workflow index: the current version, with the JPA workflow index, takes
 * over the workflows that the released 5.4.1 engine left in Axon Server and PostgreSQL, without any step of its own.
 * The index tables are added by the schema update of the persistence provider, the way {@code ddl-auto=update} adds
 * them in a Spring Boot application.
 * <p>
 * The first phase, {@code PreIndexPhaseTest} in {@code upgrade-5.4.1}, writes the cases and their stage to a file. Two
 * nodes start at once on the empty index, so both may complete it. Then every node stops, and one node claims every
 * segment from the rows. Every case must be known by exactly one node throughout, run to its end, and run each effect
 * exactly once over both versions.
 *
 * @author Stefan Dragisic
 */
class WorkflowIndexUpgradeTest {

    private static final Path STATE = Path.of(System.getProperty("upgrade.state", "target/upgrade-cases.csv"));
    private static final String PG_URL = System.getProperty("perf.pg.url", "jdbc:postgresql://localhost:55450/perf");
    private static final Duration WAIT = Duration.ofSeconds(Long.getLong("upgrade.waitSeconds", 180L));
    // false runs the same upgrade without the workflow index, as the control of this test.
    private static final boolean INDEX = Boolean.parseBoolean(System.getProperty("upgrade.index", "true"));

    private final Map<String, String> groupOf = new LinkedHashMap<>();
    private final Set<String> closed = new HashSet<>();
    private final List<Node> nodes = new ArrayList<>();
    private final List<AutoCloseable> resources = new ArrayList<>();
    private Client client;

    @AfterEach
    void tearDown() throws Exception {
        for (var resource : resources.reversed()) {
            try {
                resource.close();
            } catch (Exception e) {
                // The next resource still closes.
            }
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    void liveWorkflowsOfTheReleaseBeforeRunToTheirEndOnTheIndex() throws Exception {
        System.setProperty("upgrade.version", "5.5.0");
        for (var line : Files.readAllLines(STATE)) {
            var fields = line.split(",");
            groupOf.put(fields[0], fields[1]);
        }
        assertThat(groupOf).as("the cases the 5.4.1 phase left").hasSize(28);
        Rig rig = new AxonServerRig();
        resources.add(rig);
        client = new Client(rig);
        closed.addAll(casesOf("DONE", "CLOSED_WHILE_DOWN"));

        // when: 2 nodes with the workflow index start at once on the data of 5.4.1
        var first = CompletableFuture.supplyAsync(() -> new Node(rig, "new-1"));
        var second = CompletableFuture.supplyAsync(() -> new Node(rig, "new-2"));
        nodes.add(first.orTimeout(120, TimeUnit.SECONDS).join());
        nodes.add(second.orTimeout(120, TimeUnit.SECONDS).join());

        // then: every case 5.4.1 left open is known by exactly 1 node, and the case that ended on 5.4.1 by none
        awaitKnownExactlyOnce("after the upgrade");

        // and: the index holds the rows of exactly the open cases
        if (INDEX) await("the index holds a row for every open case",
              () -> indexRows("workflowindexentry") == openCases().size() && indexRows("workflowindexmoduleentry") == 1,
              () -> "workflow rows " + indexRows("workflowindexentry") + ", marker rows "
                      + indexRows("workflowindexmoduleentry") + ", open cases " + openCases().size());

        // when: every node stops, and 1 node claims every segment from the rows
        nodes.forEach(Node::close);
        nodes.clear();
        nodes.add(new Node(rig, "new-3"));

        // then: it knows every open case
        awaitKnownExactlyOnce("after a restart on the rows");

        // when: the cases that wait for their approval get it, and every case that waits for its close gets that
        casesOf("WAITING_FOR_APPROVAL").forEach(id -> client.publish(new CaseApproved(id)));
        await("every case ended",
              () -> {
                  closeWaitingCases();
                  return openCases().isEmpty();
              },
              () -> "open: " + openCases() + ", effects: " + UpgradeCase.effectCounts());

        // then: each effect ran exactly once over both versions
        var counts = UpgradeCase.effectCounts();
        for (var entry : groupOf.entrySet()) {
            var id = entry.getKey();
            if (entry.getValue().equals("BRIEF")) {
                assertThat(counts.get(id + "/expire")).as("expire of " + id).isEqualTo(1);
                assertThat(counts).as("effects of " + id).doesNotContainKeys(id + "/approve", id + "/archive");
            } else {
                assertThat(counts.get(id + "/approve")).as("approve of " + id).isEqualTo(1);
                assertThat(counts.get(id + "/archive")).as("archive of " + id).isEqualTo(1);
                assertThat(counts).as("effects of " + id).doesNotContainKey(id + "/expire");
            }
        }

        // and: no row is left once every workflow ended
        if (INDEX) await("the index holds no row",
              () -> indexRows("workflowindexentry") == 0 && indexRows("workflowindexwaitentry") == 0,
              () -> "workflow rows " + indexRows("workflowindexentry") + ", wait rows "
                      + indexRows("workflowindexwaitentry"));
    }

    private List<String> casesOf(String... groups) {
        var wanted = Set.of(groups);
        return groupOf.entrySet().stream().filter(e -> wanted.contains(e.getValue())).map(Map.Entry::getKey).toList();
    }

    /**
     * Returns the cases whose last effect did not run yet: the archive of a case, or the expiry of a brief case.
     */
    private Set<String> openCases() {
        var counts = UpgradeCase.effectCounts();
        var open = new TreeSet<String>();
        groupOf.forEach((id, group) -> {
            if (!counts.containsKey(id + (group.equals("BRIEF") ? "/expire" : "/archive"))) {
                open.add(id);
            }
        });
        return open;
    }

    /**
     * Closes every case that a node holds hibernated on the wait for its close. An event published before that wait
     * opened would not reach the case.
     */
    private void closeWaitingCases() {
        for (var node : nodes) {
            node.openWaitsOfHibernated().forEach((id, waits) -> {
                if (waits.contains("awaitClosed") && closed.add(id)) {
                    client.publish(new CaseClosed(id));
                }
            });
        }
    }

    private void awaitKnownExactlyOnce(String when) {
        await("every open case is known by exactly 1 node " + when,
              () -> {
                  var known = new ArrayList<String>();
                  nodes.forEach(node -> known.addAll(node.knownWorkflowIds()));
                  return known.size() == new HashSet<>(known).size() && new TreeSet<>(known).equals(openCases());
              },
              () -> {
                  var known = new ArrayList<String>();
                  nodes.forEach(node -> known.addAll(node.knownWorkflowIds()));
                  var lost = new TreeSet<>(openCases());
                  known.forEach(lost::remove);
                  var stale = new TreeSet<>(known);
                  stale.removeAll(openCases());
                  return "lost: " + lost + ", stale: " + stale + ", known twice: "
                          + (known.size() - new HashSet<>(known).size());
              });
    }

    private static void await(String what, BooleanSupplier done, Supplier<String> detail) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("Not within " + WAIT.toSeconds() + " s: " + what + ". " + detail.get());
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    private static int indexRows(String table) {
        try (var connection = DriverManager.getConnection(PG_URL, "perf", "perf");
             var statement = connection.createStatement();
             var rows = statement.executeQuery("select count(*) from " + table)) {
            rows.next();
            return rows.getInt(1);
        } catch (SQLException e) {
            // The table does not exist before the first node of the current version starts.
            return -1;
        }
    }

    private static WorkflowModule<SimpleWorkflowContext> module(Jpa jpa) {
        return WorkflowModule.configure(UpgradeCase.MODULE, SimpleWorkflowContext.class)
                             .definition(d -> d.declarative(c -> UpgradeCase::run)
                                               .workflowName(UpgradeCase.WORKFLOW)
                                               .on(EventConditions.fromType(CaseOpened.class))
                                               .customized((c, w) -> w.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("caseId"))))
                             .processorConfiguration(pc -> {
                                 pc.initialSegmentCount(UpgradeCase.SEGMENTS)
                                   .workerExecutor(() -> Executors.newScheduledThreadPool(UpgradeCase.SEGMENTS))
                                   .tokenClaimInterval(500);
                                 return pc.unitOfWorkFactory(new TransactionalUnitOfWorkFactory(
                                         jpa.transactionManager, pc.unitOfWorkFactory()
                                 ));
                             })
                             .withoutHistory()
                             .contextFactory(c -> new SimpleWorkflowContextFactory())
                             .withHibernation()
                             .hibernationInterval(1_000);
    }

    private static WorkflowConfigurer configurerOn(Rig rig) {
        var configurer = WorkflowConfigurer.create();
        var engineOfNode = new ConcurrentHashMap<String, EventStorageEngine>();
        Function<Configuration, EventStorageEngine> storeOfNode =
                cfg -> engineOfNode.computeIfAbsent("engine", key -> rig.engine(cfg));
        configurer.componentRegistry(cr -> cr
                .disableEnhancer(
                        "io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer")
                .disableEnhancer("io.axoniq.framework.postgresql.PostgresqlConfigurationEnhancer")
                .registerComponent(EventStorageEngine.class, storeOfNode::apply)
                .registerComponent(SnapshotStore.class, cfg -> rig.snapshots(cfg, storeOfNode.apply(cfg))));
        return configurer;
    }

    private final class Client implements AutoCloseable {

        private final AxonConfiguration configuration;

        private Client(Rig rig) {
            this.configuration = configurerOn(rig).start();
            resources.add(this);
        }

        void publish(Object payload) {
            var type = configuration.getComponent(MessageTypeResolver.class).resolveOrThrow(payload);
            var message = new GenericEventMessage(type, payload);
            var sink = configuration.getComponent(EventSink.class);
            configuration.getComponent(UnitOfWorkFactory.class)
                         .create()
                         .executeWithResult(ctx -> sink.publish(ctx, message))
                         .orTimeout(60, TimeUnit.SECONDS)
                         .join();
        }

        @Override
        public void close() {
            configuration.shutdown();
        }
    }

    private final class Node implements AutoCloseable {

        private final Jpa jpa;
        private final AxonConfiguration configuration;
        private final WorkflowEngine engine;
        private boolean closed;

        private Node(Rig rig, String id) {
            // The schema update adds the tables of the workflow index next to the token table of 5.4.1.
            this.jpa = new Jpa("update");
            var configurer = configurerOn(rig);
            if (INDEX) {
                configurer.registerWorkflowIndex(cfg -> new JpaWorkflowIndex(
                        jpa.transactions, cfg.getComponent(GeneralConverter.class),
                        JpaWorkflowIndexConfiguration.DEFAULT
                ));
            }
            configurer.componentRegistry(cr -> cr
                    .registerComponent(TokenStore.class, cfg -> new JpaTokenStore(
                            jpa.transactions,
                            cfg.getComponent(GeneralConverter.class),
                            JpaTokenStoreConfiguration.DEFAULT.nodeId(id).claimTimeout(Duration.ofSeconds(4))))
                    .registerModule(module(jpa)));
            this.configuration = configurer.start();
            synchronized (resources) {
                resources.add(this);
            }
            this.engine = find(configuration, WorkflowEngine.class, candidate -> true)
                    .orElseThrow(() -> new IllegalStateException("No WorkflowEngine found"));
        }

        private static <T> Optional<T> find(Configuration configuration, Class<T> type, Predicate<T> filter) {
            return configuration.getComponents(type).values().stream().filter(filter).findFirst()
                                .or(() -> configuration.getModuleConfigurations().stream()
                                                       .map(module -> find(module, type, filter))
                                                       .flatMap(Optional::stream)
                                                       .findFirst());
        }

        Map<String, Set<String>> openWaitsOfHibernated() {
            return HibernationProbe.openWaitsOfHibernated(engine);
        }

        Set<String> knownWorkflowIds() {
            var known = new TreeSet<>(HibernationProbe.hibernatedIds(engine));
            engine.workflowExecutions().forEach(execution -> known.add(execution.workflowId()));
            return known;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                configuration.shutdown();
                jpa.close();
            }
        }
    }
}
