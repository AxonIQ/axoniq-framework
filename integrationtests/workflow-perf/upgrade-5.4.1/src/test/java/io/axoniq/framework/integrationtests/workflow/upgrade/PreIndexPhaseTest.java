/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow.upgrade;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.framework.axonserver.connector.event.AxonServerEventStorageEngine;
import io.axoniq.framework.axonserver.connector.snapshot.AxonServerSnapshotStore;
import io.axoniq.framework.integrationtests.workflow.upgrade.UpgradeCase.CaseApproved;
import io.axoniq.framework.integrationtests.workflow.upgrade.UpgradeCase.CaseClosed;
import io.axoniq.framework.integrationtests.workflow.upgrade.UpgradeCase.CaseOpened;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.EntityManagerTransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.fail;

/**
 * The first phase of the upgrade test of the workflow index, on the released 5.4.1 engine: events on Axon Server, the
 * JPA token store on PostgreSQL. 5.4.1 has no workflow index and no hibernation.
 * <p>
 * Two nodes leave cases in every stage a running system has at an upgrade. Then the nodes stop, and approvals and
 * closes arrive while no node runs. The cases and their stage go to a file, which the second phase reads.
 *
 * @author Stefan Dragisic
 */
class PreIndexPhaseTest {

    private static final String PG_URL = System.getProperty("perf.pg.url", "jdbc:postgresql://localhost:55450/perf");
    private static final String AXON_SERVER = System.getProperty("perf.axonserver", "localhost:8125");
    private static final Path STATE = Path.of(System.getProperty("upgrade.state", "target/upgrade-cases.csv"));
    private static final boolean CRASH = "crash".equals(System.getProperty("upgrade.stop", "graceful"));
    private static final Duration WAIT = Duration.ofSeconds(90);

    /**
     * The stage each group of cases is in when the 5.4.1 nodes stop, and what the second phase still sends it.
     */
    enum Group {
        WAITING_FOR_APPROVAL, APPROVED_WHILE_DOWN, SLEEPING, WAITING_FOR_CLOSE, CLOSED_WHILE_DOWN, DONE, BRIEF
    }

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (var resource : resources.reversed()) {
            resource.close();
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void leavesLiveWorkflowsInEveryStage() throws Exception {
        System.setProperty("upgrade.version", "5.4.1");
        UpgradeCase.createEffectTable();
        var server = new AxonServer();
        resources.add(server);
        var first = new Node(server, "old-1");
        var second = new Node(server, "old-2");
        var cases = new ArrayList<String>();

        // given: every case but the brief ones is open and waits for its approval
        for (var group : Group.values()) {
            if (group != Group.BRIEF) {
                for (int i = 0; i < 4; i++) {
                    var caseId = group.name().toLowerCase() + "-" + i;
                    cases.add(caseId + "," + group);
                    first.publish(new CaseOpened(caseId, false));
                }
            }
        }
        pause(5_000);

        // when: 3 groups are approved, and sleep their cooldown
        approve(first, Group.WAITING_FOR_CLOSE, Group.CLOSED_WHILE_DOWN, Group.DONE);
        awaitEffect("approve", Group.WAITING_FOR_CLOSE, Group.CLOSED_WHILE_DOWN, Group.DONE);
        pause(UpgradeCase.COOLDOWN.toMillis() + 5_000);

        // and: 1 of them is closed, so it ends on 5.4.1
        close(first, Group.DONE);
        awaitEffect("archive", Group.DONE);

        // and: 1 group is approved right before the upgrade, so it sleeps through it
        approve(first, Group.SLEEPING);
        awaitEffect("approve", Group.SLEEPING);

        // and: brief cases open, so their approval expires after the upgrade
        for (int i = 0; i < 4; i++) {
            var caseId = "brief-" + i;
            cases.add(caseId + "," + Group.BRIEF);
            first.publish(new CaseOpened(caseId, true));
        }
        pause(3_000);

        // then: the nodes stop
        if (CRASH) {
            first.crash();
            second.crash();
        } else {
            first.close();
            second.close();
        }

        // and: approvals and closes arrive while no node runs
        var publisher = new Publisher(server);
        approve(publisher::publish, Group.APPROVED_WHILE_DOWN);
        close(publisher::publish, Group.CLOSED_WHILE_DOWN);
        publisher.close();
        Files.createDirectories(STATE.toAbsolutePath().getParent());
        Files.write(STATE, cases);
    }

    private static void approve(Node node, Group... groups) {
        approve(node::publish, groups);
    }

    private static void approve(java.util.function.Consumer<Object> publish, Group... groups) {
        for (var group : groups) {
            for (int i = 0; i < 4; i++) {
                publish.accept(new CaseApproved(group.name().toLowerCase() + "-" + i));
            }
        }
    }

    private static void close(Node node, Group... groups) {
        close(node::publish, groups);
    }

    private static void close(java.util.function.Consumer<Object> publish, Group... groups) {
        for (var group : groups) {
            for (int i = 0; i < 4; i++) {
                publish.accept(new CaseClosed(group.name().toLowerCase() + "-" + i));
            }
        }
    }

    private static void awaitEffect(String effect, Group... groups) {
        awaitOrFail("the " + effect + " effect of " + List.of(groups), () -> {
            var counts = UpgradeCase.effectCounts();
            for (var group : groups) {
                for (int i = 0; i < 4; i++) {
                    if (!counts.containsKey(group.name().toLowerCase() + "-" + i + "/" + effect)) {
                        return false;
                    }
                }
            }
            return true;
        });
    }

    private static void awaitOrFail(String what, BooleanSupplier done) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("Not within " + WAIT.toSeconds() + " s on 5.4.1: " + what);
            }
            pause(200);
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static WorkflowModule<SimpleWorkflowContext> module(EntityManagerTransactionManager transactions) {
        return WorkflowModule.configure(UpgradeCase.MODULE, SimpleWorkflowContext.class)
                             .definition(d -> d.declarative(c -> UpgradeCase::run)
                                               .workflowName(UpgradeCase.WORKFLOW)
                                               .on(EventConditions.fromType(CaseOpened.class))
                                               .customized((c, w) -> w.workflowIdProvider(
                                                       new PayloadPropertyWorkflowIdProvider("caseId"))))
                             .processorConfiguration(pc -> {
                                 pc.initialSegmentCount(UpgradeCase.SEGMENTS)
                                   .workerExecutor(() -> Executors.newScheduledThreadPool(UpgradeCase.SEGMENTS))
                                   .tokenClaimInterval(500)
                                   // Below the claim timeout of 4 s, so a node extends its claims before they expire.
                                   .claimExtensionThreshold(1_000);
                                 return pc.unitOfWorkFactory(
                                         new TransactionalUnitOfWorkFactory(transactions, pc.unitOfWorkFactory())
                                 );
                             })
                             .withoutHistory()
                             .contextFactory(c -> new SimpleWorkflowContextFactory());
    }

    private static WorkflowConfigurer configurerOn(AxonServer server) {
        var configurer = WorkflowConfigurer.create();
        var engineOfNode = new ConcurrentHashMap<String, EventStorageEngine>();
        Function<org.axonframework.common.configuration.Configuration, EventStorageEngine> storeOfNode =
                cfg -> engineOfNode.computeIfAbsent("engine", key -> new AxonServerEventStorageEngine(
                        server.connection, cfg.getComponent(EventConverter.class)
                ));
        configurer.componentRegistry(cr -> cr
                .disableEnhancer(
                        "io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer")
                .disableEnhancer("io.axoniq.framework.postgresql.PostgresqlConfigurationEnhancer")
                .registerComponent(EventStorageEngine.class, storeOfNode::apply)
                .registerComponent(SnapshotStore.class, cfg -> new AxonServerSnapshotStore(
                        server.connection, cfg.getComponent(EventConverter.class)
                )));
        return configurer;
    }

    private static void publish(AxonConfiguration configuration, Object payload) {
        var type = configuration.getComponent(MessageTypeResolver.class).resolveOrThrow(payload);
        var message = new GenericEventMessage(type, payload);
        var sink = configuration.getComponent(EventSink.class);
        configuration.getComponent(UnitOfWorkFactory.class)
                     .create()
                     .executeWithResult(ctx -> sink.publish(ctx, message))
                     .orTimeout(60, TimeUnit.SECONDS)
                     .join();
    }

    private static final class AxonServer implements AutoCloseable {

        private final AxonServerConnectionFactory factory;
        private final AxonServerConnection connection;

        private AxonServer() {
            var address = AXON_SERVER.split(":");
            this.factory = AxonServerConnectionFactory.forClient("upgrade-5.4.1")
                                                      .routingServers(new ServerAddress(address[0],
                                                                                        Integer.parseInt(address[1])))
                                                      .build();
            this.connection = factory.connect("default");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!connection.isReady() && System.nanoTime() < deadline) {
                pause(200);
            }
        }

        @Override
        public void close() {
            connection.disconnect();
            factory.shutdown();
        }
    }

    /**
     * Publishes events while no node runs. It runs no workflow.
     */
    private final class Publisher implements AutoCloseable {

        private final AxonConfiguration configuration;

        private Publisher(AxonServer server) {
            this.configuration = configurerOn(server).start();
        }

        void publish(Object payload) {
            PreIndexPhaseTest.publish(configuration, payload);
        }

        @Override
        public void close() {
            configuration.shutdown();
        }
    }

    private final class Node implements AutoCloseable {

        private final HikariDataSource dataSource;
        private final EntityManagerFactory factory;
        private final AxonConfiguration configuration;
        private boolean closed;

        private Node(AxonServer server, String id) {
            var config = new HikariConfig();
            config.setJdbcUrl(PG_URL);
            config.setUsername("perf");
            config.setPassword("perf");
            config.setMaximumPoolSize(UpgradeCase.SEGMENTS + 8);
            config.setAutoCommit(false);
            this.dataSource = new HikariDataSource(config);
            this.factory = Persistence.createEntityManagerFactory("upgrade-5.4.1", Map.of(
                    "jakarta.persistence.nonJtaDataSource", dataSource
            ));
            var transactions = new JpaTransactionalExecutorProvider(factory);
            ThreadLocal<EntityManager> entityManagers = ThreadLocal.withInitial(factory::createEntityManager);
            var transactionManager = new EntityManagerTransactionManager(() -> {
                var entityManager = entityManagers.get();
                if (!entityManager.getTransaction().isActive()) {
                    entityManager.clear();
                }
                return entityManager;
            });
            var configurer = configurerOn(server);
            configurer.componentRegistry(cr -> cr
                    .registerComponent(TokenStore.class, cfg -> new JpaTokenStore(
                            transactions,
                            cfg.getComponent(GeneralConverter.class),
                            JpaTokenStoreConfiguration.DEFAULT.nodeId(id).claimTimeout(Duration.ofSeconds(4))))
                    .registerModule(module(transactionManager)));
            this.configuration = configurer.start();
            resources.add(this);
        }

        void publish(Object payload) {
            PreIndexPhaseTest.publish(configuration, payload);
        }

        /** Stops the node the way a crash does for the database: its connections go first. */
        void crash() {
            if (!closed) {
                closed = true;
                factory.close();
                dataSource.close();
                try {
                    configuration.shutdown();
                } catch (RuntimeException e) {
                    // A crashed node cannot release its claims.
                }
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                configuration.shutdown();
                factory.close();
                dataSource.close();
            }
        }
    }
}
