/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.examples.workflow.loadrunner;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.examples.workflow.loadrunner.LoadWorkflow.LoadStarted;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexEntry;
import io.axoniq.framework.workflow.index.jpa.WorkflowIndexInitialization;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.JpaTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa.TokenEntry;
import org.hibernate.jpa.HibernatePersistenceConfiguration;
import org.hibernate.tool.schema.Action;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wires the load runner's application: the {@link LoadWorkflow} in a {@link WorkflowModule}, the event store, a
 * durable token store, and the workflow index.
 * <p>
 * The token store and, unless disabled, the workflow index live in an embedded HSQLDB file database, so they survive a
 * restart of the application just like the events in Axon Server do.
 *
 * @author Allard Buijze
 */
final class LoadRunnerSetup {

    /**
     * The name of the workflow module, which is also the name of its event processor.
     */
    static final String MODULE = "load-workflows";

    private LoadRunnerSetup() {
    }

    /**
     * Creates the configurer of the load runner's application.
     *
     * @param options              the command line options
     * @param entityManagerFactory the entity manager factory of the database holding the token store and index
     * @param statistics           the statistics the workflow's status changes are counted in
     * @return the configurer, ready to be started
     */
    static WorkflowConfigurer configurer(LoadRunnerOptions options,
                                         EntityManagerFactory entityManagerFactory,
                                         LoadStatistics statistics) {
        var workflow = new LoadWorkflow(options.approvalWait());
        var module = WorkflowModule
                .configure(MODULE, SimpleWorkflowContext.class)
                .definition(d -> d
                        .declarative(c -> workflow::execute)
                        .workflowName(LoadWorkflow.NAME)
                        .on(EventConditions.fromType(LoadStarted.class))
                        .customized((c, w) -> w
                                .workflowIdProvider(new PayloadPropertyWorkflowIdProvider("loadId"))
                                .registerWorkflowStatusChangeListener(
                                        WorkflowStatus.STARTED,
                                        (status, context, pc) -> statistics.recordStarted(context.workflowId())
                                )
                                .registerWorkflowStatusChangeListener(
                                        WorkflowStatus.COMPLETED,
                                        (status, context, pc) -> statistics.recordCompleted(context.workflowId())
                                )
                                .registerWorkflowStatusChangeListener(
                                        WorkflowStatus.FAILED,
                                        (status, context, pc) -> statistics.recordFailed()
                                )
                        )
                )
                .processorConfiguration(processor -> processor
                        .initialSegmentCount(options.segments())
                        .workerExecutor(() -> daemonThreadPool(options.segments()))
                        .batchSize(100)
                        // Without new events, the processor only marks its segments caught up when it checks
                        // again, at most this interval (5 seconds by default) later. A short interval keeps that
                        // delay out of the measured startup time.
                        .tokenClaimInterval(500))
                .checkpointInterval(1000)
                // The history projector replays every workflow event at each start, which would dominate the
                // startup this runner measures.
                .withoutHistory();

        var configurer = WorkflowConfigurer.create().registerWorkflowModule(module);
        if (options.hibernate()) {
            configurer.hibernation(hibernation -> hibernation.enabled(true).idleDelay(options.idleDelay()));
        }
        configureEventStore(configurer, options);
        configureTokenStore(configurer, entityManagerFactory);
        configureWorkflowIndex(configurer, options.index(), entityManagerFactory);
        return configurer;
    }

    /**
     * Uses Axon Server as the event store, or an in-memory event store for trying the runner without Axon Server.
     */
    private static void configureEventStore(WorkflowConfigurer configurer, LoadRunnerOptions options) {
        if (options.inMemoryEvents()) {
            configurer.componentRegistry(cr -> cr
                    .disableEnhancer(AxonServerConfigurationEnhancer.class)
                    .registerComponent(EventStorageEngine.class, c -> new InMemoryEventStorageEngine()));
        } else {
            // The Axon Server connector on the classpath configures Axon Server as the event store.
            configurer.componentRegistry(cr -> cr.registerComponent(
                    AxonServerConfiguration.class,
                    c -> AxonServerConfiguration.builder()
                                                .servers(options.axonServer())
                                                .componentName("workflow-load-runner")
                                                .build()
            ));
        }
    }

    /**
     * Registers a {@link JpaTokenStore}, so the workflow processor resumes where it stopped after a restart.
     * <p>
     * The events live in Axon Server, so there is no database transaction to share with event processing: every token
     * store operation runs in a transaction of its own. Without a transaction manager that binds an entity manager to
     * the processing context, the {@link JpaTransactionalExecutorProvider} only provides such transactions when it is
     * asked without a processing context, hence the lambda.
     */
    private static void configureTokenStore(WorkflowConfigurer configurer,
                                            EntityManagerFactory entityManagerFactory) {
        var ownTransactions = new JpaTransactionalExecutorProvider(entityManagerFactory);
        TransactionalExecutorProvider<EntityManager> transactions =
                processingContext -> ownTransactions.findTransactionalExecutor(null);
        configurer.componentRegistry(cr -> cr.registerComponent(
                TokenStore.class,
                c -> new JpaTokenStore(transactions,
                                       c.getComponent(GeneralConverter.class),
                                       JpaTokenStoreConfiguration.DEFAULT)
        ));
    }

    /**
     * The entities the {@link JpaWorkflowIndex} stores its data in.
     */
    private static final List<Class<?>> WORKFLOW_INDEX_ENTITIES =
            List.of(WorkflowIndexEntry.class, WorkflowIndexInitialization.class);

    /**
     * Configures where the engine keeps its workflow index. In the database, a restart restores the running workflows
     * from the index, and only reads the lifecycle events after the processor's token. Without an index, the engine's
     * default, every claim reads the list of all running workflows from the event store. An in-memory index starts
     * empty after a restart, so its first claims read that list too.
     */
    private static void configureWorkflowIndex(WorkflowConfigurer configurer,
                                               LoadRunnerOptions.Index index,
                                               EntityManagerFactory entityManagerFactory) {
        switch (index) {
            case JPA -> configurer.registerWorkflowIndex(config -> JpaWorkflowIndex.builder()
                                                                                 .entityManagerFactory(
                                                                                         entityManagerFactory
                                                                                 )
                                                                                 .build());
            case MEMORY -> configurer.registerWorkflowIndex(config -> new InMemoryWorkflowIndex());
            case NONE -> {
                // No registration: the engine keeps no index by default.
            }
        }
        if (index != LoadRunnerOptions.Index.JPA) {
            clearJpaWorkflowIndex(entityManagerFactory);
        }
    }

    /**
     * Empties the tables of the {@link JpaWorkflowIndex}. A run without that index advances the processor's tokens
     * without writing the index, so the index no longer matches the tokens. Emptying it makes the next run with the
     * index initialize it again from the running workflows in the event store.
     */
    private static void clearJpaWorkflowIndex(EntityManagerFactory entityManagerFactory) {
        entityManagerFactory.runInTransaction(entityManager -> WORKFLOW_INDEX_ENTITIES.forEach(
                entity -> entityManager.createQuery("DELETE FROM " + entity.getSimpleName()).executeUpdate()
        ));
    }

    /**
     * Opens the embedded HSQLDB file database in the data directory, creating its tables when needed.
     *
     * @param options the command line options, naming the data directory
     * @return the entity manager factory of the database
     */
    static EntityManagerFactory entityManagerFactory(LoadRunnerOptions options) {
        // An in-memory event store starts empty, so the tokens and index of an earlier run don't apply to it.
        String database = options.inMemoryEvents() ? "mem:load-runner" : "file:" + options.dataDirectory()
                                                                                          .resolve("load-runner")
                                                                                          .toAbsolutePath();
        var entities = new ArrayList<Class<?>>();
        entities.add(TokenEntry.class);
        entities.addAll(WORKFLOW_INDEX_ENTITIES);
        return new HibernatePersistenceConfiguration("load-runner")
                .jdbcUrl("jdbc:hsqldb:" + database + ";shutdown=true;hsqldb.default_table_type=cached")
                .jdbcCredentials("sa", "")
                .jdbcPoolSize(32)
                .managedClasses(entities)
                .schemaToolingAction(Action.UPDATE)
                .createEntityManagerFactory();
    }

    private static ScheduledExecutorService daemonThreadPool(int threads) {
        var counter = new AtomicInteger();
        return Executors.newScheduledThreadPool(threads, runnable -> {
            var thread = new Thread(runnable, "workflow-processor-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }
}
