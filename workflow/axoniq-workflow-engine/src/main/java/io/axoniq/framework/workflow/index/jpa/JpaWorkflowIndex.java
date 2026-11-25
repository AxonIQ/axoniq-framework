/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import io.axoniq.framework.workflow.runtime.execution.SegmentKeyRange;
import io.axoniq.framework.workflow.runtime.execution.WaitAssociation;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndex;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitClosed;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WaitOpened;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtCleared;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowEnded;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowStarted;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexRows;
import jakarta.persistence.EntityManager;
import org.axonframework.common.TypeReference;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * JPA implementation of {@link WorkflowIndex}, on the entities {@link WorkflowIndexEntry},
 * {@link WorkflowIndexWaitEntry} and {@link WorkflowIndexSegmentEntry}. Give it the
 * {@link TransactionalExecutorProvider} of the token store, so the rows and the token commit together. When the index
 * lives in another database, use {@link JpaWorkflowIndexConfiguration#storesWithToken(boolean) storesWithToken(false)}.
 * <p>
 * Example usage:
 * <pre>{@code
 * WorkflowConfigurer.create().registerWorkflowIndex(config -> new JpaWorkflowIndex(
 *         new JpaTransactionalExecutorProvider(entityManagerFactory), config.getComponent(Converter.class),
 *         JpaWorkflowIndexConfiguration.DEFAULT));
 * }</pre>
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
public class JpaWorkflowIndex implements WorkflowIndex {

    private static final TypeReference<HashSet<String>> ASSOCIATIONS_TYPE_REF = new TypeReference<>() {
    };
    private static final String MODULE_PARAM = "workflowModule";
    private static final String WORKFLOW_ID_PARAM = "workflowId";
    private static final String STEP_NAME_PARAM = "stepName";

    private final TransactionalExecutorProvider<EntityManager> transactionalExecutorProvider;
    private final Converter converter;
    private final boolean storesWithToken;

    /**
     * Creates a JPA workflow index.
     *
     * @param transactionalExecutorProvider provides the {@link TransactionalExecutor} every statement runs through
     * @param converter                     converts the associations of a wait to and from the bytes of its row
     * @param configuration                 the configuration of this index
     */
    public JpaWorkflowIndex(TransactionalExecutorProvider<EntityManager> transactionalExecutorProvider,
                            Converter converter,
                            JpaWorkflowIndexConfiguration configuration) {
        this.transactionalExecutorProvider = requireNonNull(
                transactionalExecutorProvider, "The TransactionalExecutorProvider must not be null."
        );
        this.converter = requireNonNull(converter, "The Converter must not be null.");
        this.storesWithToken = requireNonNull(configuration, "The configuration must not be null.").storesWithToken();
    }

    @Override
    public CompletableFuture<WorkflowIndexRows> load(String module,
                                                     Segment segment,
                                                     @Nullable ProcessingContext context) {
        var range = SegmentKeyRange.of(segment);
        return executor(context).apply(entityManager -> {
            var running = new HashSet<String>();
            var wakesAt = new HashMap<String, Instant>();
            entityManager.createQuery(
                                 "select e from WorkflowIndexEntry e "
                                         + "where e.workflowModule = :workflowModule "
                                         + "and e.segmentKey between :from and :to",
                                 WorkflowIndexEntry.class
                         )
                         .setParameter(MODULE_PARAM, module)
                         .setParameter("from", range.from())
                         .setParameter("to", range.to())
                         .getResultList()
                         .forEach(entry -> {
                             running.add(entry.getWorkflowId());
                             if (entry.getWakesAt() != null) {
                                 wakesAt.put(entry.getWorkflowId(), Instant.parse(entry.getWakesAt()));
                             }
                         });
            List<WaitAssociation> waits = new ArrayList<>();
            entityManager.createQuery(
                                 "select e from WorkflowIndexWaitEntry e "
                                         + "where e.workflowModule = :workflowModule "
                                         + "and e.segmentKey between :from and :to",
                                 WorkflowIndexWaitEntry.class
                         )
                         .setParameter(MODULE_PARAM, module)
                         .setParameter("from", range.from())
                         .setParameter("to", range.to())
                         .getResultList()
                         .forEach(entry -> waits.add(waitOf(entry)));
            var position = entityManager.createQuery(
                                                "select e.segmentPosition from WorkflowIndexSegmentEntry e "
                                                        + "where e.workflowModule = :workflowModule "
                                                        + "and e.segmentFrom = :from and e.segmentTo = :to",
                                                Long.class
                                        )
                                        .setParameter(MODULE_PARAM, module)
                                        .setParameter("from", range.from())
                                        .setParameter("to", range.to())
                                        .getResultList();
            return new WorkflowIndexRows(
                    running, wakesAt, waits,
                    position.isEmpty() ? OptionalLong.empty() : OptionalLong.of(position.getFirst())
            );
        });
    }

    @Override
    public CompletableFuture<Void> store(String module,
                                         WorkflowIndexChanges changes,
                                         @Nullable ProcessingContext context) {
        return executor(context).accept(entityManager -> {
            // No entity may stay in the persistence context this store shares with the token store.
            List<Object> added = new ArrayList<>();
            for (WorkflowIndexChange change : changes.changes()) {
                apply(entityManager, module, change, added);
            }
            var position = changes.position();
            if (position != null) {
                movePosition(entityManager, module, position, added);
            }
            entityManager.flush();
            added.forEach(entityManager::detach);
        });
    }

    @Override
    public boolean storesWithToken() {
        return storesWithToken;
    }

    private void apply(EntityManager entityManager, String module, WorkflowIndexChange change, List<Object> added) {
        switch (change) {
            case WorkflowStarted started -> {
                if (!hasWorkflow(entityManager, module, started.workflowId())) {
                    add(entityManager, added, new WorkflowIndexEntry(
                            module, started.workflowId(), SegmentKeyRange.keyOf(started.workflowId()), null
                    ));
                }
            }
            case WorkflowEnded ended -> {
                entityManager.createQuery("delete from WorkflowIndexEntry e "
                                                  + "where e.workflowModule = :workflowModule "
                                                  + "and e.workflowId = :workflowId")
                             .setParameter(MODULE_PARAM, module)
                             .setParameter(WORKFLOW_ID_PARAM, ended.workflowId())
                             .executeUpdate();
                entityManager.createQuery("delete from WorkflowIndexWaitEntry e "
                                                  + "where e.workflowModule = :workflowModule "
                                                  + "and e.workflowId = :workflowId")
                             .setParameter(MODULE_PARAM, module)
                             .setParameter(WORKFLOW_ID_PARAM, ended.workflowId())
                             .executeUpdate();
            }
            case WaitOpened opened -> {
                var wait = opened.association();
                removeWait(entityManager, module, wait.workflowId(), wait.stepName());
                var eventName = wait.eventName();
                add(entityManager, added, new WorkflowIndexWaitEntry(
                        module,
                        wait.workflowId(),
                        wait.stepName(),
                        SegmentKeyRange.keyOf(wait.workflowId()),
                        eventName == null ? null : eventName.name(),
                        requireNonNull(converter.convert(new HashSet<>(wait.associations()), byte[].class),
                                       "The associations must convert to bytes.")
                ));
            }
            case WaitClosed closed -> removeWait(entityManager, module, closed.workflowId(), closed.stepName());
            case WakesAtSet set -> {
                // Added when missing: a workflow can hibernate before its start is stored.
                if (setWakesAt(entityManager, module, set.workflowId(), set.wakesAt().toString()) == 0) {
                    add(entityManager, added, new WorkflowIndexEntry(
                            module, set.workflowId(), SegmentKeyRange.keyOf(set.workflowId()), set.wakesAt().toString()
                    ));
                }
            }
            // Never added: a workflow that woke and ended must not return.
            case WakesAtCleared cleared -> setWakesAt(entityManager, module, cleared.workflowId(), null);
        }
    }

    private static void add(EntityManager entityManager, List<Object> added, Object entry) {
        entityManager.persist(entry);
        added.add(entry);
    }

    private static void movePosition(EntityManager entityManager,
                                     String module,
                                     WorkflowIndexChanges.SegmentPosition position,
                                     List<Object> added) {
        var range = position.range();
        // Removes the positions of the segments a split or a merge replaced: those that overlap this range.
        entityManager.createQuery("delete from WorkflowIndexSegmentEntry e "
                                          + "where e.workflowModule = :workflowModule "
                                          + "and e.segmentFrom <= :to and e.segmentTo >= :from "
                                          + "and (e.segmentFrom <> :from or e.segmentTo <> :to)")
                     .setParameter(MODULE_PARAM, module)
                     .setParameter("from", range.from())
                     .setParameter("to", range.to())
                     .executeUpdate();
        int updated = entityManager.createQuery("update WorkflowIndexSegmentEntry e "
                                                        + "set e.segmentPosition = :position "
                                                        + "where e.workflowModule = :workflowModule "
                                                        + "and e.segmentFrom = :from and e.segmentTo = :to")
                                   .setParameter("position", position.position())
                                   .setParameter(MODULE_PARAM, module)
                                   .setParameter("from", range.from())
                                   .setParameter("to", range.to())
                                   .executeUpdate();
        if (updated == 0) {
            add(entityManager, added, new WorkflowIndexSegmentEntry(
                    module, range.from(), range.to(), position.position()
            ));
        }
    }

    private static boolean hasWorkflow(EntityManager entityManager, String module, String workflowId) {
        return entityManager.createQuery("select count(e) from WorkflowIndexEntry e "
                                                 + "where e.workflowModule = :workflowModule "
                                                 + "and e.workflowId = :workflowId", Long.class)
                            .setParameter(MODULE_PARAM, module)
                            .setParameter(WORKFLOW_ID_PARAM, workflowId)
                            .getSingleResult() > 0;
    }

    private static int setWakesAt(EntityManager entityManager,
                                  String module,
                                  String workflowId,
                                  @Nullable String wakesAt) {
        return entityManager.createQuery("update WorkflowIndexEntry e set e.wakesAt = :wakesAt "
                                                 + "where e.workflowModule = :workflowModule "
                                                 + "and e.workflowId = :workflowId")
                            .setParameter("wakesAt", wakesAt)
                            .setParameter(MODULE_PARAM, module)
                            .setParameter(WORKFLOW_ID_PARAM, workflowId)
                            .executeUpdate();
    }

    private static void removeWait(EntityManager entityManager, String module, String workflowId, String stepName) {
        entityManager.createQuery("delete from WorkflowIndexWaitEntry e "
                                          + "where e.workflowModule = :workflowModule "
                                          + "and e.workflowId = :workflowId "
                                          + "and e.stepName = :stepName")
                     .setParameter(MODULE_PARAM, module)
                     .setParameter(WORKFLOW_ID_PARAM, workflowId)
                     .setParameter(STEP_NAME_PARAM, stepName)
                     .executeUpdate();
    }

    private WaitAssociation waitOf(WorkflowIndexWaitEntry entry) {
        Set<String> associations = converter.convert(entry.getAssociations(), ASSOCIATIONS_TYPE_REF.getType());
        var eventName = entry.getEventName();
        return new WaitAssociation(entry.getWorkflowId(),
                                   entry.getStepName(),
                                   eventName == null ? null : new QualifiedName(eventName),
                                   associations == null ? Set.of() : associations);
    }

    private TransactionalExecutor<EntityManager> executor(@Nullable ProcessingContext context) {
        return transactionalExecutorProvider.getTransactionalExecutor(context);
    }
}
