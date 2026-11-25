/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.execution.EventSourcedRunningWorkflows;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWaitAssociations;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.common.configuration.Module;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.util.MetadataUtils.getWorkflowDefinitionId;
import static org.axonframework.eventsourcing.configuration.EventSourcedEntityModule.declarative;

/**
 * The event-sourced entity {@link Module} of one of the three entities the workflow engine sources: the
 * {@link EventSourcedWorkflowState}, the {@link EventSourcedRunningWorkflows} or the
 * {@link EventSourcedWaitAssociations}. It decides whether the entity is snapshotted once the application's components
 * are known.
 * <p>
 * The {@link WorkflowSnapshotConfiguration} component makes this decision when the module is built. The first case
 * that fits wins:
 * <ol>
 *     <li>When it {@link WorkflowSnapshotConfiguration#enabled() enables} snapshots, the entity is snapshotted with its
 *     policy. An application that cannot take snapshots then fails to start.</li>
 *     <li>When it {@link WorkflowSnapshotConfiguration#disabled() disables} snapshots, the entity is not
 *     snapshotted.</li>
 *     <li>An entity with a {@link SnapshotPolicy} component registered under its deprecated name,
 *     {@link WorkflowConfigurationDefaults#WORKFLOW_STATE_SNAPSHOT_POLICY},
 *     {@link WorkflowConfigurationDefaults#RUNNING_WORKFLOWS_SNAPSHOT_POLICY} or
 *     {@link WorkflowConfigurationDefaults#WAIT_ASSOCIATIONS_SNAPSHOT_POLICY}, is snapshotted with that policy, and a
 *     warning is logged. Such a component takes precedence over the policy of the
 *     {@code WorkflowSnapshotConfiguration}. An explicitly enabled or disabled configuration ignores these
 *     components.</li>
 *     <li>The {@link EventSourcedRunningWorkflows} and the {@link EventSourcedWaitAssociations} are not snapshotted.
 *     Each is 1 entity for every segment, and a segment claim with a workflow index does not load it.</li>
 *     <li>Otherwise the {@link EventSourcedWorkflowState} is snapshotted with its policy of the
 *     {@code WorkflowSnapshotConfiguration}, but only when the application can take snapshots: it has a
 *     {@link SnapshotStore} and a {@link GeneralConverter}, and its {@link MessageTypeResolver} resolves the entity.
 *     Without these, the entity is sourced without snapshots, as Axon Framework's snapshotting entity module would
 *     otherwise fail the start of the application.</li>
 * </ol>
 * The decision is made after every {@link org.axonframework.common.configuration.ConfigurationEnhancer} ran, so a
 * {@code SnapshotStore} registered by any enhancer or Spring bean counts, whatever its order. Checking for the
 * {@code SnapshotStore} and the {@code GeneralConverter} does not create them; only the {@code MessageTypeResolver}
 * is created to ask it.
 * <p>
 * A snapshotted entity stores its snapshots through a {@link FailSafeSnapshotStore}, so a {@code SnapshotStore} that
 * fails synchronously while storing a snapshot does not fail the load of the entity: snapshots are an optimization.
 * <p>
 * Internal, because it only exists to make this decision for {@link WorkflowConfigurationDefaults}, which registers it.
 *
 * @param <E> the type of the entity
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
final class WorkflowEntityModule<E> implements Module {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEntityModule.class);

    private final EventSourcedEntityModule.OptionalPhase<String, E> entityModule;
    private final Class<E> entityType;
    private final Function<WorkflowSnapshotConfiguration, SnapshotPolicy> policy;
    private final String legacyPolicyName;
    private final boolean snapshotsByDefault;

    private WorkflowEntityModule(EventSourcedEntityModule.OptionalPhase<String, E> entityModule,
                                 Class<E> entityType,
                                 Function<WorkflowSnapshotConfiguration, SnapshotPolicy> policy,
                                 String legacyPolicyName,
                                 boolean snapshotsByDefault) {
        this.entityModule = entityModule;
        this.entityType = entityType;
        this.policy = policy;
        this.legacyPolicyName = legacyPolicyName;
        this.snapshotsByDefault = snapshotsByDefault;
    }

    /**
     * Creates the module of {@link EventSourcedWorkflowState}, sourcing the state of a workflow instance from the
     * events tagged with its workflow id.
     *
     * @return the module of {@link EventSourcedWorkflowState}
     */
    @SuppressWarnings("deprecation")
    static WorkflowEntityModule<EventSourcedWorkflowState> workflowState() {
        var entityModule = declarative(String.class, EventSourcedWorkflowState.class)
                .messagingModel((c, model) -> model.entityEvolver(
                        (entity, event, context) -> EventSourcedWorkflowState.requireEventSourcedState(
                                entity.evolve(event, context))
                ).build())
                .entityFactory(c -> (identifier, firstEvent, context) -> new EventSourcedWorkflowState(
                        identifier,
                        // No prior event exists yet when loadOrCreate runs ahead of the workflow's start
                        // event becoming durable; empty metadata falls through to the same error below.
                        getWorkflowDefinitionId(firstEvent == null
                                                        ? Metadata.emptyInstance()
                                                        : firstEvent.metadata()).orElseThrow(
                                () -> new IllegalStateException(
                                        ("Workflow state for '%s' cannot be created without "
                                                + "workflowDefinitionId metadata.").formatted(identifier)))))
                .criteriaResolver(c -> (identifier, context) -> EventSourcedWorkflowState.criteriaBuilder(identifier));
        return new WorkflowEntityModule<>(entityModule,
                                          EventSourcedWorkflowState.class,
                                          WorkflowSnapshotConfiguration::workflowState,
                                          WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY,
                                          true);
    }

    /**
     * Creates the module of {@link EventSourcedRunningWorkflows}, sourcing the running-workflow list from the workflow
     * lifecycle events.
     *
     * @return the module of {@link EventSourcedRunningWorkflows}
     */
    @SuppressWarnings("deprecation")
    static WorkflowEntityModule<EventSourcedRunningWorkflows> runningWorkflows() {
        var entityModule = declarative(String.class, EventSourcedRunningWorkflows.class)
                .messagingModel((c, model) -> model.entityEvolver((entity, event, context) -> {
                    entity.evolve(event.metadata());
                    return entity;
                }).build())
                .entityFactory(c -> (identifier, firstEvent, context) -> new EventSourcedRunningWorkflows())
                .criteriaResolver(c -> (identifier, context) -> EventSourcedRunningWorkflows.criteriaBuilder());
        return new WorkflowEntityModule<>(entityModule,
                                          EventSourcedRunningWorkflows.class,
                                          WorkflowSnapshotConfiguration::runningWorkflows,
                                          WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY,
                                          false);
    }

    /**
     * Creates the module of {@link EventSourcedWaitAssociations}, sourcing the open-wait list from the wait events and
     * the workflow lifecycle events.
     *
     * @return the module of {@link EventSourcedWaitAssociations}
     */
    @SuppressWarnings("deprecation")
    static WorkflowEntityModule<EventSourcedWaitAssociations> waitAssociations() {
        var entityModule = declarative(String.class, EventSourcedWaitAssociations.class)
                .messagingModel((c, model) -> model.entityEvolver((entity, event, context) -> {
                    entity.evolve(event);
                    return entity;
                }).build())
                .entityFactory(c -> (identifier, firstEvent, context) -> new EventSourcedWaitAssociations())
                .criteriaResolver(c -> (identifier, context) -> EventSourcedWaitAssociations.criteriaBuilder());
        return new WorkflowEntityModule<>(entityModule,
                                          EventSourcedWaitAssociations.class,
                                          WorkflowSnapshotConfiguration::waitAssociations,
                                          WorkflowConfigurationDefaults.WAIT_ASSOCIATIONS_SNAPSHOT_POLICY,
                                          false);
    }

    @Override
    public String name() {
        return entityModule.build().name();
    }

    @Override
    public Configuration build(Configuration parent, LifecycleRegistry lifecycleRegistry) {
        var snapshotPolicy = snapshotPolicy(parent);
        if (snapshotPolicy != null) {
            entityModule.snapshotPolicy(snapshotPolicy);
            // The given parent belongs to the registry of this module, which the entity module registers its own
            // components in when it is built. The application's SnapshotStore is the one of its parent. Without a
            // SnapshotStore, Axon Framework fails the start with a message naming the missing store.
            var application = parent.getParent();
            if (application != null && application.hasComponent(SnapshotStore.class)) {
                var moduleRegistry = parent.getOptionalComponent(ComponentRegistry.class).orElseThrow(
                        () -> new IllegalStateException(
                                "The module of [%s] has no ComponentRegistry to register its SnapshotStore in."
                                        .formatted(entityType.getName())));
                moduleRegistry.registerComponent(
                        SnapshotStore.class,
                        c -> new FailSafeSnapshotStore(application.getComponent(SnapshotStore.class))
                );
            }
        }
        return entityModule.build().build(parent, lifecycleRegistry);
    }

    @Nullable
    private SnapshotPolicy snapshotPolicy(Configuration config) {
        var settings = config.getOptionalComponent(WorkflowSnapshotConfiguration.class)
                             .orElseGet(WorkflowSnapshotConfiguration::new);
        var hasLegacyPolicy = config.hasComponent(SnapshotPolicy.class, legacyPolicyName);
        if (hasLegacyPolicy && (settings.enabled() || settings.disabled())) {
            logger.warn("The SnapshotPolicy component named [{}] is ignored, as the WorkflowSnapshotConfiguration "
                                + "{} snapshots. Configure the snapshots of the workflow engine through "
                                + "WorkflowConfigurer#snapshots instead.",
                        legacyPolicyName, settings.enabled() ? "enables" : "disables");
        }
        var entityPolicy = policy.apply(settings);
        if (settings.enabled()) {
            return entityPolicy;
        }
        if (settings.disabled()) {
            logger.debug("Snapshots of [{}] are disabled.", entityType.getName());
            return null;
        }
        if (hasLegacyPolicy) {
            logger.warn("The SnapshotPolicy component named [{}] is deprecated. It still snapshots [{}], as "
                                + "snapshots are neither enabled nor disabled, and takes precedence over the "
                                + "WorkflowSnapshotConfiguration's policy of that entity, including one configured "
                                + "through WorkflowConfigurer#snapshots or the axon.workflow.snapshots properties. "
                                + "Configure the snapshots of the workflow engine through WorkflowConfigurer#snapshots "
                                + "instead.",
                        legacyPolicyName, entityType.getName());
            return config.getComponent(SnapshotPolicy.class, legacyPolicyName);
        }
        if (!snapshotsByDefault) {
            logger.debug("Snapshots of [{}] are off, as they are only taken when snapshots are enabled.",
                         entityType.getName());
            return null;
        }
        if (!config.hasComponent(SnapshotStore.class)) {
            logger.debug("Snapshots of [{}] are off, as the application has no SnapshotStore.", entityType.getName());
            return null;
        }
        if (!config.hasComponent(GeneralConverter.class)) {
            logger.info("Snapshots of [{}] are off, as the application has a SnapshotStore, but no GeneralConverter.",
                        entityType.getName());
            return null;
        }
        if (!resolvesEntityType(config)) {
            logger.info("Snapshots of [{}] are off, as the application has a SnapshotStore, but its "
                                + "MessageTypeResolver does not resolve it.",
                        entityType.getName());
            return null;
        }
        logger.debug("Snapshots of [{}] are on, with policy [{}].", entityType.getName(), entityPolicy);
        return entityPolicy;
    }

    private boolean resolvesEntityType(Configuration config) {
        try {
            return config.getOptionalComponent(MessageTypeResolver.class)
                         .flatMap(resolver -> resolver.resolve(entityType))
                         .isPresent();
        } catch (RuntimeException e) {
            logger.debug("The MessageTypeResolver failed to resolve [{}].", entityType.getName(), e);
            return false;
        }
    }
}
