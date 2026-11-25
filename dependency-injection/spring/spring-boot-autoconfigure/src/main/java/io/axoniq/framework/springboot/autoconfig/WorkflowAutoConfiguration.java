/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.springboot.WorkflowProperties;
import io.axoniq.framework.springboot.workflow.WorkflowDefinitionLookup;
import io.axoniq.framework.workflow.configuration.WorkflowSnapshotConfiguration;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Role;

/**
 * Autoconfiguration for workflow infrastructure.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@AutoConfiguration
@ConditionalOnClass(WorkflowEngine.class)
@ConditionalOnProperty(prefix = "axon.workflow", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(WorkflowProperties.class)
public class WorkflowAutoConfiguration {

    /**
     * Provides Spring workflow definition lookup.
     *
     * @return The lookup for annotations for later workflow handling registrations.
     */
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    @Bean
    public static WorkflowDefinitionLookup workflowDefinitionLookup() {
        return new WorkflowDefinitionLookup();
    }

    /**
     * Provides the {@link WorkflowSnapshotConfiguration} of the {@code axon.workflow.snapshots} properties, which
     * decides whether, and how, the workflow engine snapshots its entities.
     * <p>
     * Unless {@code axon.workflow.snapshots.enabled} is set, the workflow engine snapshots the state of each workflow
     * instance, with the policy of its {@code after-events} property, whenever the application can take snapshots,
     * such as when it has a {@code SnapshotStore} bean. Setting it to {@code true} requires a {@code SnapshotStore} bean. Setting it to
     * {@code false} takes no snapshot at all. An application defining its own {@code WorkflowSnapshotConfiguration}
     * bean replaces this one, and the properties with it.
     *
     * @param workflowProperties the {@link WorkflowProperties} holding the snapshot settings
     * @return the {@link WorkflowSnapshotConfiguration} of the {@code axon.workflow.snapshots} properties
     */
    @Bean
    @ConditionalOnMissingBean
    public WorkflowSnapshotConfiguration workflowSnapshotConfiguration(WorkflowProperties workflowProperties) {
        var properties = workflowProperties.getSnapshots();
        var snapshots = new WorkflowSnapshotConfiguration()
                .runningWorkflows(SnapshotPolicy.afterEvents(properties.getRunningWorkflows().getAfterEvents()))
                .workflowState(SnapshotPolicy.afterEvents(properties.getWorkflowState().getAfterEvents()))
                .waitAssociations(SnapshotPolicy.afterEvents(properties.getWaitAssociations().getAfterEvents()));
        var enabled = properties.getEnabled();
        return enabled == null ? snapshots : snapshots.enabled(enabled);
    }
}
