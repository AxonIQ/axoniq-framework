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
import io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
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
     * Provides the {@link SnapshotPolicy} of the running-workflow list when snapshots are enabled.
     *
     * @param workflowProperties the {@link WorkflowProperties} holding the snapshot settings
     * @return the {@link SnapshotPolicy} of the running-workflow list
     */
    @Bean(WorkflowConfigurationDefaults.RUNNING_WORKFLOWS_SNAPSHOT_POLICY)
    @ConditionalOnProperty(prefix = "axon.workflow.snapshots", name = "enabled", havingValue = "true")
    public SnapshotPolicy runningWorkflowsSnapshotPolicy(WorkflowProperties workflowProperties) {
        return SnapshotPolicy.afterEvents(workflowProperties.getSnapshots().getRunningWorkflows().getAfterEvents());
    }

    /**
     * Provides the {@link SnapshotPolicy} of the state of each workflow instance when snapshots are enabled.
     *
     * @param workflowProperties the {@link WorkflowProperties} holding the snapshot settings
     * @return the {@link SnapshotPolicy} of the state of each workflow instance
     */
    @Bean(WorkflowConfigurationDefaults.WORKFLOW_STATE_SNAPSHOT_POLICY)
    @ConditionalOnProperty(prefix = "axon.workflow.snapshots", name = "enabled", havingValue = "true")
    public SnapshotPolicy workflowStateSnapshotPolicy(WorkflowProperties workflowProperties) {
        return SnapshotPolicy.afterEvents(workflowProperties.getSnapshots().getWorkflowState().getAfterEvents());
    }
}
