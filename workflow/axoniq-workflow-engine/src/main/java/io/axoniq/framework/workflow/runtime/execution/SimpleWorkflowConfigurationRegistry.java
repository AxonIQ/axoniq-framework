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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory workflow definition registry implementation.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class SimpleWorkflowConfigurationRegistry
        implements WorkflowConfigurationRegistry<SimpleWorkflowConfigurationRegistry> {

    private final ConcurrentHashMap<QualifiedName, List<PredicatedWorkflowConfiguration>> workflowsConfigurations = new ConcurrentHashMap<>();

    /**
     * Highest-version configurations per start-event name, recomputed on registration. The lookup runs for every
     * streamed event (see {@link WorkflowEngineSequencingPolicy} and {@code WorkflowEngine}), so it must not re-derive
     * version ordering per call; events without a registered start condition resolve to an empty list without creating
     * an entry.
     */
    private final ConcurrentHashMap<QualifiedName, List<PredicatedWorkflowConfiguration>> highestVersionConfigurations = new ConcurrentHashMap<>();

    @Override
    public SimpleWorkflowConfigurationRegistry register(
            EventCondition eventCondition,
            WorkflowConfiguration<?> workflowConfiguration
    ) {
        Objects.requireNonNull(workflowConfiguration, "The given workflow configuration cannot be null.");
        Objects.requireNonNull(eventCondition, "The given event condition cannot be null.");

        // Fail fast: reject unparseable semver at registration rather than at start time.
        Version.validate(workflowConfiguration.workflowVersion());

        workflowsConfigurations.compute(eventCondition.qualifiedName(), (q, workflowConfigurations) -> {
            if (workflowConfigurations == null) {
                workflowConfigurations = new CopyOnWriteArrayList<>();
            }
            workflowConfigurations.add(new PredicatedWorkflowConfiguration(eventCondition.predicate(),
                                                                           workflowConfiguration));
            return workflowConfigurations;
        });
        highestVersionConfigurations.put(
                eventCondition.qualifiedName(),
                WorkflowConfigurationRegistry.super.getHighestVersionConfigurations(eventCondition.qualifiedName())
        );

        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The memoized lookup is keyed by {@link MessageType#qualifiedName()}, as the event's version does not select a
     * configuration yet. Taking the whole type here keeps {@link MessageType#version()} in reach of the lookup for when
     * it does.
     */
    @Override
    public List<PredicatedWorkflowConfiguration> getHighestVersionConfigurations(MessageType type) {
        return getHighestVersionConfigurations(type.qualifiedName());
    }

    @Override
    public List<PredicatedWorkflowConfiguration> getHighestVersionConfigurations(QualifiedName qualifiedName) {
        return highestVersionConfigurations.getOrDefault(qualifiedName, List.of());
    }

    @Override
    public Set<QualifiedName> supportedEvents() {
        return Set.copyOf(workflowsConfigurations.keySet());
    }


    @Override
    public List<PredicatedWorkflowConfiguration> getWorkflowsConfigurations(QualifiedName qualifiedName) {
        return workflowsConfigurations.getOrDefault(qualifiedName, List.of());
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        var qualifiedNamesToDefinitions = this.workflowsConfigurations.entrySet().stream()
                                                                      .map((e) -> new WorkflowDefinitionDescriptor(e.getKey(),
                                                                                                                   e.getValue()))
                                                                      .toList();
        descriptor.describeProperty("workflowDefinitions", qualifiedNamesToDefinitions);
    }

    record WorkflowDefinitionDescriptor(
            QualifiedName qualifiedName,
            List<PredicatedWorkflowConfiguration> configurations
    ) implements DescribableComponent {

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty(qualifiedName.toString(), configurations.stream().map(configuration -> {
                var definitionClass = configuration.configuration().workflowDefinition().getClass();
                return String.format("%s", definitionClass.getName());
            }).toList());
        }
    }
}
