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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Registry for holding workflow configurations with corresponding start conditions.
 *
 * @param <W> type of the registry.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface WorkflowConfigurationRegistry<W extends WorkflowConfigurationRegistry<W>>
        extends DescribableComponent {

    @Nonnull
    default W register(
            @Nonnull QualifiedName qualifiedName,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    ) {
        return register(EventConditions.fromQualifiedName(qualifiedName), workflowConfiguration);
    }

    @Nonnull
    W register(
            @Nonnull EventCondition eventCondition,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    );

    @Nonnull
    Set<QualifiedName> supportedEvents();

    @Nonnull
    List<PredicatedWorkflowConfiguration> getWorkflowsConfigurations(@Nonnull QualifiedName qualifiedName);


    record PredicatedWorkflowConfiguration(
            BiPredicate<EventMessage, ProcessingContext> predicate,
            WorkflowConfiguration<?> configuration
    ) {

    }
}
