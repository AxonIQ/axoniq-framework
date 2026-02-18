/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public interface WorkflowDefinitionRegistry<W extends WorkflowDefinitionRegistry<W>> extends DescribableComponent {

    @Nonnull
    default W register(
            @Nonnull QualifiedName name,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    ) {
        return register(new EventCondition(name, (e) -> true), workflowConfiguration);
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
            Predicate<EventMessage> predicate,
            WorkflowConfiguration<?> configuration
    ) {

    }
}
