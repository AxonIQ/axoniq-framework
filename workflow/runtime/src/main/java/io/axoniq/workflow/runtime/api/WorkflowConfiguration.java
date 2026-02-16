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

import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;

/**
 * Configures definition, context factory and correlation provider.
 *
 * @param <T> workflow context type.
 */
public interface WorkflowConfiguration<T extends WorkflowContext> {

  @Nonnull
  WorkflowDefinition<T> workflowDefinition();

  @Nonnull
  AssociationProvider associationProvider();

  @Nonnull
  default String workflowName() {
    return this.getClass().getSimpleName();
  }

  @Nonnull
  default EventNameCustomizer eventNameCustomizer() {
    return DefaultEventNameCustomizer.Builder.eventName();
  }

  @Nonnull
  WorkflowContextFactory<T> workflowContextFactory();

  @Nonnull
  WorkflowStateFactory workflowStateFactory();

}
