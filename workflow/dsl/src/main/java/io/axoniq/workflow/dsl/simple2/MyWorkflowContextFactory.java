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
package io.axoniq.workflow.dsl.simple2;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowServices;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class MyWorkflowContextFactory implements WorkflowContextFactory<MyWorkflowContext> {

  private final EventNameCustomizer parentCustomizer;

  public MyWorkflowContextFactory(
    @Nonnull EventNameCustomizer parentCustomizer
  ) {
    this.parentCustomizer = parentCustomizer;
  }

  @NotNull
  @Override
  public MyWorkflowContext createContext(
    @NotNull Map<String, Object> initialPayload,
    @Nonnull String workflowId,
    @Nonnull ProcessingContext processingContext,
    @Nonnull WorkflowServices workflowServices) {
    return new MyWorkflowContext(
      workflowId,
      initialPayload,
      processingContext,
      parentCustomizer,
      workflowServices);
  }
}
