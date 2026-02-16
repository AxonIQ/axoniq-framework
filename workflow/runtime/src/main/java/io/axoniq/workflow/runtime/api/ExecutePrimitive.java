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
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Map;


public interface ExecutePrimitive {
  /**
   * Execute primitive.
   *
   * @param stepName         name of the step.
   * @param local            local context passed to the call.
   * @param action           action to execute.
   * @param parameterMapping reducer for parameters (to reduce local and workflow contexts -> effective parameters).
   * @param resultMapping    reducer for result (to reduce result and global context -> global context after action).
   * @param timeout          timeout of the action.
   * @param eventNameCustomizer event name customizer.
   * @return result.
   */
  @Nonnull
  WorkflowStepResult execute(
    @Nonnull String stepName,
    @Nullable Map<String, Object> local,
    @Nonnull PayloadProcessor action,
    @Nonnull PayloadReducer parameterMapping,
    @Nonnull PayloadReducer resultMapping,
    @Nonnull Duration timeout,
    @Nonnull EventNameCustomizer eventNameCustomizer
  );

}
