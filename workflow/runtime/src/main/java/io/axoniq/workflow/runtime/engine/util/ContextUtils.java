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
package io.axoniq.workflow.runtime.engine.util;

import io.axoniq.workflow.runtime.api.WorkflowServices;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ContextUtils {

  public static <R> CompletableFuture<R> executeWithResult(
    @Nullable String id,
    @Nonnull WorkflowServices workflowServices,
    @Nonnull Context parentContext,
    @Nonnull Function<ProcessingContext, CompletableFuture<R>> action) {
    var uow = (id == null)
      ? workflowServices.getUnitOfWorkFactory()
      .create(customize -> customize.workScheduler(workflowServices.getExecutor()))
      : workflowServices.getUnitOfWorkFactory()
      .create(id, customize -> customize.workScheduler(workflowServices.getExecutor()));
    return uow.executeWithResult(c -> {
      var ctx = ContextUtils.copyResources(parentContext, c);
      return action.apply(ctx);
    });
  }

  public static ProcessingContext copyResources(Context from,
                                                ProcessingContext to) {
    var fromResource = from.resources();
    //noinspection unchecked
    fromResource.forEach((k, v) -> to.putResource((Context.ResourceKey<Object>) k, v));
    return to;
  }


  private ContextUtils() {
    // avoid instantiation
  }
}
