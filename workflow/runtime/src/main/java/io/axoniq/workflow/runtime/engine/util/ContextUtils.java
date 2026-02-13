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
