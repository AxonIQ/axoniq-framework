package io.axoniq.workflow.runtime.engine.util;

import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.SimpleContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;

public class ContextUtils {

  public static ProcessingContext withResources(ProcessingContext processingContext, Map<Context.ResourceKey<?>, Object> resources) {
    ProcessingContext copy = processingContext;
    for (Map.Entry<Context.ResourceKey<?>, Object> resourceEntry : resources.entrySet()) {
      copy = copy.withResource((Context.ResourceKey<? super Object>) resourceEntry.getKey(), resourceEntry.getValue());
    }
    return copy;
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
