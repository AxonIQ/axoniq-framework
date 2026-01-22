package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SimpleWorkflowConfigurationRegistry implements WorkflowConfigurationRegistry<SimpleWorkflowConfigurationRegistry> {

  private final ConcurrentHashMap<QualifiedName, List<WorkflowConfiguration<?>>> workflowsConfigurations = new ConcurrentHashMap<>();

  @Override
  @Nonnull
  public SimpleWorkflowConfigurationRegistry register(@NotNull Set<QualifiedName> names, @NotNull WorkflowConfiguration<?> workflowConfiguration) {
    Objects.requireNonNull(workflowConfiguration, "The given workflow configuration cannot be null.");
    names.forEach(name -> workflowsConfigurations.compute(name, (q, workflowConfigurations) -> {
      if (workflowConfigurations == null) {
        workflowConfigurations = new CopyOnWriteArrayList<>();
      }
      workflowConfigurations.add(workflowConfiguration);
      return workflowConfigurations;
    }));

    return this;
  }

  @Override
  @Nonnull
  public Set<QualifiedName> supportedEvents() {
    return Set.copyOf(workflowsConfigurations.keySet());
  }

  @Nonnull
  @Override
  public Map<QualifiedName, List<WorkflowConfiguration<?>>> getWorkflowsConfigurations() {
    return Map.copyOf(workflowsConfigurations);
  }
}
