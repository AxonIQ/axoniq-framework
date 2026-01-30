package io.axoniq.workflow.runtime.engine.registry;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DefaultWorkflowRepository implements WorkflowRepository<DefaultWorkflowRepository> {

  private final ConcurrentHashMap<QualifiedName, List<WorkflowConfiguration<?>>> workflowsConfigurations = new ConcurrentHashMap<>();

  @Override
  @Nonnull
  public DefaultWorkflowRepository register(@NotNull Set<QualifiedName> names, @NotNull WorkflowConfiguration<?> workflowConfiguration) {
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


  @Override
  @Nonnull
  public Map<QualifiedName, List<WorkflowConfiguration<?>>> getWorkflowsConfigurations() {
    return Map.copyOf(workflowsConfigurations);
  }

  @Nonnull
  @Override
  public List<WorkflowConfiguration<?>> getWorkflowsConfigurations(@Nonnull QualifiedName qualifiedName) {
    return workflowsConfigurations.getOrDefault(qualifiedName, List.of());
  }

  @Override
  public void describeTo(@NotNull ComponentDescriptor descriptor) {
    var qualifiedNamesToDefinitions = this.workflowsConfigurations.entrySet().stream()
      .map((e) -> new WorkflowDefinitionDescriptor(e.getKey(), e.getValue())).toList();
    descriptor.describeProperty("workflowDefinitions", qualifiedNamesToDefinitions);
  }

  record WorkflowDefinitionDescriptor(
    QualifiedName qualifiedName,
    List<WorkflowConfiguration<?>> configurations
  ) implements DescribableComponent {

    @Override
    public void describeTo(@NotNull ComponentDescriptor descriptor) {
      descriptor.describeProperty(qualifiedName.toString(), configurations.stream().map(configuration -> {
        var definitionClass = configuration.workflowDefinition().getClass();
        return String.format("%s", definitionClass.getName());
      }).toList());
    }
  }

}
