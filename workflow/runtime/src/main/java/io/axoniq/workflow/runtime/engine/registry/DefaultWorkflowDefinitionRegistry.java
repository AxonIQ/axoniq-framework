package io.axoniq.workflow.runtime.engine.registry;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

public class DefaultWorkflowDefinitionRegistry implements WorkflowDefinitionRegistry<DefaultWorkflowDefinitionRegistry> {

  private final ConcurrentHashMap<QualifiedName, List<PredicatedWorkflowConfiguration>> workflowsConfigurations = new ConcurrentHashMap<>();

  @Override
  @Nonnull
  public DefaultWorkflowDefinitionRegistry register(
    @NotNull Set<QualifiedName> names,
    @NotNull WorkflowConfiguration<?> workflowConfiguration,
    @NotNull Predicate<EventMessage> startPredicate
  ) {
    Objects.requireNonNull(workflowConfiguration, "The given workflow configuration cannot be null.");
    names.forEach(name -> workflowsConfigurations.compute(name, (q, workflowConfigurations) -> {
      if (workflowConfigurations == null) {
        workflowConfigurations = new CopyOnWriteArrayList<>();
      }
      workflowConfigurations.add(new PredicatedWorkflowConfiguration(startPredicate, workflowConfiguration));
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
  public List<PredicatedWorkflowConfiguration> getWorkflowsConfigurations(@Nonnull QualifiedName qualifiedName) {
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
    List<PredicatedWorkflowConfiguration> configurations
  ) implements DescribableComponent {

    @Override
    public void describeTo(@NotNull ComponentDescriptor descriptor) {
      descriptor.describeProperty(qualifiedName.toString(), configurations.stream().map(configuration -> {
        var definitionClass = configuration.configuration().workflowDefinition().getClass();
        return String.format("%s", definitionClass.getName());
      }).toList());
    }
  }


}
