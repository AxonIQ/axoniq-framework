package io.axoniq.workflow.runtime.engine.registry;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public interface WorkflowDefinitionRegistry<W extends WorkflowDefinitionRegistry<W>> extends DescribableComponent {

  @Nonnull
  default W register(
    @Nonnull QualifiedName name,
    @Nonnull WorkflowConfiguration<?> workflowConfiguration
  ) {
    return register(Set.of(name), workflowConfiguration);
  }

  @Nonnull
  W register(
    @Nonnull Set<QualifiedName> names,
    @Nonnull WorkflowConfiguration<?> workflowConfiguration
  );

  @Nonnull
  Set<QualifiedName> supportedEvents();

  @Nonnull
  Map<QualifiedName, List<WorkflowConfiguration<?>>> getWorkflowsConfigurations();

  @Nonnull
  List<WorkflowConfiguration<?>> getWorkflowsConfigurations(@Nonnull QualifiedName qualifiedName);
}
