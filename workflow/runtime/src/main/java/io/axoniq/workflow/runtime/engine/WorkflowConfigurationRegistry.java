package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface WorkflowConfigurationRegistry<W extends WorkflowConfigurationRegistry<W>> {

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
}
