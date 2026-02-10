package io.axoniq.workflow.runtime.engine.registry;

import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public interface WorkflowDefinitionRegistry<W extends WorkflowDefinitionRegistry<W>> extends DescribableComponent {

  @Nonnull
  default W register(
    @Nonnull QualifiedName name,
    @Nonnull WorkflowConfiguration<?> workflowConfiguration
  ) {
    return register(Set.of(name), workflowConfiguration, (e) -> true);
  }

  @Nonnull
  W register(
    @Nonnull Set<QualifiedName> names,
    @Nonnull WorkflowConfiguration<?> workflowConfiguration,
    @Nonnull Predicate<EventMessage> startPredicate
  );

  @Nonnull
  Set<QualifiedName> supportedEvents();

  @Nonnull
  List<PredicatedWorkflowConfiguration> getWorkflowsConfigurations(@Nonnull QualifiedName qualifiedName);


  record PredicatedWorkflowConfiguration(
    Predicate<EventMessage> predicate,
    WorkflowConfiguration<?> configuration
  ) {}

}
