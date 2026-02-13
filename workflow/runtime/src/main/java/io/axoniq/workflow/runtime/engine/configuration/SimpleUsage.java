package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.dsl.Payload;
import io.axoniq.workflow.dsl.simple2.MyWorkflowContext;
import io.axoniq.workflow.dsl.simple2.MyWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.engine.execution.ContextToStateAdoptingStateFactory;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Map;
import java.util.Optional;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;

public class SimpleUsage {

  public void test() {
    WorkflowModule

      // DSL Part
      .declarative(MyWorkflowContext.class)
      .workflowContextFactory(c -> new MyWorkflowContextFactory(
        c.getComponent(EventNameCustomizer.class)
      ))
      .workflowStateFactory(c -> new ContextToStateAdoptingStateFactory<>(MyWorkflowContext.class))

      // Workflow configuration part
      .definitions(d -> d.declarative("name of this workflow")

        .on(c -> new EventCondition(new QualifiedName("io.my.StartEvent"), e -> true))
        .workflowDefinition(c -> this::execute)
        .eventNameCustomizer(c -> () -> eventName().namespace("io.my.namespace"))
        .workflowIdProvider(c -> this::association)
        .notCustomized()

        .declarative("another workflow")
        .on(c -> new EventCondition(new QualifiedName("io.my.StartEvent"), e -> true))
        .workflowDefinition(c -> this::execute2)
        .eventNameCustomizer(c -> () -> eventName().namespace("io.my.namespace2"))
        .workflowIdProvider(c -> this::association)
        .notCustomized()
      );

  }

  public void execute(MyWorkflowContext ctx) {
    ctx.execute("activate customer", Payload.payload(Map.of()), p -> {

      return Payload.payload(Map.of());
    });
  }

  public void execute2(MyWorkflowContext ctx) {
  }

  public Optional<String> association(Map<String, Object> payload) {
    return Optional.ofNullable((String) payload.get("corelationKey"));
  }
}
