package io.axoniq.workflow.runtime.test.configuration;

import io.axoniq.workflow.runtime.engine.configuration.PrettyPrintingRecordingEventStore;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jetbrains.annotations.NotNull;

public class WorkflowTestEnhancer implements ConfigurationEnhancer {

  @Override
  public void enhance(@NotNull ComponentRegistry registry) {
    registry
      .registerComponent(DelayedPublisher.class, cfg ->
        new DelayedPublisher(
          cfg.getComponent(EventSink.class),
          cfg.getComponent(WorkflowEngine.class).getExecutor(),
          cfg.getComponent(MessageTypeResolver.class)
        )
      ).registerDecorator(EventStore.class, InterceptingEventStore.DECORATION_ORDER - 1, (configuration, name, delegate) ->
        PrettyPrintingRecordingEventStore.eventStore(delegate)
      );
  }
}
