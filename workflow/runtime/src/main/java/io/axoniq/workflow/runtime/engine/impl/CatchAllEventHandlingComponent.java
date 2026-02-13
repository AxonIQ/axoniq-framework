package io.axoniq.workflow.runtime.engine.impl;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.*;
import org.axonframework.messaging.eventhandling.sequencing.HierarchicalSequencingPolicy;
import org.axonframework.messaging.eventhandling.sequencing.SequencingPolicy;
import org.axonframework.messaging.eventhandling.sequencing.SequentialPerAggregatePolicy;
import org.axonframework.messaging.eventhandling.sequencing.SequentialPolicy;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class CatchAllEventHandlingComponent implements EventHandlingComponent {

  private static final Logger logger = LoggerFactory.getLogger(CatchAllEventHandlingComponent.class);

  private final SequencingPolicy sequencingPolicy;
  private final EventHandler eventHandler;

  public CatchAllEventHandlingComponent(EventHandler eventHandler) {
    this.sequencingPolicy = new HierarchicalSequencingPolicy(
      SequentialPerAggregatePolicy.instance(),
      SequentialPolicy.INSTANCE
    );
    this.eventHandler = eventHandler;
  }

  @NotNull
  @Override
  public MessageStream.Empty<Message> handle(@NotNull EventMessage event, @NotNull ProcessingContext context) {
    logger.debug("Handling event {}", event);
    return eventHandler.handle(event, context);
  }

  @Override
  public Set<QualifiedName> supportedEvents() {
    return Set.of();
  }

  @Override
  public boolean supports(@NotNull QualifiedName eventName) {
    return true;
  }

  @NotNull
  @Override
  public Object sequenceIdentifierFor(@NotNull EventMessage event, @NotNull ProcessingContext context) {
    return sequencingPolicy.getSequenceIdentifierFor(event, context);
  }

  @Override
  public void describeTo(@NotNull ComponentDescriptor descriptor) {
   descriptor.describeProperty("event-handler", eventHandler.getClass());
  }
}
