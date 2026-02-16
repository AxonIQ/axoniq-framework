package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.Predicate;

public record EventCondition(
  @Nonnull QualifiedName qualifiedName,
  @Nonnull Predicate<EventMessage> payloadPredicate
) implements Predicate<EventMessage> {

  public static ComponentBuilder<EventCondition> fromType(Class<?> clazz) {
    return (c) -> new EventCondition(c.getComponent(MessageTypeResolver.class).resolve(clazz)
      .orElse(new MessageType(clazz))
      .qualifiedName(), (e) -> true);
  }

  @Override
  public boolean test(EventMessage eventMessage) {
    return eventMessage.type().qualifiedName().equals(this.qualifiedName) &&
      payloadPredicate.test(eventMessage);
  }
}
