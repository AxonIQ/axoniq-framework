/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
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
