package io.axoniq.workflow.runtime.___stash;

import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.List;
import java.util.function.Predicate;

public interface StateManager extends DescribableComponent {

  List<EventMessage> getHistory(String workflowId);

  List<EventMessage> getEventByPayloadType(Class<?> clazz);

  /**
   * Subscribe to events of a specific type that match the given filter.
   *
   * @param qualifiedName the qualified name of the event payload to listen for
   * @param filter predicate to filter events (applied to EventMessage)
   * @param listener callback invoked when a matching event is appended
   * @return a Subscription handle to cancel the streaming
   */
  Subscription subscribe(QualifiedName qualifiedName, Predicate<EventMessage> filter, EventListener listener);
}
