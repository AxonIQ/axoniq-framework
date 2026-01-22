package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.api.workflow.StateManager;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.axoniq.workflow.runtime.util.MetadataUtils.METADATA_KEY_WORKFLOW_ID;

public class SimpleStateManager implements EventAppender, StateManager {

  private final List<EventMessage> events = new ArrayList<>();
  private final List<SubscriptionEntry> subscriptions = new CopyOnWriteArrayList<>();

  @Override
  public List<EventMessage> getHistory(String workflowId) {
    return events.stream()
      .filter(EventMessageUtils.workflowIdFilter(workflowId))
      .toList();
  }

  public List<Object> getEventPayloads(String workflowId) {
    return getHistory(workflowId)
      .stream()
      .map(event -> event.payloadAs(Object.class))
      .toList();
  }

  @Override
  public List<EventMessage> getEventByPayloadType(Class<?> clazz) {
    return events.stream().filter(e -> e.payloadType().equals(clazz)).toList();
  }

  @Override
  public Subscription subscribe(QualifiedName qualifiedName, Predicate<EventMessage> filter, EventListener listener) {
    var entry = new SubscriptionEntry(qualifiedName, filter, listener);
    subscriptions.add(entry);
    return entry;
  }

  private void notifySubscribers(EventMessage eventMessage) {
    subscriptions.removeIf(sub -> {
      if (sub.matches(eventMessage)) {
        return sub.listener.onEvent(eventMessage);
      }
      return false;
    });
  }

  void append(@Nonnull EventMessage eventMessage) {
    events.add(eventMessage);
    notifySubscribers(eventMessage);
  }

  @Override
  public void append(@NotNull List<?> events) {
    events.forEach(event -> {
      if (event instanceof EventMessage) {
        append((EventMessage) event);
      } else {
        append(new GenericEventMessage(new MessageType(event.getClass()), event));
      }
    });
  }

  /**
   * Internal subscription entry that implements Subscription for cancellation.
   */
  private class SubscriptionEntry implements Subscription {
    private final QualifiedName qualifiedName;
    private final Predicate<EventMessage> filter;
    private final EventListener listener;
    private final AtomicBoolean active = new AtomicBoolean(true);

    SubscriptionEntry(QualifiedName qualifiedName, Predicate<EventMessage> filter, EventListener listener) {
      this.qualifiedName = qualifiedName;
      this.filter = filter;
      this.listener = listener;
    }

    boolean matches(EventMessage event) {
      return active.get()
        && event.type().qualifiedName().equals(qualifiedName)
        && filter.test(event);
    }

    @Override
    public void cancel() {
      if (active.compareAndSet(true, false)) {
        subscriptions.remove(this);
      }
    }

    @Override
    public boolean isActive() {
      return active.get();
    }
  }

  @Override
  public void describeTo(@NotNull ComponentDescriptor descriptor) {
    Map<String, List<EventMessage>> eventsByWorkflowId = events.stream()
      .filter(e -> e.metadata().containsKey(METADATA_KEY_WORKFLOW_ID))
      .collect(Collectors.groupingBy(e -> e.metadata().getOrDefault(METADATA_KEY_WORKFLOW_ID, "none")));
    var events = eventsByWorkflowId.entrySet().stream()
      .filter(entry -> !entry.getKey().equals("none"))
      .map(e -> new WorkflowEventDescriptor(e.getKey(), e.getValue()))
      .toList();
    descriptor.describeProperty("workflowEvents", events);
  }

  record WorkflowEventDescriptor(
    String workflowId,
    List<EventMessage> events
  ) implements DescribableComponent {

    @Override
    public void describeTo(@NotNull ComponentDescriptor descriptor) {
      descriptor.describeProperty(workflowId, events.stream().map(event -> {
        var status = MetadataUtils.getStepStatus(event.metadata()).map(Enum::name).orElse("none");
        var name = event.type().qualifiedName().toString();
        return String.format("%s (%s): %s", name, status, event.payload());
      }).toList());
    }
  }
}
