package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.util.EventMessageUtils;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
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

public class StateManager {

  private static final Logger logger = LoggerFactory.getLogger(StateManager.class);

  private final List<EventMessage> events = new ArrayList<>();
  private final List<SubscriptionEntry> subscriptions = new CopyOnWriteArrayList<>();

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

  public List<EventMessage> getEventByPayloadType(Class<?> clazz) {
    return events.stream().filter(e -> e.payloadType().equals(clazz)).toList();
  }

  public void append(EventMessage eventMessage) {
    events.add(eventMessage);
    notifyListeners(eventMessage);
  }

  /**
   * Subscribe to events of a specific type that match the given filter.
   *
   * @param qualifiedName the qualified name of the event payload to listen for
   * @param filter predicate to filter events (applied to EventMessage)
   * @param listener callback invoked when a matching event is appended
   * @return a Subscription handle to cancel the subscription
   */
  public Subscription subscribe(QualifiedName qualifiedName, Predicate<EventMessage> filter, EventListener listener) {
    var entry = new SubscriptionEntry(qualifiedName, filter, listener);
    subscriptions.add(entry);
    return entry;
  }

  private void notifyListeners(EventMessage eventMessage) {
    subscriptions.removeIf(sub -> {
      if (sub.matches(eventMessage)) {
        return sub.listener.onEvent(eventMessage);
      }
      return false;
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

  public void printPayloads() {
    // Group events by workflowId
    Map<String, List<EventMessage>> eventsByWorkflowId = events.stream()
      .filter(e -> e.metadata().containsKey(METADATA_KEY_WORKFLOW_ID))
      .collect(Collectors.groupingBy(e -> e.metadata().getOrDefault(METADATA_KEY_WORKFLOW_ID, "none")));

    // Print payloads for each workflowId
    eventsByWorkflowId.forEach((workflowId, workflowEvents) -> {
      if (!workflowId.equals("none")) {
        logger.info("Dumping events for workflow '{}'", workflowId);
        logger.info("------------------");
        workflowEvents.forEach(event -> {
          var status = MetadataUtils.getStepStatus(event.metadata()).map(Enum::name).orElse("none");
          var name = event.type().qualifiedName().toString();
          logger.info("{} ({}): {}", name, status, event.payloadAs(Object.class));
        });
        logger.info("------------------");
      }
    });
  }

}
