package io.axoniq.workflow.runtime.engine;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class StateManager {

  public static final String KEY_WF_ID = "workflowId";
  private static final Logger logger = LoggerFactory.getLogger(StateManager.class);

  private final List<EventMessage> events = new ArrayList<>();

  public List<EventMessage> getHistory(String workflowId) {
    return events.stream()
      .filter(e -> e.metadata().containsKey(KEY_WF_ID) && Objects.requireNonNull(e.metadata().get(KEY_WF_ID)).equals(workflowId))
      .toList();
  }

  public List<EventMessage> getEventByCriteria(Class<?> clazz) {
    return events.stream().filter(e -> e.payloadType().equals(clazz)).toList();
  }

  public void append(EventMessage eventMessage) {
    events.add(eventMessage);
  }

  public void append(String workflowId, EventMessage event) {
    var workflowEvent = event.andMetadata(Map.of(KEY_WF_ID, workflowId));
    events.add(workflowEvent);
  }

  public void appendAll(String workflowId, List<EventMessage> eventMessages) {
    eventMessages.forEach(event -> {
      var workflowEvent = event.andMetadata(Map.of(KEY_WF_ID, workflowId));
      events.add(workflowEvent);
    });
  }

  public List<Object> getEventPayloads(String workflowId) {
    return getHistory(workflowId).stream()
      .map(event -> event.payloadAs(Object.class))
      .toList();
  }

  public void printPayloads() {
    // Group events by workflowId
    Map<String, List<EventMessage>> eventsByWorkflowId = events.stream()
      .filter(e -> e.metadata().containsKey(KEY_WF_ID))
      .collect(Collectors.groupingBy(e -> e.metadata().getOrDefault(KEY_WF_ID, "none")));

    // Print payloads for each workflowId
    eventsByWorkflowId.forEach((workflowId, workflowEvents) -> {
      if (!workflowId.equals("none")) {
        logger.info("Dumping events for workflow '{}'", workflowId);
        logger.info("------------------");
        getEventPayloads(workflowId).forEach(payload -> {
          logger.info("{}", payload);
        });
        logger.info("------------------");
      }
    });
  }

}
