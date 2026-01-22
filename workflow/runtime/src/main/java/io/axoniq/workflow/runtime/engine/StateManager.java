package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.util.EventMessageUtils;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.axoniq.workflow.runtime.util.MetadataUtils.METADATA_KEY_WORKFLOW_ID;

public class StateManager {

  private static final Logger logger = LoggerFactory.getLogger(StateManager.class);

  private final List<EventMessage> events = new ArrayList<>();

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
