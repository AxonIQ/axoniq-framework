package io.axoniq.workflow.runtime.engine;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateManager {

  private static final Logger logger = LoggerFactory.getLogger(StateManager.class);

  private final Map<String, List<EventMessage>> events = new ConcurrentHashMap<>();

  public List<EventMessage> getHistory(String workflowId) {
    return events.getOrDefault(workflowId, new ArrayList<>());
  }

  public void append(String workflowId, EventMessage event) {
    events.putIfAbsent(workflowId, new ArrayList<>());
    events.get(workflowId).add(event);
  }

  public void appendAll(String workflowId, List<EventMessage> eventMessages) {
    events.putIfAbsent(workflowId, new ArrayList<>());
    events.get(workflowId).addAll(eventMessages);
  }

  public void clear(String workflowId) {
    events.remove(workflowId);
  }

  public void clearAll() {
    events.clear();
  }

  public void print() {
    events.keySet().forEach(workflowId -> {
      logger.info("Dumping events for workflow '{}'", workflowId);
      logger.info("------------------");
      events.get(workflowId).forEach(eventMessage -> {
        logger.info("{}", eventMessage);
      });
      logger.info("------------------");
    });
  }
}
