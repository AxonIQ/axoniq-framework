package io.axoniq.example.workflow;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static io.axoniq.example.workflow.Waiter.waitWithProgress;

public class UserService {

  static Logger logger = LoggerFactory.getLogger(UserService.class);

  public static boolean createUser() {
    logger.info("Creating user.");
    return true;
  }

  public static Map<String, Object> activateUser(ProcessingContext pc, Map<String, Object> payload) {
    Instant now = Instant.now();
    logger.info("Activating user with id: {}", payload.get("id"));
    waitWithProgress(1_000);
    logger.info("Activation took {}.", Duration.between(Instant.now(), now));
    return Map.of();
  }
}
