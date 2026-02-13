package io.axoniq.workflow.runtime.test.fixture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationService {

  static Logger logger = LoggerFactory.getLogger(NotificationService.class);

  public static void sendEmail() {
    logger.info("Sending welcome mail to user.");
  }
}
