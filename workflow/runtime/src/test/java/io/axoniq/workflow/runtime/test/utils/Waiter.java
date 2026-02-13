package io.axoniq.workflow.runtime.test.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Waiter {
  static Logger logger = LoggerFactory.getLogger(Waiter.class);

  public static void waitWithProgress(long millis) {
    try {
      for (long i = 0; i < millis; i = i + 200) {
        Thread.sleep(i);
        logger.info("Waiting for {} / {} millis.", i, millis);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
