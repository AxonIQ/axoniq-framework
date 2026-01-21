package io.axoniq.workflow.runtime.util;

public class Utils {

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for events", ie);
    }
  }

  public static <T> T createInstance(Class<T> clazz) {
    try {
      var constructor = clazz.getDeclaredConstructor();
      constructor.setAccessible(true);
      return constructor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate " + clazz.getName(), e);
    }
  }

}
