package io.axoniq.workflow.runtime;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DelayedPublisher {

  private final EventSink eventSink;
  private final Executor executor;
  private final List<Schedule> schedules = new ArrayList<>();

  public DelayedPublisher(EventSink eventSink, Executor executor) {
    this.eventSink = eventSink;
    this.executor = executor;
  }

  public void addSchedules(List<Schedule> schedules) {
    this.schedules.addAll(schedules);
  }

  public CompletableFuture<Void> start() {
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

    for (Schedule schedule : schedules) {
      future = future.thenCompose(v ->
        CompletableFuture.supplyAsync(() -> {
          eventSink.publish(null, new GenericEventMessage(
            MessageType.fromString(schedule.event.getClass().getTypeName() + "#0.0.1"),
            schedule.event)
          );
          return null;
        }, CompletableFuture.delayedExecutor(schedule.duration.toMillis(), TimeUnit.MILLISECONDS, executor))
      );
    }
    return future;
  }

  public record Schedule(
    Duration duration,
    Object event
  ) {
    public static Schedule of(Duration duration, Object event) {
      return new Schedule(duration, event);
    }

    public static Schedule ofMillis(long millis, Object event) {
      return new Schedule(Duration.ofMillis(millis), event);
    }
  }

}
