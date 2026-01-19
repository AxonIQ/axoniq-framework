package io.axoniq.workflow.runtime;

import io.axoniq.workflow.runtime.engine.StateManager;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DelayedPublisher {

  private final StateManager stateManager;
  private final List<Schedule> schedules = new ArrayList<>();

  public DelayedPublisher(StateManager stateManager) {
    this.stateManager = stateManager;
  }

  void addSchedules(List<Schedule> schedules) {
    this.schedules.addAll(schedules);
  }

  CompletableFuture<Void> arm() {
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

    for (Schedule schedule : schedules) {
      future = future.thenCompose(v ->
        CompletableFuture.supplyAsync(() -> {
          stateManager.append(new GenericEventMessage(
            MessageType.fromString(schedule.event.getClass().getTypeName() + "#0.1"),
            schedule.event)
          );
          return null;
        }, CompletableFuture.delayedExecutor(schedule.duration.toMillis(), TimeUnit.MILLISECONDS))
      );
    }
    return future;
  }


  record Schedule(
    Duration duration,
    Object event
  ) {
    static Schedule of(Duration duration, Object event) {
      return new Schedule(duration, event);
    }

    static Schedule ofMillis(long millis, Object event) {
      return new Schedule(Duration.ofMillis(millis), event);
    }
  }

}
