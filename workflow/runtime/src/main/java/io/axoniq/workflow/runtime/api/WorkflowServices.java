package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.util.concurrent.Executor;

public interface WorkflowServices {

  @Nonnull
  Clock getClock();

  @Nonnull
  UnitOfWorkFactory getUnitOfWorkFactory();

  @Nonnull
  Executor getExecutor();

  @Nonnull
  EventSink getEventSink();

  @Nonnull
  Converter getConverter();
}
