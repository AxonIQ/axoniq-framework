package io.axoniq.workflow.runtime.api.workflow;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.util.concurrent.Executor;

public interface WorkflowServices {

  Clock getClock();

  UnitOfWorkFactory getUnitOfWorkFactory();

  Executor getExecutor();

  EventSink getEventSink();

  Converter getConverter();
}
