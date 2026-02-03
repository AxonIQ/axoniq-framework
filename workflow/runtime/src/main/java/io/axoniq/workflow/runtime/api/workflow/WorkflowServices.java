package io.axoniq.workflow.runtime.api.workflow;

import io.axoniq.workflow.runtime.engine.impl.multi.EventSubscriptionManager;
import io.axoniq.workflow.runtime.engine.impl.multi.TaskManager;
import io.axoniq.workflow.runtime.engine.impl.multi.WorkflowEventAppender;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.util.concurrent.Executor;

public interface WorkflowServices {

  // required for primitives
  EventSubscriptionManager getEventSubscriptionManager();
  WorkflowEventAppender getWorkflowEventAppender();
  TaskManager getTaskManager();
  Clock getClock();

  // FIXME -> internal?
  UnitOfWorkFactory getUnitOfWorkFactory();
  // FIXME -> internal?
  Executor getExecutor();

  EventSink getEventSink();
}
