package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.engine.support.EventSubscriptionManager;
import io.axoniq.workflow.runtime.engine.support.TaskManager;
import io.axoniq.workflow.runtime.engine.support.WorkflowEventAppender;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;

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

}
