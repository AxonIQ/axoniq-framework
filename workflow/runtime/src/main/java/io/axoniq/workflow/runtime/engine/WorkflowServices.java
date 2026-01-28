package io.axoniq.workflow.runtime.engine;

import io.axoniq.workflow.runtime.engine.streaming.EventSubscriptionManager;
import io.axoniq.workflow.runtime.engine.streaming.WorkflowEventAppender;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;

import java.time.Clock;
import java.util.concurrent.Executor;

public interface WorkflowServices {

  EventSubscriptionManager getEventSubscriptionManager();
  WorkflowEventAppender getWorkflowEventAppender();
  UnitOfWorkFactory getUnitOfWorkFactory();
  Executor getExecutor();
  Clock getClock();

}
