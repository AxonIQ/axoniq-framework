package io.axoniq.workflow.runtime.engine.execution;

public class ExecutionSuspended extends Throwable {

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
