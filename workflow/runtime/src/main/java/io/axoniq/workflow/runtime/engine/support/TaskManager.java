package io.axoniq.workflow.runtime.engine.support;

import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.engine.WorkflowServices;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskManager {

  private final WorkflowServices workflowServices;
  private final ConcurrentHashMap<String, List<CompletableFuture<Map<String, Object>>>> tasks = new ConcurrentHashMap<>();

  public TaskManager(WorkflowServices workflowServices) {
    this.workflowServices = workflowServices;
  }

  public CompletableFuture<Map<String, Object>> execute(String workflowId, PayloadProcessor action, Map<String, Object> payload) {

    CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();
    tasks.compute(workflowId, (key, existingTasks) -> {
      List<CompletableFuture<Map<String, Object>>> workflowTasks = existingTasks != null ? existingTasks : new CopyOnWriteArrayList<>();
      workflowTasks.add(future);
      return workflowTasks;
    });

    schedule(workflowId, future, action, payload);

    return future;
  }

  private void schedule(String workflowId, CompletableFuture<Map<String, Object>> future, PayloadProcessor action, Map<String, Object> payload) {
    CompletableFuture.supplyAsync(() -> action.apply(payload), this.workflowServices.getExecutor())
      .thenApply(result -> {
          AtomicBoolean completed = new AtomicBoolean(false);
          tasks.computeIfPresent(workflowId, (id, tasksForWorkflow) -> {
            tasksForWorkflow.remove(future);
            future.complete(result);
            completed.set(true);
            return tasksForWorkflow;
          });
          return completed.get();
        }
      );
  }

}
