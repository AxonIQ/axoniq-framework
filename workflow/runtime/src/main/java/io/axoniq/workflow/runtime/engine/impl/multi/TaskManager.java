package io.axoniq.workflow.runtime.engine.impl.multi;

import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor;
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskManager {

  public static final Integer MAX = 100; // FIXME

  private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);
  private final WorkflowServices workflowServices;
  private final BlockingQueue<TaskWithFuture> submissionQueue = new LinkedBlockingQueue<>(MAX);
  private final ConcurrentHashMap<String, List<CompletableFuture<Map<String, Object>>>> tasksByWorkflowId = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private CompletableFuture<Void> executionFuture;

  public TaskManager(WorkflowServices workflowServices) {
    this.workflowServices = workflowServices;
  }

  public CompletableFuture<Map<String, Object>> execute(String workflowId, PayloadProcessor action, Map<String, Object> payload) {
    CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();
    tasksByWorkflowId.compute(workflowId, (key, existingTasks) -> {
      List<CompletableFuture<Map<String, Object>>> workflowTasks = existingTasks != null ? existingTasks : new CopyOnWriteArrayList<>();
      workflowTasks.add(future);
      return workflowTasks;
    });

    Task task = new Task(workflowId, action, payload);
    submissionQueue.add(new TaskWithFuture(task, future));

    return future;
  }

  public void start() {
    if (running.compareAndSet(false, true)) {
      executionFuture = CompletableFuture.runAsync(() -> {
        while (running.get()) {
          try {
            TaskWithFuture taskWithFuture = submissionQueue.take();
            // offload from task manager thread
            var taskExecution = CompletableFuture.supplyAsync(
              () -> taskWithFuture.task.payloadProcessor().apply(taskWithFuture.task.payload()),
              this.workflowServices.getExecutor()
            );
            // couple the timeout of the "client's" future to the future of the task execution.
            taskWithFuture.future.exceptionally((e) -> {
              if (e instanceof TimeoutException) {
                taskExecution.completeExceptionally(e);
              }
              throw new CompletionException(e);
            });
            taskExecution.thenApply(result -> {
                AtomicBoolean completed = new AtomicBoolean(false);
                tasksByWorkflowId.computeIfPresent(taskWithFuture.task.workflowId(),
                  (id, tasksForWorkflow) -> {
                    tasksForWorkflow.remove(taskWithFuture.future);
                    if (!taskWithFuture.future.isDone()) {
                      taskWithFuture.future.complete(result);
                      completed.set(true);
                    }
                    return tasksForWorkflow;
                  });
                return completed.get();
              }
            );
          } catch (InterruptedException e) {
            // FIXME -> Make sure on complete, timeout, error, cancel the futures are removed from the data structures above
            //
            Thread.currentThread().interrupt();
            break;
          } catch (Exception e) {
            logger.error("Error in TaskManager execution loop.", e);
          }
        }
      }, workflowServices.getExecutor());
    }
  }


  public void shutdown() {
    if (running.compareAndSet(true, false)) {
      if (executionFuture != null && !executionFuture.isDone()) {
        executionFuture.cancel(true);
        try {
          // Wait up to 1 second for the future to complete
          executionFuture.exceptionally(ex -> null).get(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
          // Ignore, we're shutting down anyway
        }
      }
    }
  }

  public int cancel(String workflowId) {
    List<CompletableFuture<Map<String, Object>>> workflowTasks = tasksByWorkflowId.remove(workflowId);

    if (workflowTasks == null || workflowTasks.isEmpty()) {
      return 0;
    }

    int count = 0;
    for (CompletableFuture<Map<String, Object>> future : workflowTasks) {
      if (!future.isDone()) {
        future.completeExceptionally(new InterruptedException("Workflow interrupted: " + workflowId));
        count++;
      }
    }
    return count;
  }


  record Task(String workflowId, PayloadProcessor payloadProcessor, Map<String, Object> payload) {
  }

  record TaskWithFuture(Task task, CompletableFuture<Map<String, Object>> future) {
  }
}
