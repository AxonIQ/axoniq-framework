package io.axoniq.shardlab;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.RetryPolicy;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;

/**
 * The lab workload: an order workflow that waits for a business event, a long-running job, and a flaky job that
 * retries with backoff.
 */
@Component
public class LabWorkflows {

    private final StepLog stepLog;

    public LabWorkflows(StepLog stepLog) {
        this.stepLog = stepLog;
    }

    @Workflow(idProperty = "orderId",
              startOnEventClass = Events.OrderPlaced.class,
              workflowName = "OrderWorkflow",
              workflowNamespace = "io.axoniq.shardlab")
    public void order(SimpleWorkflowContext ctx) {
        var orderId = String.valueOf(ctx.workflowPayload().get("orderId"));
        ctx.awaitExecute("reserve", Map.of(), (pc, p) -> {
            stepLog.record(orderId, "reserve");
            return Map.of();
        });

        ctx.awaitEvent("awaitPayment",
                       Events.PaymentReceived.class,
                       associate(payloadProperty("orderId"), equalsTo(orderId)),
                       step -> step.timeout(Duration.ofMinutes(10)));

        ctx.awaitExecute("ship", Map.of(), (pc, p) -> {
            stepLog.record(orderId, "ship");
            return Map.of();
        });
    }

    @Workflow(idProperty = "jobId",
              startOnEventClass = Events.SlowJobRequested.class,
              workflowName = "SlowWorkflow",
              workflowNamespace = "io.axoniq.shardlab")
    public void slow(SimpleWorkflowContext ctx) {
        var jobId = String.valueOf(ctx.workflowPayload().get("jobId"));
        var seconds = ((Number) ctx.workflowPayload().getOrDefault("seconds", 5)).intValue();
        ctx.awaitExecute("slowStep", Map.of(), (pc, p) -> {
            stepLog.record(jobId, "slowStep-begin");
            sleep(seconds * 1000L);
            stepLog.record(jobId, "slowStep-end");
            return Map.of();
        }, step -> step.timeout(Duration.ofMinutes(10)));
        ctx.awaitExecute("finish", Map.of(), (pc, p) -> {
            stepLog.record(jobId, "finish");
            return Map.of();
        });
    }

    @Workflow(idProperty = "jobId",
              startOnEventClass = Events.FlakyJobRequested.class,
              workflowName = "FlakyWorkflow",
              workflowNamespace = "io.axoniq.shardlab")
    public void flaky(SimpleWorkflowContext ctx) {
        var jobId = String.valueOf(ctx.workflowPayload().get("jobId"));
        var failures = ((Number) ctx.workflowPayload().getOrDefault("failures", 2)).intValue();
        ctx.awaitExecute("flakyStep", Map.of(), (pc, p) -> {
            var attempt = stepLog.countAttempts(jobId);
            stepLog.record(jobId, "flakyStep-attempt-" + attempt);
            if (attempt < failures) {
                throw new IllegalStateException("flaky failure " + attempt);
            }
            return Map.of();
        }, step -> step.timeout(Duration.ofMinutes(10))
                       .retryPolicy(RetryPolicy.maxRetries(10)
                                               .withBackoff(BackoffStrategy.fixed(Duration.ofSeconds(3)))));
        ctx.awaitExecute("flakyDone", Map.of(), (pc, p) -> {
            stepLog.record(jobId, "flakyDone");
            return Map.of();
        });
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
